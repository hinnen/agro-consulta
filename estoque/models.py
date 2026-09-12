from django.conf import settings
from django.db import models


class Estoque(models.Model):
    empresa = models.ForeignKey('base.Empresa', on_delete=models.CASCADE, related_name='estoques', null=True, blank=True)
    produto = models.ForeignKey('produtos.Produto', on_delete=models.CASCADE, related_name='estoques')
    loja = models.ForeignKey('base.Loja', on_delete=models.CASCADE, related_name='estoques')
    saldo = models.DecimalField(max_digits=10, decimal_places=3, default=0)
    estoque_minimo = models.DecimalField(max_digits=10, decimal_places=3, default=0)
    atualizado_em = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = 'Estoque'
        verbose_name_plural = 'Estoques'
        ordering = ['loja__nome', 'produto__nome']
        constraints = [models.UniqueConstraint(fields=['produto', 'loja'], name='unique_produto_loja_estoque')]

class OrigemAjusteEstoque(models.TextChoices):
    """Origem do ajuste na camada Agro (Mongo ERP não é alterado por estes registros)."""

    AJUSTE_PIN = "ajuste_pin", "Ajuste PIN / modal"
    ENTRADA_NF_AGRO = "entrada_nf_agro", "Entrada NF (Agro)"
    ESTORNO_ENTRADA_NF_AGRO = "estorno_entrada_nf_agro", "Estorno entrada NF (reabrir)"
    BAIXA_VENDA_PDV = "baixa_venda_pdv", "Baixa venda PDV"
    DEVOLUCAO_VENDA_PDV = "devolucao_venda_pdv", "Devolução venda PDV"
    TRANSFERENCIA_UI = "transferencia_ui", "Transferência / tela"
    PLANILHA = "planilha", "Importação planilha"
    VENCIMENTO_EM_LOJA = "vencimento_em_loja", "Vencimento em Loja"
    USO_LOJA = "uso_loja", "Uso loja"
    ESTORNO_USO_LOJA = "estorno_uso_loja", "Estorno uso loja"
    CONTAGEM_CICLICA = "contagem_ciclica", "Contagem cíclica"
    OUTRO = "outro", "Outro"


class EstoqueSyncHealth(models.Model):
    """
    Registro singleton (pk=1): último ping ao Mongo de estoque, build de catálogo PDV e alertas.
    """

    id = models.PositiveSmallIntegerField(primary_key=True, default=1, editable=False)
    mongo_ultimo_ping_em = models.DateTimeField(null=True, blank=True)
    mongo_ultimo_ok = models.BooleanField(default=True)
    mongo_ultimo_erro = models.TextField(blank=True)
    catalogo_ultimo_build_em = models.DateTimeField(null=True, blank=True)
    catalogo_ultima_versao = models.CharField(max_length=80, blank=True)
    falhas_sequenciais_mongo = models.PositiveIntegerField(default=0)
    atualizado_em = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Saúde sync estoque (Agro)"
        verbose_name_plural = "Saúde sync estoque (Agro)"

    def save(self, *args, **kwargs):
        self.pk = 1
        super().save(*args, **kwargs)


class AjusteRapidoEstoque(models.Model):
    empresa = models.ForeignKey('base.Empresa', on_delete=models.CASCADE, null=True, blank=True)
    loja = models.ForeignKey('base.Loja', on_delete=models.CASCADE, null=True, blank=True)
    produto_externo_id = models.CharField(max_length=100, db_index=True)
    codigo_interno = models.CharField(max_length=100, blank=True, default='')
    nome_produto = models.CharField(max_length=255, blank=True, default='')
    deposito = models.CharField(max_length=20, db_index=True)
    saldo_erp_referencia = models.DecimalField(max_digits=12, decimal_places=3, default=0)
    saldo_informado = models.DecimalField(max_digits=12, decimal_places=3, default=0)
    diferenca_saldo = models.DecimalField(max_digits=12, decimal_places=3, default=0)
    origem = models.CharField(
        max_length=40,
        choices=OrigemAjusteEstoque.choices,
        default=OrigemAjusteEstoque.OUTRO,
        db_index=True,
    )
    usuario = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="ajustes_estoque_agro",
    )
    observacao = models.TextField(blank=True)
    criado_em = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ['-criado_em']

    def save(self, *args, **kwargs):
        self.diferenca_saldo = self.saldo_informado - self.saldo_erp_referencia
        super().save(*args, **kwargs)

class ConfiguracaoTransferencia(models.Model):
    produto_externo_id = models.CharField("ID do Produto", max_length=100, unique=True, db_index=True)
    nome_produto = models.CharField("Nome do Produto", max_length=255, blank=True, default='')
    
    # Parâmetros da Fórmula
    venda_media_diaria = models.DecimalField("Venda Média Diária", max_digits=10, decimal_places=3, default=0)
    capacidade_maxima = models.DecimalField("Capacidade Máxima", max_digits=10, decimal_places=3, default=0)
    dias_cobertura = models.IntegerField("Dias de Cobertura", default=1)
    estoque_seguranca = models.DecimalField("Estoque Segurança", max_digits=10, decimal_places=3, default=0)
    
    atualizado_em = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = 'Configuração de Transferência'
        verbose_name_plural = 'Configurações de Transferências'

    @property
    def capacidade_minima(self):
        """Ponto de Pedido/Transferência"""
        return (self.venda_media_diaria * self.dias_cobertura) + self.estoque_seguranca

class PedidoTransferencia(models.Model):
    """Separação Vila→Centro: um registro aberto por produto (status IMPRESSO)."""

    produto_externo_id = models.CharField(max_length=100, db_index=True)
    quantidade = models.DecimalField(max_digits=10, decimal_places=3)
    criado_em = models.DateTimeField(auto_now_add=True)
    lote_uuid = models.UUIDField(null=True, blank=True, db_index=True)
    status = models.CharField(max_length=20, default="IMPRESSO", db_index=True)
    impresso_em = models.DateTimeField(null=True, blank=True)


class HistoricoTransferencia(models.Model):
    """Auditoria: impressão de lote, transferências e cancelamentos de separação."""

    TIPO_LOTE_IMPRESSO = "LOTE_IMPRESSO"
    TIPO_TRANSFER_ITEM = "TRANSFER_VILA_ITEM"
    TIPO_TRANSFER_LOTE = "TRANSFER_VILA_LOTE"
    TIPO_CANCEL_SEP = "CANCELAR_SEPARACAO"

    tipo = models.CharField(max_length=32, db_index=True)
    criado_em = models.DateTimeField(auto_now_add=True, db_index=True)
    lote_uuid = models.UUIDField(null=True, blank=True, db_index=True)
    produto_externo_id = models.CharField(max_length=100, blank=True, db_index=True)
    quantidade = models.DecimalField(max_digits=10, decimal_places=3, null=True, blank=True)
    usuario_label = models.CharField(max_length=200, blank=True)
    observacao = models.TextField(blank=True)

    class Meta:
        ordering = ["-criado_em", "-id"]
        verbose_name = "Histórico de transferência"
        verbose_name_plural = "Históricos de transferências"


class PoliticaEstoque(models.Model):
    empresa = models.ForeignKey("base.Empresa", on_delete=models.CASCADE)
    loja = models.ForeignKey("base.Loja", on_delete=models.CASCADE)
    produto = models.ForeignKey("produtos.Produto", on_delete=models.CASCADE)

    estoque_seguranca = models.DecimalField(max_digits=12, decimal_places=3, default=0)
    dias_cobertura = models.DecimalField(max_digits=8, decimal_places=2, default=15)
    capacidade_maxima = models.DecimalField(
        max_digits=12, decimal_places=3, null=True, blank=True
    )

    estoque_minimo_manual = models.DecimalField(
        max_digits=12, decimal_places=3, null=True, blank=True
    )
    estoque_ideal_manual = models.DecimalField(
        max_digits=12, decimal_places=3, null=True, blank=True
    )

    permite_transferencia = models.BooleanField(default=True)
    permite_compra = models.BooleanField(default=True)
    prioridade_manual = models.IntegerField(default=0)

    class Meta:
        db_table = "politica_estoque"
        constraints = [
            models.UniqueConstraint(
                fields=["empresa", "loja", "produto"],
                name="uniq_politica_estoque_emp_loja_prod",
            )
        ]
        indexes = [
            models.Index(fields=["empresa", "loja", "produto"]),
        ]


class IndicadorProdutoLoja(models.Model):
    empresa = models.ForeignKey("base.Empresa", on_delete=models.CASCADE)
    loja = models.ForeignKey("base.Loja", on_delete=models.CASCADE)
    produto = models.ForeignKey("produtos.Produto", on_delete=models.CASCADE)

    data_base = models.DateField(db_index=True)

    saldo_atual = models.DecimalField(max_digits=12, decimal_places=3, default=0)
    venda_media_dia = models.DecimalField(max_digits=12, decimal_places=4, default=0)
    dias_sem_venda = models.IntegerField(default=9999)
    dias_cobertura_atual = models.DecimalField(max_digits=12, decimal_places=2, default=0)

    estoque_minimo = models.DecimalField(max_digits=12, decimal_places=3, default=0)
    estoque_ideal = models.DecimalField(max_digits=12, decimal_places=3, default=0)
    necessidade = models.DecimalField(max_digits=12, decimal_places=3, default=0)

    custo_medio = models.DecimalField(max_digits=12, decimal_places=4, default=0)
    preco_venda = models.DecimalField(max_digits=12, decimal_places=4, default=0)
    margem_bruta_pct = models.DecimalField(max_digits=8, decimal_places=2, default=0)

    score_prioridade = models.DecimalField(max_digits=12, decimal_places=4, default=0)
    classe_abc = models.CharField(max_length=1, blank=True, default="")
    classe_criticidade = models.CharField(max_length=20, blank=True, default="")

    sugestao_acao = models.CharField(max_length=30, blank=True, default="")
    qtd_transferir = models.DecimalField(max_digits=12, decimal_places=3, default=0)
    qtd_comprar = models.DecimalField(max_digits=12, decimal_places=3, default=0)
    loja_origem_sugerida = models.ForeignKey(
        "base.Loja",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="indicadores_como_origem_sugerida",
    )

    atualizado_em = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "indicador_produto_loja"
        constraints = [
            models.UniqueConstraint(
                fields=["empresa", "loja", "produto", "data_base"],
                name="uniq_indicador_emp_loja_prod_data",
            )
        ]
        indexes = [
            models.Index(fields=["empresa", "loja", "data_base"]),
            models.Index(fields=["empresa", "loja", "score_prioridade"]),
            models.Index(fields=["empresa", "loja", "classe_abc", "score_prioridade"]),
        ]


class ContagemCiclicaEscopo(models.TextChoices):
    LOJA = "loja", "Loja inteira"
    CATEGORIA = "categoria", "Categoria"
    CORREDOR = "corredor", "Corredor"


class ContagemCiclicaStatus(models.TextChoices):
    PASS1 = "pass1", "Passagem 1"
    PASS2 = "pass2", "Recontagem"
    FECHADA = "fechada", "Fechada"
    CANCELADA = "cancelada", "Cancelada"


class ContagemCiclicaSessao(models.Model):
    """Inventário cíclico multi-celular — estoque só grava no fechamento."""

    deposito = models.CharField(max_length=20, db_index=True)
    escopo_tipo = models.CharField(
        max_length=20,
        choices=ContagemCiclicaEscopo.choices,
        db_index=True,
    )
    escopo_valor = models.CharField(
        max_length=200,
        blank=True,
        default="",
        help_text="Categoria ou nome do corredor (vazio se loja inteira).",
    )
    dias_movimentacao = models.PositiveIntegerField(
        default=60,
        help_text="Só produtos com movimento Agro no depósito nos últimos N dias. 0 = sem filtro.",
    )
    status = models.CharField(
        max_length=20,
        choices=ContagemCiclicaStatus.choices,
        default=ContagemCiclicaStatus.PASS1,
        db_index=True,
    )
    passagem_atual = models.PositiveSmallIntegerField(default=1)
    total_itens = models.PositiveIntegerField(default=0)
    contados_pass1 = models.PositiveIntegerField(default=0)
    contados_pass2 = models.PositiveIntegerField(default=0)
    aberta_por_rotulo = models.CharField(max_length=120, blank=True, default="")
    aberta_por = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="contagens_ciclicas_abertas",
    )
    aberta_em = models.DateTimeField(auto_now_add=True)
    pass1_fechada_em = models.DateTimeField(null=True, blank=True)
    fechada_em = models.DateTimeField(null=True, blank=True)
    observacao = models.TextField(blank=True, default="")

    class Meta:
        ordering = ["-aberta_em"]
        indexes = [
            models.Index(fields=["deposito", "status"]),
            models.Index(fields=["deposito", "escopo_tipo", "escopo_valor", "status"]),
        ]

    def __str__(self):
        return f"#{self.pk} {self.deposito} {self.escopo_tipo}:{self.escopo_valor} · {self.status}"


class ContagemCiclicaLinha(models.Model):
    sessao = models.ForeignKey(
        ContagemCiclicaSessao,
        on_delete=models.CASCADE,
        related_name="linhas",
    )
    produto_externo_id = models.CharField(max_length=100, db_index=True)
    codigo_interno = models.CharField(max_length=100, blank=True, default="")
    nome_produto = models.CharField(max_length=255, blank=True, default="")
    categoria = models.CharField(max_length=200, blank=True, default="")
    saldo_referencia = models.DecimalField(max_digits=12, decimal_places=3, default=0)
    custo_ref = models.DecimalField(max_digits=12, decimal_places=2, default=0)
    qtd_pass1 = models.DecimalField(max_digits=12, decimal_places=3, null=True, blank=True)
    qtd_pass2 = models.DecimalField(max_digits=12, decimal_places=3, null=True, blank=True)
    qtd_final = models.DecimalField(max_digits=12, decimal_places=3, null=True, blank=True)
    contado_pass1 = models.BooleanField(default=False, db_index=True)
    contado_pass2 = models.BooleanField(default=False, db_index=True)
    precisa_recontagem = models.BooleanField(default=False, db_index=True)
    auto_zero_pass1 = models.BooleanField(default=False)
    operador_pass1 = models.CharField(max_length=120, blank=True, default="")
    operador_pass2 = models.CharField(max_length=120, blank=True, default="")
    contado_pass1_em = models.DateTimeField(null=True, blank=True)
    contado_pass2_em = models.DateTimeField(null=True, blank=True)
    ajuste_id = models.PositiveIntegerField(null=True, blank=True)

    class Meta:
        ordering = ["nome_produto", "id"]
        constraints = [
            models.UniqueConstraint(
                fields=["sessao", "produto_externo_id"],
                name="uniq_ciclica_sessao_produto",
            )
        ]
        indexes = [
            models.Index(fields=["sessao", "contado_pass1"]),
            models.Index(fields=["sessao", "precisa_recontagem"]),
        ]

    def __str__(self):
        return f"{self.produto_externo_id} · sessão {self.sessao_id}"


class ContagemCiclicaParticipante(models.Model):
    sessao = models.ForeignKey(
        ContagemCiclicaSessao,
        on_delete=models.CASCADE,
        related_name="participantes",
    )
    operador_rotulo = models.CharField(max_length=120)
    usuario = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="contagens_ciclicas_participacoes",
    )
    entrou_em = models.DateTimeField(auto_now_add=True)
    ultimo_ping_em = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["operador_rotulo"]
        constraints = [
            models.UniqueConstraint(
                fields=["sessao", "operador_rotulo"],
                name="uniq_ciclica_sessao_operador",
            )
        ]

    def __str__(self):
        return f"{self.operador_rotulo} · sessão {self.sessao_id}"


class SolicitacaoTransferenciaPdv(models.Model):
    """Pedido de transferência entre lojas feito no PDV (Centro ↔ Vila)."""

    STATUS_PENDENTE = "pendente"
    STATUS_ACEITO = "aceito"
    STATUS_PRONTO = "pronto"
    STATUS_CONCLUIDO = "concluido"
    STATUS_CANCELADO = "cancelado"
    STATUS_CHOICES = (
        (STATUS_PENDENTE, "Pendente"),
        (STATUS_ACEITO, "Aceito"),
        (STATUS_PRONTO, "Pronto"),
        (STATUS_CONCLUIDO, "Concluído"),
        (STATUS_CANCELADO, "Cancelado"),
    )
    STATUS_ABERTOS = (STATUS_PENDENTE, STATUS_ACEITO, STATUS_PRONTO)

    loja_origem = models.CharField(max_length=20, db_index=True)
    loja_destino = models.CharField(max_length=20, db_index=True)
    status = models.CharField(
        max_length=20,
        choices=STATUS_CHOICES,
        default=STATUS_PENDENTE,
        db_index=True,
    )
    observacao = models.CharField(max_length=400, blank=True, default="")
    criado_em = models.DateTimeField(auto_now_add=True, db_index=True)
    atualizado_em = models.DateTimeField(auto_now=True)

    criado_por_label = models.CharField(max_length=150, blank=True, default="")
    criado_por = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="solicitacoes_transf_pdv_criadas",
    )
    aceito_em = models.DateTimeField(null=True, blank=True)
    aceito_por_label = models.CharField(max_length=150, blank=True, default="")
    aceito_por = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="solicitacoes_transf_pdv_aceitas",
    )
    pronto_em = models.DateTimeField(null=True, blank=True)
    pronto_por_label = models.CharField(max_length=150, blank=True, default="")
    pronto_por = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="solicitacoes_transf_pdv_prontas",
    )
    concluido_em = models.DateTimeField(null=True, blank=True)
    concluido_por_label = models.CharField(max_length=150, blank=True, default="")
    concluido_por = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="solicitacoes_transf_pdv_concluidas",
    )
    cancelado_em = models.DateTimeField(null=True, blank=True)
    cancelado_por_label = models.CharField(max_length=150, blank=True, default="")
    cancelado_por = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="solicitacoes_transf_pdv_canceladas",
    )
    cancelado_motivo = models.CharField(max_length=300, blank=True, default="")

    class Meta:
        ordering = ["-criado_em", "-id"]
        verbose_name = "Solicitação de transferência PDV"
        verbose_name_plural = "Solicitações de transferência PDV"
        indexes = [
            models.Index(fields=["loja_origem", "status"]),
            models.Index(fields=["loja_destino", "status"]),
        ]

    def __str__(self):
        return f"#{self.pk} {self.loja_origem}→{self.loja_destino} {self.status}"


class SolicitacaoTransferenciaPdvItem(models.Model):
    solicitacao = models.ForeignKey(
        SolicitacaoTransferenciaPdv,
        on_delete=models.CASCADE,
        related_name="itens",
    )
    produto_externo_id = models.CharField(max_length=100, db_index=True)
    nome_produto = models.CharField(max_length=255)
    codigo_interno = models.CharField(max_length=100, blank=True, default="")
    quantidade = models.DecimalField(max_digits=10, decimal_places=3)
    # Qtd pedida original; `quantidade` vira a enviada ao transferir (pode ser menor / 0).
    quantidade_pedida = models.DecimalField(max_digits=10, decimal_places=3, default=0)

    class Meta:
        ordering = ["id"]
        verbose_name = "Item de solicitação PDV"
        verbose_name_plural = "Itens de solicitação PDV"

    def __str__(self):
        return f"{self.nome_produto} × {self.quantidade}"


class SolicitacaoTransferenciaPdvEvento(models.Model):
    solicitacao = models.ForeignKey(
        SolicitacaoTransferenciaPdv,
        on_delete=models.CASCADE,
        related_name="eventos",
    )
    acao = models.CharField(max_length=30, db_index=True)
    status_de = models.CharField(max_length=20, blank=True, default="")
    status_para = models.CharField(max_length=20, blank=True, default="")
    operador_label = models.CharField(max_length=150, blank=True, default="")
    operador = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="eventos_transf_pdv",
    )
    observacao = models.CharField(max_length=400, blank=True, default="")
    criado_em = models.DateTimeField(auto_now_add=True, db_index=True)

    class Meta:
        ordering = ["-criado_em", "-id"]
        verbose_name = "Evento de solicitação PDV"
        verbose_name_plural = "Eventos de solicitação PDV"
