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
    BAIXA_VENDA_PDV = "baixa_venda_pdv", "Baixa venda PDV"
    TRANSFERENCIA_UI = "transferencia_ui", "Transferência / tela"
    PLANILHA = "planilha", "Importação planilha"
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
