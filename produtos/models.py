from django.conf import settings
from django.contrib.auth.models import User
from django.core.validators import MaxValueValidator, MinValueValidator
from django.db import models
from django.utils import timezone


def compor_endereco_resumo_cliente(
    cep="",
    uf="",
    cidade="",
    bairro="",
    logradouro="",
    numero="",
    complemento="",
):
    """Uma linha para busca/lista; mesma ordem usada na importação Mongo/ERP."""
    parts = []
    l1 = ", ".join(
        x for x in (logradouro or "", numero or "") if str(x).strip()
    ).strip(", ")
    if l1:
        parts.append(l1)
    if (complemento or "").strip():
        parts.append(str(complemento).strip())
    if (bairro or "").strip():
        parts.append(str(bairro).strip())
    cb = "/".join(x for x in (cidade or "", uf or "") if str(x).strip())
    if cb:
        parts.append(cb)
    if (cep or "").strip():
        parts.append(f"CEP {str(cep).strip()}")
    return " · ".join(parts) if parts else ""


class Produto(models.Model):
    empresa = models.ForeignKey(
        'base.Empresa',
        on_delete=models.CASCADE,
        related_name='produtos',
        verbose_name='Empresa',
        null=True,
        blank=True,
    )
    codigo_interno = models.CharField(max_length=50)
    codigo_barras = models.CharField(max_length=50, blank=True, null=True)
    codigo_nfe = models.CharField(max_length=64, blank=True, default="")
    produto_externo_id = models.CharField(
        max_length=64,
        blank=True,
        null=True,
        db_index=True,
        unique=True,
        verbose_name="ID legado Mongo/ERP",
    )
    erp_produto_id = models.CharField(
        max_length=64,
        blank=True,
        default="",
        db_index=True,
        help_text="ID decimal/nativo para Pedidos/Salvar no ERP legado.",
    )
    nome = models.CharField(max_length=300)
    categoria = models.CharField(max_length=200, blank=True, null=True)
    marca = models.CharField(max_length=120, blank=True, null=True)
    modelo = models.CharField(
        max_length=200,
        blank=True,
        default="",
        verbose_name="Modelo",
        help_text="Modelo/versão do produto (cadastro SisVale).",
    )
    fornecedor_texto = models.CharField(max_length=300, blank=True, default="")
    subcategoria = models.CharField(max_length=200, blank=True, default="")
    subcategoria_2 = models.CharField(max_length=200, blank=True, default="")
    subcategoria_3 = models.CharField(max_length=200, blank=True, default="")
    subcategoria_4 = models.CharField(max_length=200, blank=True, default="")
    descricao = models.TextField(blank=True, default="")
    ncm = models.CharField(max_length=16, blank=True, default="")
    cadastro_inativo = models.BooleanField(default=False)
    cadastro_somente_agro = models.BooleanField(default=False)
    unidade = models.CharField(max_length=20, default='UN')
    custo = models.DecimalField(max_digits=10, decimal_places=2, default=0)
    preco_venda = models.DecimalField(max_digits=10, decimal_places=2, default=0)
    cashback_percentual = models.DecimalField(
        "Cashback (%)",
        max_digits=5,
        decimal_places=2,
        default=1,
        validators=[MinValueValidator(0), MaxValueValidator(100)],
        help_text="Percentual de cashback gerado na venda deste produto.",
    )
    ativo = models.BooleanField(default=True)
    criado_em = models.DateTimeField(auto_now_add=True)
    atualizado_em = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = 'Produto'
        verbose_name_plural = 'Produtos'
        ordering = ['nome']
        constraints = [
            models.UniqueConstraint(
                fields=['empresa', 'codigo_interno'],
                name='unique_codigo_interno_por_empresa',
            )
        ]

    def __str__(self):
        return f'{self.codigo_interno} - {self.nome}'


class ClienteAgro(models.Model):
    """Cliente cadastrado localmente no Agro (PDV / loja)."""

    nome = models.CharField(max_length=200)
    whatsapp = models.CharField("WhatsApp", max_length=20, blank=True, default="")
    cpf = models.CharField(max_length=14, blank=True, default="")
    endereco = models.CharField(
        max_length=500,
        blank=True,
        default="",
        verbose_name="Endereço (resumo)",
        help_text="Preenchido automaticamente a partir dos campos abaixo quando existirem.",
    )
    cep = models.CharField("CEP", max_length=12, blank=True, default="")
    uf = models.CharField("UF", max_length=2, blank=True, default="")
    cidade = models.CharField("Cidade", max_length=120, blank=True, default="")
    bairro = models.CharField("Bairro", max_length=120, blank=True, default="")
    logradouro = models.CharField("Logradouro", max_length=300, blank=True, default="")
    numero = models.CharField("Número", max_length=30, blank=True, default="")
    complemento = models.CharField(
        "Complemento",
        max_length=200,
        blank=True,
        default="",
    )
    plus_code = models.CharField(
        "Plus Code / local rural (Maps)",
        max_length=120,
        blank=True,
        default="",
        help_text="Ex.: 8X5R+7M9 Jacupiranga — abre direto no Google Maps na busca.",
    )
    referencia_rural = models.CharField(
        "Referência (entrega)",
        max_length=300,
        blank=True,
        default="",
        help_text="Porteira, km, cor — texto para o entregador; não compõe o link do Maps.",
    )
    maps_url_manual = models.CharField(
        "Link do Maps (colado)",
        max_length=600,
        blank=True,
        default="",
    )
    ativo = models.BooleanField(default=True)
    externo_id = models.CharField(
        max_length=80,
        blank=True,
        default="",
        db_index=True,
        verbose_name="ID externo (Mongo/ERP)",
        help_text="Chave da fonte; vazio = cadastro manual só no Agro.",
    )
    origem_import = models.CharField(
        max_length=20,
        blank=True,
        default="",
        verbose_name="Origem da importação",
        help_text="mongo, erp_api ou vazio (manual).",
    )
    editado_local = models.BooleanField(
        default=False,
        verbose_name="Editado no Agro",
        help_text="Se verdadeiro, sincronização não sobrescreve dados do cliente (incl. endereço).",
    )
    saldo_cashback = models.DecimalField(
        "Saldo cashback",
        max_digits=12,
        decimal_places=2,
        default=0,
    )
    saldo_vale_credito = models.DecimalField(
        "Saldo vale crédito",
        max_digits=12,
        decimal_places=2,
        default=0,
    )
    limite_fiado_local = models.DecimalField(
        "Limite fiado (local)",
        max_digits=12,
        decimal_places=2,
        default=0,
        help_text="Quando maior que zero, substitui o limite vindo do ERP/Mongo para este cliente.",
    )
    relacionamento_extras_json = models.JSONField(
        "Relacionamento (pets, saúde, anotações)",
        default=dict,
        blank=True,
        help_text="Pets, lembretes de saúde e anotações do F8 — fonte Agro (Postgres).",
    )
    criado_em = models.DateTimeField(auto_now_add=True)
    atualizado_em = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["nome"]
        verbose_name = "Cliente Agro"
        verbose_name_plural = "Clientes Agro"
        constraints = [
            models.UniqueConstraint(
                fields=["externo_id"],
                condition=models.Q(externo_id__gt=""),
                name="unique_clienteagro_externo_id_quando_preenchido",
            ),
        ]

    def _tem_campos_endereco_estruturados(self) -> bool:
        return any(
            (getattr(self, f) or "").strip()
            for f in (
                "cep",
                "uf",
                "cidade",
                "bairro",
                "logradouro",
                "numero",
                "complemento",
            )
        )

    def clean(self):
        from produtos.cliente_whatsapp_util import validar_whatsapp_modelo

        super().clean()
        validar_whatsapp_modelo(self)

    def save(self, *args, **kwargs):
        from produtos.cliente_whatsapp_util import extrair_whatsapp_digits

        self.whatsapp = extrair_whatsapp_digits(self.whatsapp)
        uf_kw = kwargs.get("update_fields")
        if self._tem_campos_endereco_estruturados():
            self.endereco = compor_endereco_resumo_cliente(
                self.cep,
                self.uf,
                self.cidade,
                self.bairro,
                self.logradouro,
                self.numero,
                self.complemento,
            )[:500]
            if uf_kw is not None:
                uf_kw = list(uf_kw)
                if "endereco" not in uf_kw:
                    uf_kw.append("endereco")
                kwargs["update_fields"] = uf_kw
        super().save(*args, **kwargs)

    def __str__(self):
        return self.nome


class ClienteAgroEventoAgro(models.Model):
    """Log de operações no cadastro do cliente (PIN + histórico)."""

    class Tipo(models.TextChoices):
        LIMPAR_WHATSAPP = "limpar_whatsapp", "Limpar telefone"
        TRANSFERIR_SALDOS = "transferir_saldos", "Transferir cashback/vale"
        EXCLUIR = "excluir", "Excluir cadastro"
        VALE_MANUAL = "vale_manual", "Vale crédito manual"
        VALE_PAGO = "vale_pago", "Vale crédito pago (caixa)"

    tipo = models.CharField(max_length=24, choices=Tipo.choices, db_index=True)
    cliente_agro = models.ForeignKey(
        ClienteAgro,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="eventos_cadastro",
    )
    cliente_pk_snap = models.PositiveIntegerField(null=True, blank=True, db_index=True)
    cliente_nome_snap = models.CharField(max_length=200, blank=True, default="")
    destino_agro = models.ForeignKey(
        ClienteAgro,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="eventos_cadastro_destino",
    )
    destino_pk_snap = models.PositiveIntegerField(null=True, blank=True)
    destino_nome_snap = models.CharField(max_length=200, blank=True, default="")
    payload_json = models.JSONField(default=dict, blank=True)
    usuario = models.CharField(max_length=150, blank=True, default="")
    pin_operador = models.CharField(max_length=150, blank=True, default="")
    origem_tela = models.CharField(max_length=32, blank=True, default="")
    criado_em = models.DateTimeField(auto_now_add=True, db_index=True)

    class Meta:
        ordering = ["-criado_em"]
        verbose_name = "Evento cadastro cliente"
        verbose_name_plural = "Eventos cadastro cliente"
        indexes = [
            models.Index(fields=["cliente_pk_snap", "-criado_em"], name="cli_evt_pk_dt_idx"),
        ]

    def __str__(self):
        return f"{self.get_tipo_display()} · {self.cliente_nome_snap or self.pk}"


class SessaoCaixa(models.Model):
    """Turno de caixa: abertura com fundo de troco; vendas podem ser vinculadas até o fechamento."""

    class PontoCaixa(models.TextChoices):
        GAVETA = "gaveta", "Caixa Gaveta (Centro)"
        VILA = "vila", "Caixa Vila Elias"
        NOTEBOOK = "notebook", "Caixa Notebook"
        TESTE = "teste", "Caixa Teste"

    usuario = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="sessoes_caixa",
        help_text="Login Django de quem abriu o turno.",
    )
    usuario_fechamento = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="sessoes_caixa_fechadas",
        help_text="Login Django de quem fechou o turno.",
    )
    aberto_em = models.DateTimeField(auto_now_add=True)
    valor_abertura = models.DecimalField(max_digits=12, decimal_places=2, default=0)
    valor_abertura_sugerido = models.DecimalField(
        max_digits=12,
        decimal_places=2,
        null=True,
        blank=True,
        help_text="Dinheiro contado no último fechamento do mesmo ponto (sugestão na abertura).",
    )
    diferenca_abertura = models.DecimalField(
        max_digits=12,
        decimal_places=2,
        null=True,
        blank=True,
        help_text="valor_abertura − valor_abertura_sugerido (só quando havia sugestão).",
    )
    observacao_abertura = models.CharField(max_length=500, blank=True, default="")
    fechado_em = models.DateTimeField(null=True, blank=True)
    valor_fechamento = models.DecimalField(
        max_digits=12, decimal_places=2, null=True, blank=True
    )
    observacao_fechamento = models.CharField(max_length=500, blank=True, default="")
    conferencia_fechamento = models.JSONField(
        null=True,
        blank=True,
        help_text="Conferência por forma: {forma: {esperado, contado, diferenca}}.",
    )
    ponto_caixa = models.CharField(
        max_length=16,
        choices=PontoCaixa.choices,
        default=PontoCaixa.GAVETA,
        db_index=True,
        help_text="Ponto físico do turno: gaveta Centro, Vila Elias, notebook (satélite) ou teste.",
    )
    sessao_principal = models.ForeignKey(
        "self",
        on_delete=models.PROTECT,
        null=True,
        blank=True,
        related_name="pontos_vinculados",
        help_text="Turno principal (Caixa Gaveta) quando este registro for satélite.",
    )

    class Meta:
        ordering = ["-aberto_em"]
        verbose_name = "Sessão de caixa"
        verbose_name_plural = "Sessões de caixa"

    def __str__(self):
        dt = self.aberto_em.strftime("%d/%m/%Y %H:%M") if self.aberto_em else ""
        rotulo = self.get_ponto_caixa_display()
        return f"{rotulo} #{self.pk} — {dt}"


class MovimentoCaixa(models.Model):
    """Reforço ou retirada manual no turno, por forma de pagamento."""

    class Tipo(models.TextChoices):
        REFORCO = "reforco", "Reforço"
        RETIRADA = "retirada", "Retirada"

    sessao_caixa = models.ForeignKey(
        SessaoCaixa,
        on_delete=models.CASCADE,
        related_name="movimentos",
    )
    tipo = models.CharField(max_length=12, choices=Tipo.choices, db_index=True)
    forma_pagamento = models.CharField(max_length=80)
    valor = models.DecimalField(max_digits=12, decimal_places=2)
    observacao = models.CharField(max_length=500, blank=True, default="")
    criado_em = models.DateTimeField(auto_now_add=True)
    usuario = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="movimentos_caixa",
    )

    class Meta:
        ordering = ["-criado_em"]
        verbose_name = "Movimento de caixa"
        verbose_name_plural = "Movimentos de caixa"

    def __str__(self):
        return f"{self.get_tipo_display()} {self.forma_pagamento} R$ {self.valor}"


class VendaAgro(models.Model):
    """Venda registrada pelo PDV Agro (fonte local); orçamento pode ser espelhado no ERP."""

    class ErpSyncStatus(models.TextChoices):
        PENDENTE = "pendente", "Aguardando ERP"
        ACEITO = "aceito", "Aceito no ERP"
        RECUSADO_ERP = "recusado_erp", "Recusado pelo ERP"
        FALHA_COMUNICACAO = "falha_comunicacao", "Falha na comunicação"

    cliente_nome = models.CharField(max_length=300, blank=True, default="")
    cliente_id_erp = models.CharField(max_length=32, blank=True, default="")
    cliente_documento = models.CharField(max_length=20, blank=True, default="")
    total = models.DecimalField(max_digits=12, decimal_places=2, default=0)
    frete = models.DecimalField(
        max_digits=12,
        decimal_places=2,
        default=0,
        help_text="Taxa de entrega cobrada na venda (compõe o total).",
    )
    forma_pagamento = models.CharField(max_length=80, blank=True, default="")
    pagamentos_json = models.JSONField(
        null=True,
        blank=True,
        help_text="Parcelas por forma [{forma, valor}] quando a venda tem mais de um pagamento.",
    )
    fiado_cronograma_json = models.JSONField(
        default=list,
        blank=True,
        help_text="Parcelas do fiado [{parcela, dias, vencimento, valor}] para envio manual ao ERP.",
    )
    erp_sync_status = models.CharField(
        max_length=24,
        choices=ErpSyncStatus.choices,
        blank=True,
        default="",
        db_index=True,
        help_text="Resultado do envio ao ERP (Pedidos/Salvar). Vazio = registro antigo antes deste campo.",
    )
    enviado_erp = models.BooleanField(default=False)
    erp_http_status = models.PositiveIntegerField(null=True, blank=True)
    erp_resposta = models.JSONField(null=True, blank=True)
    erp_envio_log_json = models.JSONField(
        null=True,
        blank=True,
        help_text="Histórico de tentativas/reversões de envio manual ao ERP [{ts, acao, ok, ...}].",
    )
    usuario_registro = models.CharField(max_length=150, blank=True, default="")
    sessao_caixa = models.ForeignKey(
        SessaoCaixa,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="vendas",
    )
    criado_em = models.DateTimeField(auto_now_add=True)
    estoque_baixa_agro_aplicada = models.BooleanField(
        default=False,
        db_index=True,
        help_text="Se True, já foi registrada baixa de estoque na camada Agro (AjusteRapidoEstoque) para esta venda.",
    )
    deposito = models.CharField(
        max_length=16,
        blank=True,
        default="centro",
        db_index=True,
        help_text="Depósito da baixa de estoque nesta venda: centro | vila (Vila Elias).",
    )
    devolvida_em = models.DateTimeField(
        null=True,
        blank=True,
        db_index=True,
        help_text="Quando preenchido, a venda foi devolvida (estoque e saída no caixa).",
    )
    devolucao_motivo = models.TextField(blank=True, default="")
    devolucao_pagamentos_json = models.JSONField(
        null=True,
        blank=True,
        help_text="Formas e valores devolvidos ao cliente [{forma, valor}].",
    )
    devolucao_movimento_caixa_ids = models.JSONField(
        null=True,
        blank=True,
        help_text="IDs de MovimentoCaixa (retirada) gerados na devolução.",
    )
    devolucao_usuario = models.CharField(max_length=150, blank=True, default="")
    frete_devolvido = models.DecimalField(
        max_digits=12,
        decimal_places=2,
        default=0,
        help_text="Soma do frete já devolvido (parcial ou total).",
    )
    nfce_solicitada = models.BooleanField(
        default=False,
        db_index=True,
        help_text="PDV pediu cupom fiscal (NFC-e) nesta venda.",
    )

    class Meta:
        ordering = ["-criado_em"]
        verbose_name = "Venda Agro"
        verbose_name_plural = "Vendas Agro"
        indexes = [
            models.Index(fields=["-criado_em"], name="vendaagro_criado_em_idx"),
        ]

    def __str__(self):
        return f"Venda #{self.pk} — {self.cliente_nome[:40]} — R$ {self.total}"

    @property
    def erp_sync_efetivo(self) -> str:
        """Valor de exibição para registros sem `erp_sync_status` (legado)."""
        s = (self.erp_sync_status or "").strip()
        if s:
            return s
        return self.ErpSyncStatus.ACEITO if self.enviado_erp else self.ErpSyncStatus.FALHA_COMUNICACAO

    @property
    def devolvida(self) -> bool:
        return self.devolvida_em is not None

    @property
    def tem_devolucao_parcial(self) -> bool:
        """Há ao menos um evento de devolução, mas a venda ainda não está totalmente devolvida."""
        if self.devolvida_em is not None:
            return False
        try:
            return self.devolucoes.exists()
        except Exception:
            return False

    def tem_fiado(self) -> bool:
        from produtos.fiado_credito_util import venda_local_tem_fiado

        return venda_local_tem_fiado(self)

    def fiado_aguarda_envio_erp(self) -> bool:
        return (
            self.tem_fiado()
            and (self.erp_sync_status or "") == self.ErpSyncStatus.PENDENTE
            and not self.enviado_erp
        )


class NfceNumeracaoAgro(models.Model):
    """Controle sequencial NFC-e — um contador por CNPJ emitente + série."""

    emitente_cnpj = models.CharField(
        max_length=14,
        blank=True,
        default="",
        db_index=True,
        help_text="CNPJ do emitente (Centro ou Vila). Numeração independente por CNPJ.",
    )
    serie = models.PositiveSmallIntegerField(default=1)
    proximo_numero = models.PositiveIntegerField(default=1)
    atualizado_em = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Numeração NFC-e"
        verbose_name_plural = "Numerações NFC-e"
        constraints = [
            models.UniqueConstraint(
                fields=["emitente_cnpj", "serie"],
                name="nfce_numeracao_cnpj_serie_uniq",
            ),
        ]

    def __str__(self):
        return f"{self.emitente_cnpj or '?'} · Série {self.serie} — próximo nº {self.proximo_numero}"


class NfceDocumentoAgro(models.Model):
    """NFC-e (modelo 65) emitida pelo PDV Agro — XML autorizado para arquivo mensal."""

    class Status(models.TextChoices):
        AUTORIZADA = "autorizada", "Autorizada"
        CANCELADA = "cancelada", "Cancelada"
        REJEITADA = "rejeitada", "Rejeitada"
        ERRO = "erro", "Erro técnico"

    venda = models.OneToOneField(
        VendaAgro,
        on_delete=models.CASCADE,
        related_name="nfce",
    )
    status = models.CharField(max_length=16, choices=Status.choices, db_index=True)
    chave = models.CharField(max_length=44, blank=True, default="", db_index=True)
    numero = models.PositiveIntegerField(default=0)
    serie = models.PositiveSmallIntegerField(default=1)
    emitente_cnpj = models.CharField(
        max_length=14,
        blank=True,
        default="",
        db_index=True,
        help_text="CNPJ no XML emit (Centro /0001 ou Vila /0002).",
    )
    protocolo = models.CharField(max_length=20, blank=True, default="")
    dest_cpf = models.CharField(
        max_length=14,
        blank=True,
        default="",
        help_text="CPF (11) ou CNPJ (14) do destinatário na NFC-e.",
    )
    consumidor_sem_identificacao = models.BooleanField(default=False)
    xml_autorizado = models.TextField(blank=True, default="")
    qr_code_url = models.TextField(blank=True, default="")
    mensagem_sefaz = models.TextField(blank=True, default="")
    tp_amb = models.PositiveSmallIntegerField(default=2, help_text="1 produção · 2 homologação")
    criado_em = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["-criado_em"]
        verbose_name = "NFC-e Agro"
        verbose_name_plural = "NFC-e Agro"

    def __str__(self):
        return f"NFC-e {self.chave or self.pk} — venda #{self.venda_id}"


class FiadoTituloAgro(models.Model):
    """Título de crédito loja (fiado) — parcela ou venda PDV / importação ERP."""

    class Situacao(models.TextChoices):
        ABERTO = "aberto", "Em aberto"
        PARCIAL = "parcial", "Pago parcialmente"
        QUITADO = "quitado", "Quitado"
        CANCELADO = "cancelado", "Cancelado"

    class Origem(models.TextChoices):
        PDV = "pdv", "PDV"
        IMPORTACAO = "importacao", "Importação"

    chave_unica = models.CharField(
        max_length=120,
        unique=True,
        db_index=True,
        help_text="Chave idempotente (pdv:… ou import:…) para evitar duplicata.",
    )
    cliente_agro = models.ForeignKey(
        ClienteAgro,
        on_delete=models.PROTECT,
        null=True,
        blank=True,
        related_name="fiado_titulos",
    )
    venda_agro = models.ForeignKey(
        VendaAgro,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="fiado_titulos",
    )
    cliente_nome = models.CharField(max_length=300)
    cliente_codigo = models.CharField(
        max_length=32,
        blank=True,
        default="",
        db_index=True,
        help_text="Código ERP / planilha quando existir.",
    )
    numero_documento = models.CharField(max_length=80, blank=True, default="")
    parcela_num = models.PositiveSmallIntegerField(default=1)
    parcela_total = models.PositiveSmallIntegerField(default=1)
    vencimento = models.DateField(db_index=True)
    valor_bruto = models.DecimalField(max_digits=12, decimal_places=2)
    valor_pago = models.DecimalField(max_digits=12, decimal_places=2, default=0)
    situacao = models.CharField(
        max_length=12,
        choices=Situacao.choices,
        default=Situacao.ABERTO,
        db_index=True,
    )
    origem = models.CharField(
        max_length=16,
        choices=Origem.choices,
        default=Origem.PDV,
        db_index=True,
    )
    descricao = models.CharField(max_length=500, blank=True, default="")
    dados_snapshot_json = models.JSONField(default=dict, blank=True)
    criado_em = models.DateTimeField(auto_now_add=True)
    atualizado_em = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["vencimento", "pk"]
        verbose_name = "Título fiado"
        verbose_name_plural = "Títulos fiado"

    def __str__(self):
        return f"{self.cliente_nome[:30]} · {self.numero_documento or self.pk} · R$ {self.valor_bruto}"

    @property
    def saldo_aberto(self):
        from decimal import Decimal

        return max(
            Decimal("0"),
            (self.valor_bruto - self.valor_pago).quantize(Decimal("0.01")),
        )


class FiadoBaixaAgro(models.Model):
    """Pagamento (baixa total ou parcial) de título fiado."""

    titulo = models.ForeignKey(
        FiadoTituloAgro,
        on_delete=models.PROTECT,
        related_name="baixas",
    )
    valor = models.DecimalField(max_digits=12, decimal_places=2)
    forma_pagamento = models.CharField(max_length=80)
    sessao_caixa = models.ForeignKey(
        SessaoCaixa,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="fiado_baixas",
    )
    movimento_caixa = models.ForeignKey(
        MovimentoCaixa,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="fiado_baixas",
    )
    usuario = models.CharField(max_length=150, blank=True, default="")
    observacao = models.CharField(max_length=500, blank=True, default="")
    criado_em = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["-criado_em"]
        verbose_name = "Baixa fiado"
        verbose_name_plural = "Baixas fiado"

    def __str__(self):
        return f"Baixa R$ {self.valor} — título #{self.titulo_id}"


class FiadoEventoAgro(models.Model):
    """Log append-only (backup/auditoria) de alterações no fiado."""

    class Tipo(models.TextChoices):
        TITULO_CRIADO = "titulo_criado", "Título criado"
        BAIXA = "baixa", "Baixa"
        LIMITE = "limite", "Limite alterado"
        CANCELAMENTO = "cancelamento", "Cancelamento"
        IMPORT = "import", "Importação"

    tipo = models.CharField(max_length=24, choices=Tipo.choices, db_index=True)
    cliente_agro = models.ForeignKey(
        ClienteAgro,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="fiado_eventos",
    )
    titulo = models.ForeignKey(
        FiadoTituloAgro,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="eventos",
    )
    baixa = models.ForeignKey(
        FiadoBaixaAgro,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="eventos",
    )
    payload_json = models.JSONField(default=dict, blank=True)
    usuario = models.CharField(max_length=150, blank=True, default="")
    criado_em = models.DateTimeField(auto_now_add=True, db_index=True)

    class Meta:
        ordering = ["-criado_em"]
        verbose_name = "Evento fiado"
        verbose_name_plural = "Eventos fiado"

    def __str__(self):
        return f"{self.get_tipo_display()} · {self.criado_em:%d/%m/%Y %H:%M}"


class TituloFinanceiroAgro(models.Model):
    """Título CP/CR no Postgres — espelho de ``DtoLancamento`` (desvinculação ERP).

    Telas de Lançamentos ainda leem Mongo; este modelo é alimentado por importação
    (``importar_titulos_financeiro_mongo_pg``) até ``AGRO_FONTE_FINANCEIRO=agro_pg``.
    """

    mongo_id = models.CharField(
        max_length=32,
        unique=True,
        db_index=True,
        help_text="ObjectId string do DtoLancamento (idempotência).",
    )
    despesa = models.BooleanField(
        db_index=True,
        help_text="True = contas a pagar; False = contas a receber.",
    )
    descricao = models.CharField(max_length=500, blank=True, default="")
    cliente = models.CharField(max_length=300, blank=True, default="")
    cliente_id = models.CharField(max_length=32, blank=True, default="", db_index=True)
    numero_documento = models.CharField(max_length=80, blank=True, default="", db_index=True)
    parcela = models.PositiveSmallIntegerField(default=0)
    plano_conta = models.CharField(max_length=200, blank=True, default="", db_index=True)
    plano_conta_id = models.CharField(max_length=32, blank=True, default="")
    grupo = models.CharField(max_length=200, blank=True, default="")
    forma_pagamento = models.CharField(max_length=120, blank=True, default="")
    forma_pagamento_id = models.CharField(max_length=32, blank=True, default="")
    banco = models.CharField(max_length=120, blank=True, default="")
    banco_id = models.CharField(max_length=32, blank=True, default="")
    centro_custo = models.CharField(max_length=120, blank=True, default="")
    empresa = models.CharField(max_length=200, blank=True, default="")
    observacoes = models.TextField(blank=True, default="")
    valor_bruto = models.DecimalField(max_digits=14, decimal_places=2)
    valor_pago = models.DecimalField(max_digits=14, decimal_places=2, default=0)
    valor_restante = models.DecimalField(max_digits=14, decimal_places=2, default=0)
    quitado = models.BooleanField(default=False, db_index=True)
    data_vencimento = models.DateField(null=True, blank=True, db_index=True)
    data_competencia = models.DateField(null=True, blank=True, db_index=True)
    data_fluxo = models.DateField(null=True, blank=True, db_index=True)
    data_pagamento = models.DateField(null=True, blank=True, db_index=True)
    agro_recorrente = models.BooleanField(default=False)
    recorrencia_intervalo_meses = models.PositiveSmallIntegerField(default=1)
    agro_recorrente_sempre = models.BooleanField(default=False)
    boleto_codigo_barras = models.CharField(max_length=54, blank=True, default="")
    usuario_lancou = models.CharField(max_length=150, blank=True, default="")
    usuario_quitou = models.CharField(max_length=150, blank=True, default="")
    modificado_por = models.CharField(max_length=200, blank=True, default="")
    criado_por = models.CharField(max_length=200, blank=True, default="")
    mongo_congelado = models.BooleanField(
        default=False,
        help_text="``AgroFonteVerdade`` no documento Mongo.",
    )
    mongo_ultima_atualizacao = models.DateTimeField(null=True, blank=True)
    dados_snapshot_json = models.JSONField(default=dict, blank=True)
    importado_em = models.DateTimeField(auto_now_add=True)
    atualizado_em = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-data_vencimento", "pk"]
        verbose_name = "Título financeiro Agro"
        verbose_name_plural = "Títulos financeiros Agro"
        indexes = [
            models.Index(fields=["despesa", "quitado", "data_vencimento"]),
        ]

    def __str__(self):
        tipo = "CP" if self.despesa else "CR"
        return f"{tipo} · {self.descricao[:40] or self.numero_documento or self.mongo_id}"


class PlanoUnificacaoLoteAgro(models.Model):
    """Backup do último apply de unificação de plano_conta (permite reverter)."""

    class Status(models.TextChoices):
        APLICADO = "aplicado", "Aplicado"
        REVERTIDO = "revertido", "Revertido"

    criado_em = models.DateTimeField(auto_now_add=True)
    usuario = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="plano_unificacao_lotes",
    )
    n_titulos = models.PositiveIntegerField(default=0)
    status = models.CharField(
        max_length=16,
        choices=Status.choices,
        default=Status.APLICADO,
        db_index=True,
    )
    alteracoes = models.JSONField(
        default=list,
        help_text="Lista {mongo_id, de, para} antes de renomear.",
    )
    revertido_em = models.DateTimeField(null=True, blank=True)
    revertido_por = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="plano_unificacao_reversoes",
    )

    class Meta:
        ordering = ["-criado_em"]
        verbose_name = "Lote unificação planos CP"
        verbose_name_plural = "Lotes unificação planos CP"

    def __str__(self):
        return f"{self.criado_em:%d/%m/%Y %H:%M} · {self.n_titulos} tít. · {self.status}"


class PlanoContaAgro(models.Model):
    """Cadastro oficial de planos de despesa (CP) — Postgres, sem Mongo.

    Títulos antigos podem ter grafias diferentes; aliases mapeiam sem apagar dados.
    """

    class Tipo(models.TextChoices):
        FIXA = "fixa", "Fixa"
        VARIAVEL = "variavel", "Variável"
        OUTRA = "outra", "Outra"

    nome = models.CharField(max_length=200, unique=True, db_index=True)
    tipo = models.CharField(
        max_length=16,
        choices=Tipo.choices,
        default=Tipo.OUTRA,
        blank=True,
    )
    grupo = models.CharField(max_length=120, blank=True, default="")
    observacao = models.CharField(max_length=400, blank=True, default="")
    ativo = models.BooleanField(default=True, db_index=True)
    exibir_pdv = models.BooleanField(
        default=False,
        db_index=True,
        verbose_name="Mostrar no PDV",
        help_text="Se marcado, aparece no select de plano da saída/retirada do caixa.",
    )
    criado_em = models.DateTimeField(auto_now_add=True)
    atualizado_em = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["nome"]
        verbose_name = "Plano de conta Agro"
        verbose_name_plural = "Planos de conta Agro"

    def __str__(self):
        return self.nome


class PlanoContaAliasAgro(models.Model):
    """Grafia encontrada em títulos → plano oficial (sem alterar ``TituloFinanceiroAgro``)."""

    grafia = models.CharField(max_length=200, unique=True, db_index=True)
    plano = models.ForeignKey(
        PlanoContaAgro,
        on_delete=models.CASCADE,
        related_name="aliases",
    )
    criado_em = models.DateTimeField(auto_now_add=True)
    criado_por = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="plano_conta_aliases",
    )

    class Meta:
        ordering = ["grafia"]
        verbose_name = "Alias plano de conta"
        verbose_name_plural = "Aliases plano de conta"

    def __str__(self):
        return f"{self.grafia} → {self.plano_id}"


class EntradaNotaRascunhoAgro(models.Model):
    """Rascunho do assistente Entrada NF (etapas 1–6); substitui ``AgroEntradaNotaRascunho`` no Mongo."""

    rascunho_id = models.CharField(max_length=24, primary_key=True)
    status = models.CharField(max_length=40, db_index=True, default="com_pendencias")
    modo = models.CharField(max_length=40, blank=True, default="manual")
    usuario = models.CharField(max_length=200, blank=True, default="")
    usuario_ultima_alteracao = models.CharField(max_length=200, blank=True, default="")
    usuario_estoque_aplicado = models.CharField(max_length=200, blank=True, default="")
    xml_chave = models.CharField(max_length=44, blank=True, default="")
    cabecalho = models.JSONField(default=dict, blank=True)
    linhas = models.JSONField(default=list, blank=True)
    extra = models.JSONField(default=dict, blank=True)
    criado_em = models.DateTimeField(db_index=True)
    atualizado_em = models.DateTimeField(null=True, blank=True, db_index=True)
    estoque_aplicado_em = models.DateTimeField(null=True, blank=True)

    class Meta:
        ordering = ["-criado_em"]
        verbose_name = "Rascunho Entrada NF Agro"
        verbose_name_plural = "Rascunhos Entrada NF Agro"
        indexes = [
            models.Index(fields=["-criado_em"]),
            models.Index(fields=["status", "-atualizado_em"]),
        ]

    def __str__(self):
        cab = self.cabecalho if isinstance(self.cabecalho, dict) else {}
        nf = str(cab.get("numero") or "").strip() or "—"
        return f"Entrada NF {nf} · {self.rascunho_id[:8]}…"


class AgroNfeDistDfeCursor(models.Model):
    """Cursor ultNSU da Dist DF-e (SEFAZ) por CNPJ — sobrevive a restart (Postgres)."""

    cnpj = models.CharField(max_length=14, unique=True, db_index=True)
    ult_nsu = models.CharField(max_length=15, default="000000000000000")
    atualizado_em = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Cursor Dist DF-e"
        verbose_name_plural = "Cursores Dist DF-e"

    def __str__(self):
        return f"{self.cnpj} · NSU {self.ult_nsu}"

class AgroNfeDistDfeDocumento(models.Model):
    """Caixa de entrada Dist DF-e — notas puxadas da SEFAZ."""

    class Schema(models.TextChoices):
        NFE = "nfe", "NF-e completa"
        RESUMO = "resumo", "Resumo"
        OUTRO = "outro", "Outro"

    class Status(models.TextChoices):
        PENDENTE = "pendente", "Pendente"
        CARREGADA = "carregada", "Carregada na grade"
        PROCESSADA = "processada", "Entrada concluída"
        IGNORADA = "ignorada", "Ignorada"

    cnpj = models.CharField(max_length=14, db_index=True)
    chave = models.CharField(max_length=44, db_index=True)
    nsu = models.CharField(max_length=15, blank=True, default="", db_index=True)
    schema = models.CharField(
        max_length=16, choices=Schema.choices, default=Schema.NFE, db_index=True
    )
    xml = models.TextField(blank=True, default="")
    emit_nome = models.CharField(max_length=300, blank=True, default="")
    numero = models.CharField(max_length=20, blank=True, default="")
    valor_total = models.DecimalField(max_digits=14, decimal_places=2, default=0)
    dh_emi = models.CharField(max_length=40, blank=True, default="")
    status = models.CharField(
        max_length=16, choices=Status.choices, default=Status.PENDENTE, db_index=True
    )
    rascunho_id = models.CharField(max_length=64, blank=True, default="")
    manifestacao_status = models.CharField(max_length=20, blank=True, default="", db_index=True)
    manifestacao_protocolo = models.CharField(max_length=30, blank=True, default="")
    manifestacao_mensagem = models.CharField(max_length=500, blank=True, default="")
    manifestacao_em = models.DateTimeField(null=True, blank=True)
    criado_em = models.DateTimeField(auto_now_add=True, db_index=True)
    atualizado_em = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Documento Dist DF-e"
        verbose_name_plural = "Documentos Dist DF-e"
        constraints = [
            models.UniqueConstraint(fields=["cnpj", "chave"], name="uniq_dfe_doc_cnpj_chave"),
        ]
        indexes = [
            models.Index(fields=["cnpj", "status", "-criado_em"]),
        ]

    def __str__(self):
        return f"DF-e {self.numero or self.chave[:8]} · {self.status}"


class EntradaNfeVinculoAgro(models.Model):
    """Vínculo cProd/descrição da NF → produto SisVale (Postgres, multi-PC).

    Fonte da verdade do auto-casamento no «Ler XML». Mongo/overlay são espelho opcional.
    """

    class Tipo(models.TextChoices):
        C_PROD = "c_prod", "Código fornecedor (cProd)"
        X_PROD = "x_prod", "Descrição (xProd)"

    tipo = models.CharField(max_length=16, choices=Tipo.choices, db_index=True)
    chave = models.CharField(max_length=120, db_index=True)
    emit_cnpj = models.CharField(max_length=14, blank=True, default="", db_index=True)
    produto_externo_id = models.CharField(max_length=64, db_index=True)
    nome_catalogo = models.CharField(max_length=300, blank=True, default="")
    criado_em = models.DateTimeField(auto_now_add=True)
    atualizado_em = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Vínculo Entrada NF Agro"
        verbose_name_plural = "Vínculos Entrada NF Agro"
        constraints = [
            models.UniqueConstraint(
                fields=["tipo", "chave", "emit_cnpj"],
                name="entrada_nfe_vinculo_tipo_chave_cnpj_uq",
            ),
        ]
        indexes = [
            models.Index(fields=["tipo", "chave"], name="entrada_nfe_vinculo_tipo_chave"),
        ]

    def __str__(self):
        cnpj = self.emit_cnpj or "—"
        return f"{self.tipo}:{self.chave} · {cnpj} → {self.produto_externo_id}"


class PdvMercadoPagoPointOrder(models.Model):
    """Pedido Point criado a partir do PDV; após pagamento no terminal, dispara Pedidos/Salvar."""

    class Status(models.TextChoices):
        PENDING = "pending", "Aguardando pagamento"
        PAID = "paid", "Pago no terminal"
        ABANDONED = "abandoned", "Abandonado pelo operador"
        FINALIZED = "finalized", "Finalizado (ERP)"
        FAILED = "failed", "Falha"

    external_reference = models.CharField(max_length=64, unique=True, db_index=True)
    mp_order_id = models.CharField(max_length=80, db_index=True)
    valor_cobrado = models.DecimalField(max_digits=12, decimal_places=2)
    erp_payload = models.JSONField()
    django_session_key = models.CharField(max_length=50, blank=True, default="")
    status = models.CharField(
        max_length=20,
        choices=Status.choices,
        default=Status.PENDING,
        db_index=True,
    )
    venda = models.ForeignKey(
        "VendaAgro",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="pedidos_mp_point",
    )
    mp_last_status = models.CharField(max_length=48, blank=True, default="")
    criado_em = models.DateTimeField(auto_now_add=True)
    atualizado_em = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-criado_em"]
        verbose_name = "Pedido Mercado Pago Point (PDV)"
        verbose_name_plural = "Pedidos Mercado Pago Point (PDV)"

    def __str__(self):
        return f"MP Point {self.external_reference} — {self.status}"


class ItemVendaAgro(models.Model):
    venda = models.ForeignKey(
        VendaAgro,
        on_delete=models.CASCADE,
        related_name="itens",
    )
    produto_id_externo = models.CharField(max_length=64, blank=True, default="")
    codigo = models.CharField(max_length=120, blank=True, default="")
    descricao = models.CharField(max_length=500, blank=True, default="")
    quantidade = models.DecimalField(max_digits=14, decimal_places=4, default=0)
    valor_unitario = models.DecimalField(max_digits=12, decimal_places=4, default=0)
    valor_total = models.DecimalField(max_digits=12, decimal_places=2, default=0)
    unidade = models.CharField(
        max_length=12,
        blank=True,
        default="UN",
        help_text="UN, KG, etc. — usado na NFC-e (uCom/uTrib).",
    )
    quantidade_devolvida = models.DecimalField(
        max_digits=14,
        decimal_places=4,
        default=0,
        help_text="Quantidade já devolvida (parcial acumulada).",
    )

    class Meta:
        verbose_name = "Item de venda Agro"
        verbose_name_plural = "Itens de venda Agro"

    def __str__(self):
        return f"{self.descricao[:30]} x {self.quantidade}"

    @property
    def quantidade_restante(self):
        from decimal import Decimal

        q = self.quantidade or Decimal("0")
        d = self.quantidade_devolvida or Decimal("0")
        r = q - d
        return r if r > 0 else Decimal("0")


class DevolucaoVendaAgro(models.Model):
    """Evento de devolução (parcial ou total) de uma VendaAgro."""

    venda = models.ForeignKey(
        VendaAgro,
        on_delete=models.CASCADE,
        related_name="devolucoes",
    )
    criado_em = models.DateTimeField(auto_now_add=True, db_index=True)
    usuario = models.CharField(max_length=150, blank=True, default="")
    motivo = models.TextField(blank=True, default="")
    total = models.DecimalField(max_digits=12, decimal_places=2, default=0)
    pagamentos_json = models.JSONField(
        null=True,
        blank=True,
        help_text="Formas e valores deste evento [{forma, valor}].",
    )
    movimento_caixa_ids = models.JSONField(
        null=True,
        blank=True,
        help_text="IDs de MovimentoCaixa (retirada) deste evento.",
    )
    incluiu_frete = models.BooleanField(default=False)
    frete_valor = models.DecimalField(max_digits=12, decimal_places=2, default=0)
    totalizou_venda = models.BooleanField(
        default=False,
        help_text="True se este evento zerou o restante da venda.",
    )

    class Meta:
        ordering = ["-criado_em"]
        verbose_name = "Devolução de venda Agro"
        verbose_name_plural = "Devoluções de venda Agro"

    def __str__(self):
        return f"Dev #{self.pk} venda #{self.venda_id} R$ {self.total}"


class DevolucaoItemVendaAgro(models.Model):
    devolucao = models.ForeignKey(
        DevolucaoVendaAgro,
        on_delete=models.CASCADE,
        related_name="itens",
    )
    item = models.ForeignKey(
        ItemVendaAgro,
        on_delete=models.CASCADE,
        related_name="devolucoes_item",
    )
    quantidade = models.DecimalField(max_digits=14, decimal_places=4, default=0)
    valor_total = models.DecimalField(max_digits=12, decimal_places=2, default=0)

    class Meta:
        verbose_name = "Item devolvido (venda Agro)"
        verbose_name_plural = "Itens devolvidos (venda Agro)"

    def __str__(self):
        return f"Item {self.item_id} x {self.quantidade}"


class PedidoEntrega(models.Model):
    """Entrega vinculada ao PDV (orçamento com entrega); painel de gestão e rotas."""

    class Status(models.TextChoices):
        PENDENTE = "pendente", "Pendente"
        SEPARANDO = "separando", "Separando"
        PRONTO_ROTA = "pronto_rota", "Pronto p/ rota"
        EM_ROTA = "em_rota", "Em rota"
        ENTREGUE = "entregue", "Entregue"
        CANCELADO = "cancelado", "Cancelado"

    status = models.CharField(
        max_length=20,
        choices=Status.choices,
        default=Status.PENDENTE,
        db_index=True,
    )
    cliente_agro = models.ForeignKey(
        "ClienteAgro",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="pedidos_entrega",
        verbose_name="Cliente (cadastro PDV)",
    )
    cliente_nome = models.CharField(max_length=300)
    telefone = models.CharField(max_length=40, blank=True, default="")
    origem = models.CharField(
        max_length=24,
        blank=True,
        default="",
        db_index=True,
        help_text="Ex.: pdv, catalogo — vazio = legado PDV.",
    )
    loja_entrega = models.CharField(
        max_length=16,
        blank=True,
        default="",
        db_index=True,
        help_text="Dono da entrega: centro | vila. Vazio = ainda sem loja (as duas veem).",
    )
    loja_assumida_em = models.DateTimeField(null=True, blank=True)
    loja_assumida_por = models.CharField(max_length=120, blank=True, default="")
    endereco_linha = models.CharField(max_length=500, blank=True, default="")
    plus_code = models.CharField(max_length=120, blank=True, default="")
    referencia_rural = models.CharField(
        max_length=300,
        blank=True,
        default="",
        help_text="Ex.: porteira azul, 2 km após o trevo.",
    )
    maps_url_manual = models.URLField(
        max_length=600,
        blank=True,
        default="",
        help_text="Link colado do Google Maps (casa no satélite).",
    )
    itens_json = models.JSONField(default=list)
    total_texto = models.CharField(max_length=48, blank=True, default="")
    orc_local_id = models.BigIntegerField(null=True, blank=True, db_index=True)
    retomar_codigo = models.CharField(max_length=40, blank=True, default="")
    operador = models.CharField(max_length=120, blank=True, default="")
    hora_prevista = models.TimeField(null=True, blank=True)
    hora_saida = models.DateTimeField(null=True, blank=True)
    hora_entrega = models.DateTimeField(null=True, blank=True)
    observacoes = models.TextField(blank=True, default="")
    forma_pagamento = models.CharField(
        max_length=40,
        blank=True,
        default="",
        verbose_name="Forma de pagamento",
    )
    troco_precisa = models.BooleanField(
        null=True,
        blank=True,
        verbose_name="Precisa de troco",
        help_text="Somente para Dinheiro: True = levar troco, False = sem troco.",
    )
    aguarda_pagamento_pdv = models.BooleanField(
        default=False,
        db_index=True,
        verbose_name="Aguarda pagamento no PDV",
        help_text="Venda do PDV pendente até fechar pagamento após a entrega.",
    )
    pdv_wizard_state = models.JSONField(
        default=dict,
        blank=True,
        help_text="Snapshot do wizard PDV para retomar no pagamento.",
    )
    sessao_caixa = models.ForeignKey(
        "SessaoCaixa",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="entregas_pdv_pendentes",
    )
    venda_agro = models.ForeignKey(
        "VendaAgro",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="pedido_entrega_origem",
    )
    criado_em = models.DateTimeField(auto_now_add=True)
    atualizado_em = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-criado_em"]
        verbose_name = "Pedido de entrega"
        verbose_name_plural = "Pedidos de entrega"

    def __str__(self):
        return f"Entrega #{self.pk} — {self.cliente_nome[:40]}"


class OpcaoBaixaFinanceiroExtra(models.Model):
    """
    Forma de pagamento ou conta/banco adicionada pelo usuário às listas da baixa no Agro.
    Complementa as opções vindas do Mongo (modo ERP ou histórico).
    """

    class Tipo(models.TextChoices):
        FORMA = "forma", "Forma de pagamento"
        BANCO = "banco", "Banco / conta"

    usuario = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name="opcoes_baixa_financeiro_extra",
    )
    tipo = models.CharField(max_length=16, choices=Tipo.choices, db_index=True)
    id_erp = models.CharField(
        "ID no ERP / Mongo",
        max_length=80,
        blank=True,
        default="",
        help_text="Recomendado: copie o ID do cadastro no ERP para manter a baixa alinhada.",
    )
    nome = models.CharField("Nome exibido", max_length=300)
    criado_em = models.DateTimeField(auto_now_add=True)

    class Meta:
        verbose_name = "Opção extra (baixa financeira)"
        verbose_name_plural = "Opções extras (baixa financeira)"
        ordering = ["tipo", "nome"]
        constraints = [
            models.UniqueConstraint(
                fields=["usuario", "tipo", "id_erp"],
                condition=models.Q(id_erp__gt=""),
                name="uniq_opcao_baixa_extra_com_id_erp",
            ),
            models.UniqueConstraint(
                fields=["usuario", "tipo", "nome"],
                condition=models.Q(id_erp=""),
                name="uniq_opcao_baixa_extra_sem_id_erp",
            ),
        ]

    def save(self, *args, **kwargs):
        self.nome = (self.nome or "").strip()[:300]
        self.id_erp = (self.id_erp or "").strip()[:80]
        super().save(*args, **kwargs)

    def __str__(self):
        suf = f" ({self.id_erp})" if self.id_erp else ""
        return f"{self.get_tipo_display()}: {self.nome}{suf}"


class LancamentoAtalhoFiltro(models.Model):
    """
    Atalhos de filtro da lista de lançamentos (2 por usuário).
    ``payload`` espelha o objeto usado nos favoritos locais (tipo, status, datas, busca, planos excl.).
    """

    usuario = models.ForeignKey(
        User,
        on_delete=models.CASCADE,
        related_name="lancamento_atalhos_filtro",
    )
    slot = models.PositiveSmallIntegerField(
        db_index=True,
        help_text="1 ou 2 — identifica o botão na barra.",
    )
    nome = models.CharField(max_length=80)
    payload = models.JSONField(default=dict, blank=True)
    atualizado_em = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Atalho de filtro (lançamentos)"
        verbose_name_plural = "Atalhos de filtro (lançamentos)"
        constraints = [
            models.UniqueConstraint(
                fields=["usuario", "slot"],
                name="uniq_lancamento_atalho_filtro_usuario_slot",
            ),
        ]
        ordering = ["usuario_id", "slot"]

    def __str__(self):
        return f"{self.usuario_id} · {self.slot} · {self.nome[:40]}"


class ComprasFolhaSaldoFiltroPreset(models.Model):
    """
    Filtros salvos da Folha de saldo (Compras) — compartilhados por toda a loja (Postgres).
    No máximo um registro com ``is_padrao=True`` (aberto automaticamente).
    """

    nome = models.CharField(max_length=80)
    payload = models.JSONField(default=dict, blank=True)
    is_padrao = models.BooleanField(default=False, db_index=True)
    criado_por = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="compras_folha_saldo_presets",
    )
    criado_em = models.DateTimeField(auto_now_add=True)
    atualizado_em = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Filtro Folha de saldo (Compras)"
        verbose_name_plural = "Filtros Folha de saldo (Compras)"
        ordering = ["-is_padrao", "nome", "pk"]

    def __str__(self):
        marca = " ★" if self.is_padrao else ""
        return f"{self.nome[:40]}{marca}"


class ProdutoGrupoAgro(models.Model):
    """
    Agrupamento lógico no Agro: um nome comercial e um preço de venda únicos,
    com variantes por marca + código de barras (cada variante pode apontar para um Id do ERP/Mongo).
    """

    nome = models.CharField("Nome do produto", max_length=300)
    preco_venda = models.DecimalField("Preço de venda", max_digits=12, decimal_places=2)
    ativo = models.BooleanField(default=True)
    usuario = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="produto_grupos_agro",
    )
    criado_em = models.DateTimeField(auto_now_add=True)
    atualizado_em = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["nome"]
        verbose_name = "Grupo de produto (Agro)"
        verbose_name_plural = "Grupos de produto (Agro)"

    def __str__(self):
        return self.nome


class ProdutoGrupoVarianteAgro(models.Model):
    """Marca + EAN dentro de um grupo; opcional vínculo com cadastro ERP (Mongo)."""

    grupo = models.ForeignKey(
        ProdutoGrupoAgro,
        on_delete=models.CASCADE,
        related_name="variantes",
    )
    marca = models.CharField(max_length=120)
    codigo_barras = models.CharField(max_length=80)
    produto_erp_id = models.CharField(
        "ID produto ERP/Mongo",
        max_length=64,
        blank=True,
        default="",
        db_index=True,
    )

    class Meta:
        ordering = ["id"]
        verbose_name = "Variante (marca / código de barras)"
        verbose_name_plural = "Variantes (marca / código de barras)"
        constraints = [
            models.UniqueConstraint(
                fields=["grupo", "marca"],
                name="uniq_prod_grupo_variante_marca_por_grupo",
            ),
            models.UniqueConstraint(
                fields=["codigo_barras"],
                condition=~models.Q(codigo_barras=""),
                name="uniq_prod_grupo_variante_codigo_barras",
            ),
        ]

    def __str__(self):
        return f"{self.grupo_id} · {self.marca} · {self.codigo_barras}"


class ProdutoGestaoOverlayAgro(models.Model):
    """
    Sobrescritas locais no Agro (PDV, gestão, cadastro ERP) sobre o espelho do ERP.
    Texto vazio ou preço nulo = voltar a usar o valor do Mongo para aquele campo. Não grava no ERP.
    """

    produto_externo_id = models.CharField(
        max_length=64,
        unique=True,
        db_index=True,
        verbose_name="ID produto (Mongo/ERP)",
    )
    nome = models.CharField(max_length=300, blank=True, default="")
    marca = models.CharField(max_length=120, blank=True, default="")
    categoria = models.CharField(max_length=200, blank=True, default="")
    fornecedor_texto = models.CharField(max_length=300, blank=True, default="")
    unidade = models.CharField(max_length=20, blank=True, default="")
    codigo_barras = models.CharField(
        max_length=80,
        blank=True,
        default="",
        db_index=True,
        verbose_name="Código de barras (override)",
    )
    codigo_nfe = models.CharField(
        max_length=64,
        blank=True,
        default="",
        db_index=True,
        verbose_name="Código / NFe GM (override)",
    )
    peso_etiqueta = models.CharField(
        max_length=40,
        blank=True,
        default="",
        verbose_name="Peso (etiqueta)",
        help_text="Texto livre para etiqueta gôndola (ex.: 5 KG, 500 g).",
    )
    subcategoria = models.CharField(max_length=200, blank=True, default="")
    subcategoria_2 = models.CharField(
        max_length=200,
        blank=True,
        default="",
        verbose_name="Subcategoria 2",
    )
    subcategoria_3 = models.CharField(
        max_length=200,
        blank=True,
        default="",
        verbose_name="Subcategoria 3",
    )
    subcategoria_4 = models.CharField(
        max_length=200,
        blank=True,
        default="",
        verbose_name="Subcategoria 4",
    )
    descricao = models.TextField(blank=True, default="", verbose_name="Descrição (override)")
    preco_venda = models.DecimalField(
        max_digits=12,
        decimal_places=2,
        null=True,
        blank=True,
        verbose_name="Preço de venda (override)",
    )
    cashback_percentual = models.DecimalField(
        "Cashback (%)",
        max_digits=5,
        decimal_places=2,
        null=True,
        blank=True,
        validators=[MinValueValidator(0), MaxValueValidator(100)],
        help_text="Vazio = usar padrão do sistema (ex.: 1%). Zero desliga cashback na venda.",
    )
    ativo_exibicao = models.BooleanField(
        null=True,
        blank=True,
        verbose_name="Ativo na listagem",
        help_text="None = seguir ERP; True/False forçar exibição de status.",
    )
    estoque_min_centro = models.DecimalField(
        max_digits=12, decimal_places=3, null=True, blank=True
    )
    estoque_max_centro = models.DecimalField(
        max_digits=12, decimal_places=3, null=True, blank=True
    )
    estoque_min_vila = models.DecimalField(
        max_digits=12, decimal_places=3, null=True, blank=True
    )
    estoque_max_vila = models.DecimalField(
        max_digits=12, decimal_places=3, null=True, blank=True
    )
    cadastro_extras = models.JSONField(
        default=dict,
        blank=True,
        verbose_name="Extras cadastro (fiscal local, kit PDV, etc.)",
        help_text="JSON livre: fiscal (NCM, CFOP…), kit (baixa_componentes, deposito), etc.",
    )
    usuario = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="produto_overlays_gestao",
    )
    atualizado_em = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Overlay gestão de produto"
        verbose_name_plural = "Overlays gestão de produtos"

    def __str__(self):
        return f"{self.produto_externo_id} · overlay"


class ProdutoCadastroAlteracaoAgro(models.Model):
    """
    Histórico de alteração do cadastro (nome, preço, códigos…).
    Não registra movimentação de estoque/saldo — só campos do cadastro SisVale.
    """

    class Origem(models.TextChoices):
        MODAL = "modal", "Modal cadastro"
        PDV = "pdv", "PDV edição rápida"
        GESTAO = "gestao", "Gestão"
        PLANILHA = "planilha", "Excel"
        NF = "nf", "Entrada NF"
        OUTRO = "outro", "Outro"

    produto_externo_id = models.CharField(max_length=64, db_index=True)
    campo = models.CharField(max_length=64, db_index=True)
    campo_label = models.CharField(max_length=80, blank=True, default="")
    valor_antes = models.CharField(max_length=500, blank=True, default="")
    valor_depois = models.CharField(max_length=500, blank=True, default="")
    usuario = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="produto_cadastro_alteracoes",
    )
    origem = models.CharField(
        max_length=16,
        choices=Origem.choices,
        default=Origem.OUTRO,
        db_index=True,
    )
    criado_em = models.DateTimeField(auto_now_add=True, db_index=True)

    class Meta:
        verbose_name = "Alteração cadastro produto"
        verbose_name_plural = "Alterações cadastro produto"
        ordering = ["-criado_em", "-id"]
        indexes = [
            models.Index(
                fields=["produto_externo_id", "-criado_em"],
                name="prod_cad_alt_pid_criado_idx",
            ),
        ]

    def __str__(self):
        return f"{self.produto_externo_id} · {self.campo} · {self.criado_em}"


class EstoqueLote(models.Model):
    """Lote / validade com saldo local (Agro) associado a um overlay de produto."""

    overlay = models.ForeignKey(
        ProdutoGestaoOverlayAgro,
        on_delete=models.CASCADE,
        related_name="lotes",
    )
    lote_codigo = models.CharField("Código do lote", max_length=100)
    data_validade = models.DateField("Data de validade")
    quantidade_atual = models.DecimalField(
        "Quantidade atual",
        max_digits=10,
        decimal_places=2,
        default=0,
    )
    deposito = models.CharField(
        "Depósito / loja",
        max_length=16,
        blank=True,
        default="",
        db_index=True,
        help_text="centro | vila — loja onde a entrada NF lançou o estoque; vazio = não definido.",
    )
    baixado_centro_em = models.DateTimeField(
        "Baixa validade Centro",
        null=True,
        blank=True,
        help_text="Quando o Centro conferiu/baixou este lote. A Vila continua vendo até baixar.",
    )
    baixado_vila_em = models.DateTimeField(
        "Baixa validade Vila",
        null=True,
        blank=True,
        help_text="Quando a Vila conferiu/baixou este lote. O Centro continua vendo até baixar.",
    )
    data_entrada = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["data_validade", "id"]
        constraints = [
            models.UniqueConstraint(
                fields=["overlay", "lote_codigo"],
                name="uniq_estoque_lote_overlay_lote",
            ),
        ]
        verbose_name = "Estoque por lote (Agro)"
        verbose_name_plural = "Estoque por lote (Agro)"

    def __str__(self) -> str:
        return f"{self.lote_codigo} — {self.data_validade}"

    def save(self, *args, **kwargs):
        super().save(*args, **kwargs)
        sync_overlay_validade_resumo_de_lotes(self.overlay)


def sync_overlay_extra_validade_para_lote(
    overlay: ProdutoGestaoOverlayAgro,
    *,
    lote_codigo: str | None = None,
    data_validade=None,
    quantidade_atual=None,
) -> EstoqueLote | None:
    """
    Espelha validade/lote do cadastro_extras em ``EstoqueLote`` (contagem do BI).
    Mantém quantidade existente; só preenche saldo operacional se ainda zero.
    """
    from decimal import Decimal
    from datetime import datetime as _dt

    ex = (
        dict(overlay.cadastro_extras) if isinstance(overlay.cadastro_extras, dict) else {}
    )
    if data_validade is None:
        raw_v = ex.get("validade")
        if not raw_v:
            return None
        try:
            data_validade = _dt.strptime(str(raw_v)[:10], "%Y-%m-%d").date()
        except (ValueError, TypeError):
            return None
    if lote_codigo is None:
        lote_codigo = str(ex.get("lote") or "—").strip()[:100] or "—"
    code = str(lote_codigo)[:100]
    el = EstoqueLote.objects.filter(overlay=overlay, lote_codigo=code).first()
    if el is None:
        el = (
            EstoqueLote.objects.filter(overlay=overlay, quantidade_atual__gt=0)
            .order_by("data_validade", "id")
            .first()
        )
        if el is not None:
            code = el.lote_codigo
    qtd = quantidade_atual
    if qtd is None:
        qtd = el.quantidade_atual if el is not None else Decimal("0")
    el, _ = EstoqueLote.objects.update_or_create(
        overlay=overlay,
        lote_codigo=code,
        defaults={
            "data_validade": data_validade,
            "quantidade_atual": qtd,
        },
    )
    return el


def garantir_estoque_lote_desde_extras(
    overlay: ProdutoGestaoOverlayAgro,
    *,
    quantidade_atual=None,
) -> list:
    """
    Se o produto tem validade no resumo (tela Validade / extras) e ainda não tem
    linha em ``EstoqueLote``, cria o lote. Assim a aba «Validade e lote» do
    cadastro mostra o mesmo que o relatório.
    """
    existentes = list(
        EstoqueLote.objects.filter(overlay=overlay).order_by("data_validade", "id")
    )
    if existentes:
        return existentes
    ex = (
        dict(overlay.cadastro_extras) if isinstance(overlay.cadastro_extras, dict) else {}
    )
    if not ex.get("validade"):
        return []
    el = sync_overlay_extra_validade_para_lote(
        overlay,
        lote_codigo=ex.get("lote"),
        quantidade_atual=quantidade_atual,
    )
    if el is None:
        return []
    return [el]


def sync_overlay_validade_resumo_de_lotes(overlay: ProdutoGestaoOverlayAgro) -> None:
    """
    Atualiza cadastro_extras.validade e .lote com o lote mais crítico
    (primeira data, preferindo quantidade > 0), para compatibilidade com o relatório.
    """
    lotes = list(
        EstoqueLote.objects.filter(overlay=overlay).order_by("data_validade", "id")
    )
    ex = (
        dict(overlay.cadastro_extras) if isinstance(overlay.cadastro_extras, dict) else {}
    )
    if not lotes:
        ex.pop("validade", None)
        ex.pop("lote", None)
    else:
        pick = next(
            (L for L in lotes if L.quantidade_atual and L.quantidade_atual > 0),
            None,
        )
        if pick is None:
            ex.pop("validade", None)
            ex.pop("lote", None)
        else:
            ex["validade"] = pick.data_validade.isoformat()[:10]
            ex["lote"] = str(pick.lote_codigo)[:80]
            ex["validade_alerta"] = False
            ex.pop("validade_msg", None)
    overlay.cadastro_extras = ex
    overlay.save(update_fields=["cadastro_extras", "atualizado_em"])


def parse_data_validade_entrada_nf(raw) -> "date | None":
    """Aceita AAAA-MM-DD (XML/input) ou DD/MM/AAAA."""
    from datetime import date as _date
    from datetime import datetime as _dt
    import re as _re

    s = str(raw or "").strip()
    if not s:
        return None
    if _re.match(r"^\d{4}-\d{2}-\d{2}", s):
        try:
            return _dt.strptime(s[:10], "%Y-%m-%d").date()
        except (ValueError, TypeError):
            return None
    m = _re.match(r"^(\d{2})/(\d{2})/(\d{4})$", s[:10] if len(s) >= 10 else s)
    if m:
        try:
            return _date(int(m.group(3)), int(m.group(2)), int(m.group(1)))
        except (ValueError, TypeError):
            return None
    return None


def registrar_lote_validade_apos_entrada_nf(
    pid: str,
    ln: dict,
    qtd,
    *,
    nome_produto: str = "",
    deposito: str = "",
) -> dict | None:
    """
    Espelha lote/validade da etapa 4 da Entrada NF em ``EstoqueLote`` (tela Validade + BI).
    Sem data → não cria. Soma quantidade se o mesmo código de lote já existir.
    ``deposito`` = centro|vila (loja do lançamento de estoque).
    """
    from decimal import Decimal

    dv = parse_data_validade_entrada_nf(ln.get("lote_validade") if isinstance(ln, dict) else None)
    if dv is None:
        return None
    lote_cod = str((ln or {}).get("lote_numero") or "").strip()[:100] or "—"
    try:
        q_add = Decimal(str(qtd)).quantize(Decimal("0.01"))
    except Exception:
        return None
    if q_add <= 0:
        return None
    dep = str(deposito or "").strip().lower()
    if dep not in ("centro", "vila"):
        dep = ""
    ov, _ = ProdutoGestaoOverlayAgro.objects.get_or_create(
        produto_externo_id=str(pid)[:64],
    )
    el = EstoqueLote.objects.filter(overlay=ov, lote_codigo=lote_cod).first()
    if el is not None:
        nova = (Decimal(el.quantidade_atual or 0) + q_add).quantize(Decimal("0.01"))
        el.data_validade = dv
        el.quantidade_atual = nova
        if dep and not (el.deposito or "").strip():
            el.deposito = dep
        elif dep:
            el.deposito = dep
        el.save()
    else:
        el = EstoqueLote.objects.create(
            overlay=ov,
            lote_codigo=lote_cod,
            data_validade=dv,
            quantidade_atual=q_add,
            deposito=dep,
        )
    try:
        sync_overlay_validade_resumo_de_lotes(ov)
    except Exception:
        pass
    return {
        "lote_id": el.pk,
        "lote_codigo": el.lote_codigo,
        "data_validade": el.data_validade.isoformat()[:10],
        "quantidade_atual": float(el.quantidade_atual),
        "deposito": el.deposito or "",
    }


def reduzir_lote_validade_estorno_entrada_nf(
    pid: str,
    *,
    lote_codigo: str = "",
    data_validade=None,
    qtd=None,
) -> None:
    """Ao reabrir NF: reduz o saldo do lote que a entrada tinha alimentado."""
    from decimal import Decimal

    if not pid:
        return
    try:
        q_sub = Decimal(str(qtd or 0)).quantize(Decimal("0.01"))
    except Exception:
        return
    if q_sub <= 0:
        return
    code = str(lote_codigo or "").strip()[:100] or "—"
    ov = ProdutoGestaoOverlayAgro.objects.filter(produto_externo_id=str(pid)[:64]).first()
    if ov is None:
        return
    el = EstoqueLote.objects.filter(overlay=ov, lote_codigo=code).first()
    if el is None and data_validade is not None:
        el = (
            EstoqueLote.objects.filter(overlay=ov, data_validade=data_validade)
            .order_by("-quantidade_atual", "id")
            .first()
        )
    if el is None:
        return
    nova = (Decimal(el.quantidade_atual or 0) - q_sub).quantize(Decimal("0.01"))
    if nova <= 0:
        ov_ref = el.overlay
        el.delete()
        if EstoqueLote.objects.filter(overlay=ov_ref).exists():
            sync_overlay_validade_resumo_de_lotes(ov_ref)
        else:
            ex = dict(ov_ref.cadastro_extras) if isinstance(ov_ref.cadastro_extras, dict) else {}
            ex.pop("validade", None)
            ex.pop("lote", None)
            ov_ref.cadastro_extras = ex
            ov_ref.save(update_fields=["cadastro_extras", "atualizado_em"])
    else:
        el.quantidade_atual = nova
        el.save()


class ProdutoMarcaVariacaoAgro(models.Model):
    """
    Variações de marca/código do produto mestre (espelho ERP) no Agro.
    Estoque e custo por linha; o custo exibido no mestre pode ser média ponderada.
    """

    produto_externo_id = models.CharField(
        max_length=64,
        db_index=True,
        verbose_name="ID produto (Mongo/ERP)",
    )
    marca = models.CharField(max_length=120)
    codigo_barras = models.CharField(max_length=80, blank=True, default="")
    codigo_fornecedor = models.CharField(max_length=80, blank=True, default="")
    codigo_interno = models.CharField(
        max_length=80,
        blank=True,
        default="",
        db_index=True,
        verbose_name="Código interno (variação)",
    )
    estoque = models.DecimalField(max_digits=14, decimal_places=4, default=0)
    custo_unitario = models.DecimalField(max_digits=14, decimal_places=4, default=0)
    ordem = models.PositiveSmallIntegerField(default=0)
    atualizado_em = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["produto_externo_id", "ordem", "id"]
        verbose_name = "Variação de marca (cadastro mestre)"
        verbose_name_plural = "Variações de marca (cadastro mestre)"

    def __str__(self):
        return f"{self.produto_externo_id} · {self.marca}"


class PromocaoAgro(models.Model):
    """Promoção configurada no Agro (PDV, venda direta, catálogo)."""

    class Tipo(models.TextChoices):
        LEVE_PAGUE = "leve_pague", "Leve X, pague Y"
        ACIMA_UNIDADES = "acima_unidades", "Acima de X unidades, pague Y"
        VALOR_DIRETO = "valor_direto", "Valor direto"

    nome = models.CharField("Nome", max_length=200)
    tipo = models.CharField("Tipo", max_length=20, choices=Tipo.choices, db_index=True)
    qtd_x = models.DecimalField(
        "Quantidade X",
        max_digits=12,
        decimal_places=3,
        null=True,
        blank=True,
        help_text="Unidades para Leve X ou limiar Acima de X.",
    )
    preco_y = models.DecimalField(
        "Preço Y (por unidade)",
        max_digits=12,
        decimal_places=4,
        null=True,
        blank=True,
        help_text="Preço promocional por unidade quando o critério for atendido.",
    )
    preco_y_t1 = models.DecimalField(
        "Preço Y · Tabela 1",
        max_digits=12,
        decimal_places=4,
        null=True,
        blank=True,
        help_text="Preço Y quando a forma cair na Tabela 1 global; vazio = usa preco_y.",
    )
    preco_y_t2 = models.DecimalField(
        "Preço Y · Tabela 2",
        max_digits=12,
        decimal_places=4,
        null=True,
        blank=True,
        help_text="Preço Y quando a forma cair na Tabela 2 global; vazio = usa preco_y.",
    )
    regra_vs_tabela = models.CharField(
        "Regra promo × tabela %",
        max_length=16,
        default="maior",
        blank=True,
        help_text="maior | promo | tabela — default da promo; item pode sobrescrever.",
    )
    resolucoes_vs_tabela = models.JSONField(
        "Resoluções promo × tabela por produto",
        default=dict,
        blank=True,
        help_text='{"produto_id": "maior"|"promo"|"tabela"}',
    )
    data_inicio = models.DateField("Início")
    data_fim = models.DateField("Fim", null=True, blank=True)
    permanente = models.BooleanField(
        "Válida permanentemente",
        default=False,
        help_text="Sem data de encerramento; vale a partir do início enquanto estiver ativa.",
    )
    telas = models.JSONField(
        "Telas",
        default=list,
        blank=True,
        help_text='Ex.: ["pdv", "venda_direta", "catalogo"]',
    )
    empresas = models.JSONField(
        "Empresas",
        default=list,
        blank=True,
        help_text='Ex.: ["centro", "vila"]',
    )
    ativo = models.BooleanField("Ativa", default=True, db_index=True)
    criado_em = models.DateTimeField(auto_now_add=True)
    atualizado_em = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Promoção"
        verbose_name_plural = "Promoções"
        ordering = ["-data_inicio", "-pk"]

    def __str__(self):
        return self.nome[:80]


class PromocaoProdutoAgro(models.Model):
    """Produto vinculado a uma promoção."""

    promocao = models.ForeignKey(
        PromocaoAgro,
        on_delete=models.CASCADE,
        related_name="produtos",
    )
    produto_externo_id = models.CharField(max_length=64, db_index=True)
    codigo = models.CharField(max_length=80, blank=True, default="")
    nome_produto = models.CharField(max_length=300, blank=True, default="")
    preco_padrao = models.DecimalField(
        max_digits=12, decimal_places=2, null=True, blank=True
    )
    preco_promocional = models.DecimalField(
        "Preço promocional (valor direto)",
        max_digits=12,
        decimal_places=4,
        null=True,
        blank=True,
    )
    preco_promocional_t1 = models.DecimalField(
        "Preço promo · Tabela 1",
        max_digits=12,
        decimal_places=4,
        null=True,
        blank=True,
    )
    preco_promocional_t2 = models.DecimalField(
        "Preço promo · Tabela 2",
        max_digits=12,
        decimal_places=4,
        null=True,
        blank=True,
    )

    class Meta:
        verbose_name = "Produto da promoção"
        verbose_name_plural = "Produtos da promoção"
        ordering = ["codigo", "nome_produto"]
        constraints = [
            models.UniqueConstraint(
                fields=["promocao", "produto_externo_id"],
                name="uniq_promocao_produto_externo",
            ),
        ]

    def __str__(self):
        return f"{self.codigo or self.produto_externo_id} · {self.nome_produto[:40]}"


class EtiquetaImpressaoHistoricoAgro(models.Model):
    """Registro de jobs de impressão de etiquetas de preço (reimpressão e auditoria)."""

    criado_em = models.DateTimeField(auto_now_add=True, db_index=True)
    usuario = models.CharField(max_length=150, blank=True, default="")
    origem = models.CharField(max_length=32, blank=True, default="fila")
    preset_id = models.CharField(max_length=64, blank=True, default="")
    preset_nome = models.CharField(max_length=120, blank=True, default="")
    texto_rodape = models.CharField(max_length=120, blank=True, default="")
    total_etiquetas = models.PositiveIntegerField(default=0)
    qtd_linhas = models.PositiveSmallIntegerField(default=0)
    resumo_nomes = models.CharField(max_length=400, blank=True, default="")
    itens_json = models.JSONField(default=list)

    class Meta:
        verbose_name = "Histórico impressão etiqueta"
        verbose_name_plural = "Históricos impressão etiquetas"
        ordering = ["-criado_em"]
        indexes = [
            models.Index(fields=["-criado_em"], name="etq_hist_criado_idx"),
        ]

    def __str__(self):
        return f"{self.criado_em:%d/%m/%Y %H:%M} · {self.total_etiquetas} etq."


class EtiquetaPresetAgro(models.Model):
    """
    Presets de layout de etiquetas — compartilhados por toda a loja (Postgres / multi-PC).
    ``client_key`` = id estável no front (ex. preset-xxx); payload = JSON completo do preset.
    """

    client_key = models.CharField(max_length=64, unique=True, db_index=True)
    nome = models.CharField(max_length=120)
    payload = models.JSONField(default=dict, blank=True)
    criado_por = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="etiqueta_presets",
    )
    criado_em = models.DateTimeField(auto_now_add=True)
    atualizado_em = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Preset etiqueta"
        verbose_name_plural = "Presets etiquetas"
        ordering = ["nome", "pk"]

    def __str__(self):
        return self.nome[:60]


class EtiquetaLoteAgro(models.Model):
    """
    Lote provisório A4 gôndola (18/folha) — progresso multi-PC no Postgres.
    Usado p.ex. abertura da Vila: lista completa + cursor do próximo a imprimir.
    """

    class Status(models.TextChoices):
        ABERTO = "aberto", "Aberto"
        CONCLUIDO = "concluido", "Concluído"
        CANCELADO = "cancelado", "Cancelado"

    nome = models.CharField(max_length=160, blank=True, default="")
    loja = models.CharField(max_length=16, blank=True, default="vila")  # vila|centro|total
    filtros_json = models.JSONField(default=dict, blank=True)
    preset_id = models.CharField(max_length=64, blank=True, default="gondola")
    status = models.CharField(
        max_length=16,
        choices=Status.choices,
        default=Status.ABERTO,
        db_index=True,
    )
    itens_json = models.JSONField(default=list, blank=True)
    cursor = models.PositiveIntegerField(default=0)
    ultima_folha_qtd = models.PositiveSmallIntegerField(default=0)
    usuario = models.CharField(max_length=150, blank=True, default="")
    criado_em = models.DateTimeField(auto_now_add=True, db_index=True)
    atualizado_em = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Lote etiquetas A4"
        verbose_name_plural = "Lotes etiquetas A4"
        ordering = ["-criado_em"]
        indexes = [
            models.Index(fields=["status", "-criado_em"], name="etq_lote_status_criado_idx"),
        ]

    def __str__(self):
        return f"{self.nome or 'Lote'} · {self.status} · cursor {self.cursor}"

    @property
    def total_itens(self) -> int:
        itens = self.itens_json if isinstance(self.itens_json, list) else []
        return len(itens)


class CadastroPlanilhaImportHistoricoAgro(models.Model):
    """Backup e histórico de importações Excel do cadastro (permite desfazer)."""

    class Status(models.TextChoices):
        APLICADO = "aplicado", "Aplicado"
        REVERTIDO = "revertido", "Revertido"

    class Tipo(models.TextChoices):
        CADASTRO = "cadastro", "Cadastro (preços/dados)"
        ESTOQUE = "estoque", "Estoque (saldos)"

    criado_em = models.DateTimeField(auto_now_add=True)
    usuario = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="cadastro_planilha_imports",
    )
    nome_arquivo = models.CharField(max_length=255, blank=True, default="")
    n_produtos = models.PositiveIntegerField(default=0)
    n_campos = models.PositiveIntegerField(default=0)
    tipo = models.CharField(
        max_length=16,
        choices=Tipo.choices,
        default=Tipo.CADASTRO,
        db_index=True,
    )
    status = models.CharField(
        max_length=16,
        choices=Status.choices,
        default=Status.APLICADO,
        db_index=True,
    )
    backup = models.JSONField(default=dict, help_text="Snapshot antes de aplicar (por produto).")
    revertido_em = models.DateTimeField(null=True, blank=True)
    revertido_por = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="cadastro_planilha_reversoes",
    )

    class Meta:
        verbose_name = "Histórico importação planilha cadastro"
        verbose_name_plural = "Históricos importação planilha cadastro"
        ordering = ["-criado_em"]
        indexes = [
            models.Index(fields=["-criado_em"], name="cad_plan_imp_criado_idx"),
            models.Index(fields=["tipo", "-criado_em"], name="cad_plan_imp_tipo_idx"),
        ]

    def __str__(self):
        return f"{self.criado_em:%d/%m/%Y %H:%M} · {self.n_produtos} prod. · {self.status}"


class DashboardVendaDiaHistoricoAgro(models.Model):
    """Vendas diárias importadas (Excel Centro) — base da meta C quando o PDV ainda não tem o dia."""

    data = models.DateField("Data", unique=True, db_index=True)
    total = models.DecimalField("Total (R$)", max_digits=14, decimal_places=2)
    deposito = models.CharField(
        "Depósito",
        max_length=16,
        default="centro",
        blank=True,
        help_text="centro · vila — planilha Renan hoje é só Centro.",
    )
    fonte = models.CharField("Fonte", max_length=64, default="planilha", blank=True)
    atualizado_em = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Venda diária histórico BI"
        verbose_name_plural = "Vendas diárias histórico BI"
        ordering = ["-data"]

    def __str__(self):
        return f"{self.data:%d/%m/%Y} · R$ {self.total}"


class RelacionamentoHistoricoImportLoteAgro(models.Model):
    """Lote do import único de vendas ERP para o F8 (somente leitura)."""

    lote_id = models.CharField("ID do lote", max_length=64, unique=True, db_index=True)
    criado_em = models.DateTimeField(auto_now_add=True)
    erp_ate = models.DateField("ERP até (inclusivo)")
    pdv_desde = models.DateField("PDV SisVale desde")
    dry_run = models.BooleanField(default=False)
    stats_json = models.JSONField(default=dict, blank=True)
    observacao = models.CharField(max_length=300, blank=True, default="")

    class Meta:
        verbose_name = "Lote import histórico ERP (F8)"
        verbose_name_plural = "Lotes import histórico ERP (F8)"
        ordering = ["-criado_em"]

    def __str__(self):
        return f"{self.lote_id} · ERP ≤ {self.erp_ate:%d/%m/%Y}"


class RelacionamentoVendaHistoricoErpAgro(models.Model):
    """Cabeçalho de venda ERP importada — não entra em caixa, fiado nem estoque."""

    lote = models.ForeignKey(
        RelacionamentoHistoricoImportLoteAgro,
        on_delete=models.CASCADE,
        related_name="vendas",
    )
    cliente_agro = models.ForeignKey(
        ClienteAgro,
        on_delete=models.CASCADE,
        related_name="vendas_historico_erp",
        db_index=True,
    )
    venda_id_erp = models.CharField(max_length=64, db_index=True)
    cliente_id_erp = models.CharField(max_length=64, blank=True, default="")
    cliente_nome_snapshot = models.CharField(max_length=300, blank=True, default="")
    data_venda = models.DateTimeField(db_index=True)
    total = models.DecimalField(max_digits=12, decimal_places=2, default=0)
    forma_pagamento = models.CharField(max_length=120, blank=True, default="")

    class Meta:
        verbose_name = "Venda histórico ERP (F8)"
        verbose_name_plural = "Vendas histórico ERP (F8)"
        ordering = ["-data_venda"]
        constraints = [
            models.UniqueConstraint(
                fields=["lote", "venda_id_erp"],
                name="rel_hist_erp_venda_lote_uid",
            ),
        ]
        indexes = [
            models.Index(fields=["cliente_agro", "-data_venda"], name="rel_hist_erp_cli_dt_idx"),
        ]

    def __str__(self):
        return f"ERP {self.venda_id_erp} · {self.data_venda:%d/%m/%Y}"


class RelacionamentoItemHistoricoErpAgro(models.Model):
    """Item de venda ERP importada — snapshot do produto na época."""

    venda = models.ForeignKey(
        RelacionamentoVendaHistoricoErpAgro,
        on_delete=models.CASCADE,
        related_name="itens",
    )
    produto_id_erp = models.CharField(max_length=64, blank=True, default="", db_index=True)
    codigo_gm = models.CharField(max_length=64, blank=True, default="", db_index=True)
    descricao = models.CharField(max_length=300, blank=True, default="")
    quantidade = models.DecimalField(max_digits=12, decimal_places=3, default=0)
    valor_unitario = models.DecimalField(max_digits=12, decimal_places=2, default=0)
    valor_total = models.DecimalField(max_digits=12, decimal_places=2, default=0)

    class Meta:
        verbose_name = "Item histórico ERP (F8)"
        verbose_name_plural = "Itens histórico ERP (F8)"
        ordering = ["id"]

    def __str__(self):
        return f"{self.descricao[:40]} × {self.quantidade}"


class CaixaConferenciaRascunhoAgro(models.Model):
    """
    Contagem (valores + cédulas) do fechar caixa — Postgres multi-PC.
    Chave estável: ``YYYY-MM-DD::centro`` ou ``YYYY-MM-DD::vila`` (não depende da lista de PKs).
    """

    turno_key = models.CharField(max_length=64, unique=True, db_index=True)
    rascunho_json = models.JSONField(default=dict, blank=True)
    cedulas_json = models.JSONField(default=dict, blank=True)
    atualizado_por = models.CharField(max_length=120, blank=True, default="")
    atualizado_em = models.DateTimeField(auto_now=True)
    criado_em = models.DateTimeField(auto_now_add=True)

    class Meta:
        verbose_name = "Rascunho contagem fechar caixa"
        verbose_name_plural = "Rascunhos contagem fechar caixa"
        ordering = ["-atualizado_em"]

    def __str__(self):
        return f"Contagem {self.turno_key}"


class OrcamentoPdvAgro(models.Model):
    """Orçamento salvo no PDV — espelho do histórico local (GMORC…) por cliente."""

    orc_local_id = models.BigIntegerField(unique=True, db_index=True)
    cliente_agro = models.ForeignKey(
        ClienteAgro,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="orcamentos_pdv",
        verbose_name="Cliente (cadastro PDV)",
    )
    cliente_nome = models.CharField(max_length=300, blank=True, default="")
    cliente_key = models.CharField(max_length=120, db_index=True)
    cliente_mode = models.CharField(max_length=32, blank=True, default="cliente")
    payload_json = models.JSONField(default=dict)
    total_texto = models.CharField(max_length=48, blank=True, default="")
    entrega = models.BooleanField(default=False)
    forma_pagamento = models.CharField(max_length=40, blank=True, default="")
    usuario_registro = models.CharField(max_length=120, blank=True, default="")
    criado_em = models.DateTimeField(auto_now_add=True, db_index=True)
    atualizado_em = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Orçamento PDV"
        verbose_name_plural = "Orçamentos PDV"
        ordering = ["-criado_em"]
        indexes = [
            models.Index(fields=["cliente_key", "-criado_em"], name="orc_pdv_cli_key_dt_idx"),
        ]

    def __str__(self):
        return f"GMORC{self.orc_local_id} · {self.cliente_nome[:40]}"


class CatalogoDeliveryConfig(models.Model):
    """Identidade do catálogo delivery público (uma loja — GM Agro)."""

    nome_loja = models.CharField(max_length=100, default="GM Agro")
    whatsapp_contato = models.CharField(
        max_length=20,
        blank=True,
        default="",
        help_text="DDI+DDD+número, só dígitos",
    )
    mensagem_boas_vindas = models.TextField(blank=True, default="")
    area_entrega = models.CharField(max_length=300, blank=True, default="")
    endereco_loja = models.CharField(
        max_length=320,
        blank=True,
        default="",
        help_text="Legado — preferir endereço 1 / 2 abaixo.",
    )
    rotulo_loja_1 = models.CharField(max_length=80, blank=True, default="Centro")
    endereco_loja_1 = models.CharField(max_length=320, blank=True, default="")
    rotulo_loja_2 = models.CharField(max_length=80, blank=True, default="Vila Elias")
    endereco_loja_2 = models.CharField(max_length=320, blank=True, default="")
    cor_primaria = models.CharField(max_length=7, default="#059669")
    cor_secundaria = models.CharField(max_length=7, default="#fff7ed")
    logo_base64 = models.TextField(
        blank=True,
        default="",
        help_text="Logotipo da loja no topo do catálogo (antes do nome).",
    )
    logo_mime = models.CharField(max_length=40, blank=True, default="image/png")
    publicado = models.BooleanField(
        default=False,
        help_text="Se desligado, o link público mostra «em breve» (staff ainda vê).",
    )
    atualizado_em = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Configuração catálogo delivery"
        verbose_name_plural = "Configuração catálogo delivery"

    def __str__(self):
        return self.nome_loja

    def logo_url(self) -> str:
        b64 = (self.logo_base64 or "").strip()
        if not b64:
            return ""
        mime = (self.logo_mime or "image/png").strip() or "image/png"
        return f"data:{mime};base64,{b64}"

    def enderecos_exibir(self) -> list[dict]:
        """Lista de lojas com rótulo + endereço (até 2)."""
        out = []
        e1 = (self.endereco_loja_1 or self.endereco_loja or "").strip()
        if e1:
            out.append(
                {
                    "rotulo": (self.rotulo_loja_1 or "Loja 1").strip() or "Loja 1",
                    "endereco": e1,
                }
            )
        e2 = (self.endereco_loja_2 or "").strip()
        if e2:
            out.append(
                {
                    "rotulo": (self.rotulo_loja_2 or "Loja 2").strip() or "Loja 2",
                    "endereco": e2,
                }
            )
        return out


class CatalogoDeliveryCategoria(models.Model):
    """
    Categorias do catálogo delivery (estilo apps de ração: Cães → Adulto / Filhote…).
    Sem parent = categoria principal; com parent = subcategoria.
    """

    nome = models.CharField(max_length=80)
    slug = models.SlugField(max_length=90, unique=True)
    ordem = models.PositiveIntegerField(default=0)
    ativo = models.BooleanField(default=True)
    parent = models.ForeignKey(
        "self",
        null=True,
        blank=True,
        on_delete=models.CASCADE,
        related_name="filhos",
        verbose_name="Categoria pai (se for subcategoria)",
    )
    imagem_base64 = models.TextField(
        blank=True,
        default="",
        help_text="Foto de capa do card no catálogo (qualquer nível).",
    )
    imagem_mime = models.CharField(max_length=40, blank=True, default="image/jpeg")
    cor = models.CharField(
        max_length=7,
        blank=True,
        default="",
        help_text="Cor do card (#059669). Vazio = verde padrão. Vale em qualquer nível.",
    )

    class Meta:
        ordering = ["ordem", "nome"]
        verbose_name = "Categoria catálogo delivery"
        verbose_name_plural = "Categorias catálogo delivery"

    def __str__(self):
        if self.parent_id:
            return f"{self.parent.nome} › {self.nome}"
        return self.nome

class DispenserMidiaAgro(models.Model):
    """Biblioteca compartilhada do Dispenser A6 (logos, pets, ingredientes, ícones)."""

    TIPO_LOGO = "logo"
    TIPO_PET = "pet"
    TIPO_ING = "ing"
    TIPO_FLAVOR_ICO = "flavor_ico"
    TIPO_CHOICES = (
        (TIPO_LOGO, "Logo"),
        (TIPO_PET, "Pet"),
        (TIPO_ING, "Ingrediente"),
        (TIPO_FLAVOR_ICO, "Ícone de sabor"),
    )

    tipo = models.CharField(max_length=20, choices=TIPO_CHOICES, db_index=True)
    item_id = models.CharField(max_length=80, db_index=True)
    label = models.CharField(max_length=120, blank=True, default="")
    data_base64 = models.TextField(blank=True, default="")
    mime = models.CharField(max_length=40, blank=True, default="image/png")
    atualizado_em = models.DateTimeField(auto_now=True)
    criado_em = models.DateTimeField(auto_now_add=True)

    class Meta:
        verbose_name = "Dispenser A6 · mídia"
        verbose_name_plural = "Dispenser A6 · mídias"
        constraints = [
            models.UniqueConstraint(
                fields=["tipo", "item_id"],
                name="uniq_dispenser_midia_tipo_item",
            )
        ]
        indexes = [
            models.Index(fields=["tipo", "atualizado_em"], name="dsp_midia_tipo_upd_idx"),
        ]

    def __str__(self):
        return f"{self.tipo}:{self.item_id}"

    def data_url(self) -> str:
        b64 = (self.data_base64 or "").strip()
        if not b64:
            return ""
        mime = (self.mime or "image/png").strip() or "image/png"
        return f"data:{mime};base64,{b64}"


class DispenserDocumentoAgro(models.Model):
    """Folhas prontas e modelos de layout do Dispenser A6 (compartilhados)."""

    TIPO_FOLHA = "folha"
    TIPO_LAYOUT = "layout"
    TIPO_SABOR = "sabor"
    TIPO_CHOICES = (
        (TIPO_FOLHA, "Folha pronta"),
        (TIPO_LAYOUT, "Modelo de layout"),
        (TIPO_SABOR, "Sabor customizado"),
    )

    tipo = models.CharField(max_length=20, choices=TIPO_CHOICES, db_index=True)
    nome = models.CharField(max_length=80)
    payload = models.JSONField(default=dict, blank=True)
    thumb = models.TextField(blank=True, default="", help_text="Miniatura JPEG/PNG (data URL ou base64).")
    atualizado_em = models.DateTimeField(auto_now=True)
    criado_em = models.DateTimeField(auto_now_add=True)

    class Meta:
        verbose_name = "Dispenser A6 · documento"
        verbose_name_plural = "Dispenser A6 · documentos"
        constraints = [
            models.UniqueConstraint(
                fields=["tipo", "nome"],
                name="uniq_dispenser_doc_tipo_nome",
            )
        ]
        indexes = [
            models.Index(fields=["tipo", "atualizado_em"], name="dsp_doc_tipo_upd_idx"),
        ]

    def __str__(self):
        return f"{self.tipo}:{self.nome}"


class UsoLojaRetiradaAgro(models.Model):
    """Saída de produto para uso interno da loja (PDV · Postgres)."""

    class Motivo(models.TextChoices):
        LIMPEZA = "limpeza", "Limpeza"
        MANUTENCAO = "manutencao", "Manutenção"
        CONSUMO = "consumo", "Consumo interno"
        AMOSTRA = "amostra", "Amostra"
        BRINDE = "brinde", "Brinde cliente"
        USO_GERALDINHO = "uso_geraldinho", "Uso Geraldinho"
        USO_GERALDO = "uso_geraldo", "Uso Geraldo"
        OUTROS = "outros", "Outros"

    deposito = models.CharField(max_length=20, db_index=True)
    quem_levou = models.CharField(max_length=120)
    motivo = models.CharField(max_length=120, blank=True, default="")
    cliente_brinde = models.ForeignKey(
        "ClienteAgro",
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="uso_loja_brindes",
        verbose_name="Cliente do brinde",
    )
    operador_pin = models.CharField(max_length=120)
    usuario = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="uso_loja_retiradas",
    )
    sessao_caixa = models.ForeignKey(
        "SessaoCaixa",
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="uso_loja_retiradas",
    )
    observacao = models.TextField(blank=True, default="")
    criado_em = models.DateTimeField(auto_now_add=True, db_index=True)
    estornado = models.BooleanField(default=False, db_index=True)
    estornado_em = models.DateTimeField(null=True, blank=True)
    estornado_por = models.CharField(max_length=120, blank=True, default="")

    class Meta:
        verbose_name = "Uso loja · retirada"
        verbose_name_plural = "Uso loja · retiradas"
        ordering = ["-criado_em", "-pk"]

    def __str__(self):
        return f"Uso loja #{self.pk} · {self.deposito} · {self.quem_levou}"


class UsoLojaRetiradaItemAgro(models.Model):
    retirada = models.ForeignKey(
        UsoLojaRetiradaAgro,
        on_delete=models.CASCADE,
        related_name="itens",
    )
    produto_externo_id = models.CharField(max_length=100, db_index=True)
    codigo_interno = models.CharField(max_length=100, blank=True, default="")
    nome_produto = models.CharField(max_length=255, blank=True, default="")
    quantidade = models.DecimalField(max_digits=12, decimal_places=3)
    preco_custo = models.DecimalField(
        max_digits=12,
        decimal_places=2,
        null=True,
        blank=True,
        help_text="Custo unitário no momento da saída (snapshot).",
    )
    preco_venda = models.DecimalField(
        max_digits=12,
        decimal_places=2,
        null=True,
        blank=True,
        help_text="Preço de venda unitário no momento da saída (snapshot).",
    )
    ajuste = models.ForeignKey(
        "estoque.AjusteRapidoEstoque",
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="uso_loja_itens",
    )
    ajuste_estorno = models.ForeignKey(
        "estoque.AjusteRapidoEstoque",
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="uso_loja_itens_estorno",
    )

    class Meta:
        verbose_name = "Uso loja · item"
        verbose_name_plural = "Uso loja · itens"
        ordering = ["pk"]

    def __str__(self):
        return f"{self.nome_produto[:40]} × {self.quantidade}"


class DispositivoLojaAgro(models.Model):
    """PC/navegador da loja — UUID estável no Chrome + nome amigável."""

    device_id = models.CharField(max_length=64, unique=True, db_index=True)
    nome = models.CharField(max_length=80, blank=True, default="")
    ponto_caixa_ultimo = models.CharField(max_length=32, blank=True, default="")
    user_agent = models.CharField(max_length=400, blank=True, default="")
    tela = models.CharField(max_length=40, blank=True, default="")
    ultimo_visto_em = models.DateTimeField(auto_now=True)
    criado_em = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["-ultimo_visto_em"]
        verbose_name = "Dispositivo loja"
        verbose_name_plural = "Dispositivos loja"

    def __str__(self):
        rotulo = (self.nome or "").strip() or self.device_id[:8]
        return f"{rotulo} ({self.device_id[:8]})"


class BugReportAgro(models.Model):
    """Reporte de bug / feedback — online (Postgres)."""

    STATUS_NOVO = "novo"
    STATUS_VISTO = "visto"
    STATUS_FEITO = "feito"
    STATUS_CHOICES = (
        (STATUS_NOVO, "Novo"),
        (STATUS_VISTO, "Visto"),
        (STATUS_FEITO, "Feito"),
    )

    o_que_aconteceu = models.TextField()
    o_que_esperava = models.TextField(blank=True, default="")
    usuario_nome = models.CharField(max_length=120, blank=True, default="")
    usuario = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="bug_reports_agro",
    )
    device_id = models.CharField(max_length=64, blank=True, default="", db_index=True)
    dispositivo_nome = models.CharField(max_length=80, blank=True, default="")
    ponto_caixa = models.CharField(max_length=32, blank=True, default="")
    url_pagina = models.CharField(max_length=500, blank=True, default="")
    versao_app = models.CharField(max_length=32, blank=True, default="")
    user_agent = models.CharField(max_length=400, blank=True, default="")
    tela = models.CharField(max_length=40, blank=True, default="")
    print_base64 = models.TextField(blank=True, default="")
    print_mime = models.CharField(max_length=40, blank=True, default="image/jpeg")
    status = models.CharField(
        max_length=16,
        choices=STATUS_CHOICES,
        default=STATUS_NOVO,
        db_index=True,
    )
    notificado_whatsapp = models.BooleanField(default=False)
    notificado_email = models.BooleanField(default=False)
    criado_em = models.DateTimeField(auto_now_add=True, db_index=True)

    class Meta:
        ordering = ["-criado_em"]
        verbose_name = "Bug report"
        verbose_name_plural = "Bug reports"

    def __str__(self):
        return f"#{self.pk} {self.usuario_nome or '?'} — {(self.o_que_aconteceu or '')[:40]}"


class AjusteCodigoPendenteAgro(models.Model):
    """Código bipado sem cadastro — fila para conferir no cadastro (Postgres multi-PC)."""

    STATUS_PENDENTE = "pendente"
    STATUS_FEITO = "feito"
    STATUS_DESCARTADO = "descartado"
    STATUS_CHOICES = (
        (STATUS_PENDENTE, "Pendente"),
        (STATUS_FEITO, "Feito"),
        (STATUS_DESCARTADO, "Descartado"),
    )

    codigo_bipado = models.CharField(max_length=64, db_index=True)
    produto_externo_id = models.CharField(max_length=100, db_index=True)
    nome_produto = models.CharField(max_length=255, blank=True, default="")
    operador = models.CharField(max_length=120, blank=True, default="")
    usuario = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="ajuste_codigos_pendentes",
    )
    status = models.CharField(
        max_length=16,
        choices=STATUS_CHOICES,
        default=STATUS_PENDENTE,
        db_index=True,
    )
    criado_em = models.DateTimeField(auto_now_add=True, db_index=True)

    class Meta:
        ordering = ["-criado_em"]
        verbose_name = "Código pendente (ajuste)"
        verbose_name_plural = "Códigos pendentes (ajuste)"

    def __str__(self):
        return f"#{self.pk} {self.codigo_bipado} → {self.nome_produto or self.produto_externo_id}"


class RepasseVilaConfigAgro(models.Model):
    """Config única — % padrão do lucro bruto no repasse Vila → Centro."""

    percentual_lucro_padrao = models.DecimalField(
        max_digits=5,
        decimal_places=2,
        default=50,
        help_text="0 a 100. Padrão na tela/PDV ao montar o envio.",
    )
    planos_desconto_centro = models.JSONField(
        default=list,
        blank=True,
        help_text="Nomes de plano de conta que descontam do lucro enviado ao Centro. "
        "Os demais descontam do que ficou na Vila.",
    )
    reserva_vila = models.DecimalField(
        max_digits=12,
        decimal_places=2,
        default=0,
        help_text=(
            "Valor manual diário que fica na Vila: desconta do lucro bruto "
            "antes de aplicar o % enviado ao Centro."
        ),
    )
    reserva_vila_desde = models.DateField(
        null=True,
        blank=True,
        help_text="A partir desta data o valor manual entra todo dia (criação do campo: 18/08/2026).",
    )
    saldo_reserva_vila = models.DecimalField(
        max_digits=12,
        decimal_places=2,
        default=0,
        help_text="Saldo do Cofrinho Salário funcionário (reserva diária configurável).",
    )
    saldo_cofre_vila_elias = models.DecimalField(
        max_digits=12,
        decimal_places=2,
        default=0,
        help_text="Saldo do Cofre Vila Elias (fatia do lucro que não vai ao Centro).",
    )
    fundo_troco_vila = models.DecimalField(
        max_digits=12,
        decimal_places=2,
        default=500,
        help_text=(
            "Alvo de dinheiro que deve ficar na gaveta da Vila após o repasse (troco). "
            "Só aviso — não bloqueia. Prioridade: Salário → Vila Elias → Centro."
        ),
    )
    atualizado_em = models.DateTimeField(auto_now=True)
    atualizado_por = models.CharField(max_length=120, blank=True, default="")

    class Meta:
        verbose_name = "Repasse Vila · config"
        verbose_name_plural = "Repasse Vila · config"

    def __str__(self):
        return f"Repasse Vila · {self.percentual_lucro_padrao}%"


class RepasseVilaCentroAgro(models.Model):
    """Envio de dinheiro da Vila Elias para o Centro (CMV + % lucro + fiado pago)."""

    class StatusCentro(models.TextChoices):
        PENDENTE = "pendente", "Pendente no Centro"
        APLICADO = "aplicado", "Aplicado no caixa Centro"

    data_ref = models.DateField(db_index=True, help_text="Dia das vendas/fiados deste cálculo.")
    percentual_lucro = models.DecimalField(max_digits=5, decimal_places=2, default=50)
    modo_dia_cheio = models.BooleanField(
        default=False,
        help_text="True = mandou o dia cheio de novo (não só o que faltava).",
    )
    incluir_cmv = models.BooleanField(default=True)
    incluir_lucro = models.BooleanField(default=True)
    incluir_fiado = models.BooleanField(default=True)
    valor_cmv = models.DecimalField(max_digits=12, decimal_places=2, default=0)
    valor_lucro = models.DecimalField(max_digits=12, decimal_places=2, default=0)
    valor_fiado = models.DecimalField(max_digits=12, decimal_places=2, default=0)
    valor_total = models.DecimalField(max_digits=12, decimal_places=2, default=0)
    # Snapshot do dia (para histórico / %)
    receita_dia = models.DecimalField(max_digits=12, decimal_places=2, default=0)
    cmv_dia = models.DecimalField(max_digits=12, decimal_places=2, default=0)
    lucro_bruto_dia = models.DecimalField(max_digits=12, decimal_places=2, default=0)
    reserva_aplicada = models.DecimalField(
        max_digits=12,
        decimal_places=2,
        default=0,
        help_text="Cofrinho Salário (config) reservado neste envio — sai do lado que fica na Vila.",
    )
    lucro_penultimo_dia = models.DecimalField(
        max_digits=12,
        decimal_places=2,
        default=0,
        help_text="Base do lucro ao Centro após cofres (legado: antes era lucro−reserva).",
    )
    fiado_pago_dia = models.DecimalField(max_digits=12, decimal_places=2, default=0)
    quem_levou = models.CharField(max_length=120)
    forma_pagamento = models.CharField(max_length=80, default="Dinheiro")
    operador = models.CharField(max_length=120, blank=True, default="")
    usuario = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="repasses_vila_centro",
    )
    sessao_vila = models.ForeignKey(
        SessaoCaixa,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="repasses_vila_saida",
    )
    sessao_centro = models.ForeignKey(
        SessaoCaixa,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="repasses_centro_entrada",
    )
    movimento_saida = models.ForeignKey(
        MovimentoCaixa,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="repasse_vila_saida",
    )
    movimento_entrada = models.ForeignKey(
        MovimentoCaixa,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="repasse_vila_entrada",
    )
    status_centro = models.CharField(
        max_length=16,
        choices=StatusCentro.choices,
        default=StatusCentro.PENDENTE,
        db_index=True,
    )
    aviso_abertura_visto = models.BooleanField(default=False)
    observacao = models.CharField(max_length=500, blank=True, default="")
    criado_em = models.DateTimeField(auto_now_add=True, db_index=True)

    class Meta:
        verbose_name = "Repasse Vila → Centro"
        verbose_name_plural = "Repasses Vila → Centro"
        ordering = ["-criado_em", "-pk"]

    def __str__(self):
        return f"Repasse #{self.pk} {self.data_ref} R$ {self.valor_total}"


class RepasseVilaReservaLogAgro(models.Model):
    """Histórico do valor manual que fica na Vila (config + aplicação no envio)."""

    class Tipo(models.TextChoices):
        CONFIG = "config", "Alteração do valor"
        APLICADO = "aplicado", "Aplicado no envio"
        DESDE = "desde", "Data início diário"

    tipo = models.CharField(max_length=16, choices=Tipo.choices, db_index=True)
    criado_em = models.DateTimeField(auto_now_add=True, db_index=True)
    operador = models.CharField(max_length=120, blank=True, default="")
    data_ref = models.DateField(null=True, blank=True, db_index=True)
    valor_antes = models.DecimalField(max_digits=12, decimal_places=2, default=0)
    valor_depois = models.DecimalField(max_digits=12, decimal_places=2, default=0)
    mensagem = models.CharField(max_length=500, blank=True, default="")
    detalhe = models.JSONField(
        default=dict,
        blank=True,
        help_text="Snapshot: lucro bruto, penúltimo, %, alvos, totais, etc.",
    )
    repasse = models.ForeignKey(
        "RepasseVilaCentroAgro",
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="logs_reserva",
    )

    class Meta:
        verbose_name = "Repasse Vila · log reserva"
        verbose_name_plural = "Repasse Vila · logs reserva"
        ordering = ["-criado_em", "-pk"]

    def __str__(self):
        return f"Reserva log {self.tipo} · {self.criado_em:%d/%m/%Y %H:%M}"


class RepasseVilaReservaMovimentoAgro(models.Model):
    """Razão imutável do dinheiro fisicamente separado nos cofrinhos da Vila."""

    class Tipo(models.TextChoices):
        SEPARACAO = "separacao", "Separação"
        RETIRADA = "retirada", "Retirada / uso"
        AJUSTE = "ajuste", "Ajuste"
        ESTORNO = "estorno", "Estorno"

    class Origem(models.TextChoices):
        FECHAMENTO = "fechamento_caixa", "Fechamento de caixa"
        REPASSE = "repasse", "Repasse Vila → Centro"
        SEPARADO = "lancamento_separado", "Lançamento separado"
        AJUSTE = "ajuste_manual", "Ajuste manual"
        SALDO_INICIAL = "saldo_inicial", "Saldo inicial"
        ESTORNO = "estorno", "Estorno"

    class Cofre(models.TextChoices):
        SALARIO = "salario", "Cofrinho Salário funcionário"
        VILA_ELIAS = "vila_elias", "Cofre Vila Elias"

    tipo = models.CharField(max_length=16, choices=Tipo.choices, db_index=True)
    origem = models.CharField(max_length=24, choices=Origem.choices, db_index=True)
    cofre = models.CharField(
        max_length=16,
        choices=Cofre.choices,
        default=Cofre.SALARIO,
        db_index=True,
        help_text="Qual cofrinho físico este movimento afeta.",
    )
    criado_em = models.DateTimeField(auto_now_add=True, db_index=True)
    data_ref = models.DateField(db_index=True)
    valor = models.DecimalField(
        max_digits=12,
        decimal_places=2,
        help_text="Variação assinada: entrada positiva; retirada negativa.",
    )
    saldo_anterior = models.DecimalField(max_digits=12, decimal_places=2, default=0)
    saldo_posterior = models.DecimalField(max_digits=12, decimal_places=2, default=0)
    operador = models.CharField(max_length=120)
    usuario = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="movimentos_reserva_vila",
    )
    observacao = models.CharField(max_length=500, blank=True, default="")
    idempotencia_chave = models.CharField(max_length=160, unique=True)
    sessao_caixa = models.ForeignKey(
        SessaoCaixa,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="movimentos_reserva_vila",
    )
    movimento_caixa = models.OneToOneField(
        MovimentoCaixa,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="movimento_reserva_vila",
    )
    repasse = models.ForeignKey(
        RepasseVilaCentroAgro,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="movimentos_cofrinho",
    )
    estornado_de = models.OneToOneField(
        "self",
        on_delete=models.PROTECT,
        null=True,
        blank=True,
        related_name="estorno_movimento",
    )
    detalhe = models.JSONField(default=dict, blank=True)

    class Meta:
        verbose_name = "Repasse Vila · movimento do cofrinho"
        verbose_name_plural = "Repasse Vila · movimentos do cofrinho"
        ordering = ["-criado_em", "-pk"]
        indexes = [
            models.Index(fields=["data_ref", "tipo"], name="rv_res_data_tipo_idx"),
        ]

    def __str__(self):
        return f"Cofrinho {self.get_tipo_display()} · {self.data_ref} · {self.valor}"


class RepasseVilaAcumuladoAjusteAgro(models.Model):
    """Ajuste manual no saldo acumulado do repasse Vila → Centro."""

    valor = models.DecimalField(
        max_digits=12,
        decimal_places=2,
        help_text="Positivo = falta levar mais. Negativo = crédito / desconto do acumulado.",
    )
    observacao = models.CharField(max_length=500)
    operador = models.CharField(max_length=120, blank=True, default="")
    data_ref = models.DateField(
        null=True,
        blank=True,
        db_index=True,
        help_text="Dia de referência opcional (só registro).",
    )
    repasse = models.ForeignKey(
        "RepasseVilaCentroAgro",
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="ajustes_acumulado",
        help_text="Repasse que quitou parte do acumulado (automático).",
    )
    criado_em = models.DateTimeField(auto_now_add=True, db_index=True)

    class Meta:
        verbose_name = "Repasse Vila · ajuste acumulado"
        verbose_name_plural = "Repasse Vila · ajustes acumulado"
        ordering = ["-criado_em", "-pk"]

    def __str__(self):
        return f"Ajuste acumulado {self.valor} · {self.criado_em:%d/%m/%Y}"


class RepasseVilaDeltaDiaAgro(models.Model):
    """Cache — delta diário (alvo físico − enviado) para acumulado rápido."""

    data_ref = models.DateField(unique=True, db_index=True)
    alvo_fisico = models.DecimalField(max_digits=12, decimal_places=2, default=0)
    enviado = models.DecimalField(max_digits=12, decimal_places=2, default=0)
    delta = models.DecimalField(max_digits=12, decimal_places=2, default=0)
    atualizado_em = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Repasse Vila · delta dia (cache)"
        verbose_name_plural = "Repasse Vila · deltas dia (cache)"
        ordering = ["-data_ref"]

    def __str__(self):
        return f"Delta {self.data_ref} · {self.delta}"


class TabelaPrecoFormaAgro(models.Model):
    """
    Tabela global de % desconto/acréscimo por forma de pagamento (PDV).
    Dois slots fixos. Não grava preço no cadastro do produto.
    """

    slot = models.PositiveSmallIntegerField(
        "Slot",
        unique=True,
        help_text="1 ou 2",
    )
    nome = models.CharField("Nome", max_length=80, default="Tabela")
    ativo = models.BooleanField("Ativa", default=False, db_index=True)
    percentual = models.DecimalField(
        "% desconto (−) ou acréscimo (+)",
        max_digits=8,
        decimal_places=4,
        default=0,
        help_text="Ex.: −0,55 = desconto 0,55%; +1,5 = acréscimo 1,5%.",
    )
    arredondar_dezena_centavos = models.BooleanField(
        "Arredondar à dezena de centavos",
        default=False,
        help_text="≤4 desce · ≥5 sobe (ex.: 10,43→10,40 · 10,45→10,50).",
    )
    formas = models.JSONField(
        "Formas de pagamento",
        default=list,
        blank=True,
        help_text='Ex.: ["Fiado", "Cartão de crédito"]',
    )
    categorias_vetadas = models.JSONField(
        "Categorias vetadas",
        default=list,
        blank=True,
    )
    produtos_vetados = models.JSONField(
        "Produtos vetados (ids)",
        default=list,
        blank=True,
    )
    atualizado_em = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Tabela preço % por forma"
        verbose_name_plural = "Tabelas preço % por forma"
        ordering = ["slot"]

    def __str__(self):
        return f"T{self.slot} · {self.nome}"


class TabelaPrecoFormaResolucaoAgro(models.Model):
    """Conflito tabela % × preço individual do cadastro — decisão por produto."""

    class Preferencia(models.TextChoices):
        TABELA = "tabela", "Usar tabela %"
        INDIVIDUAL = "individual", "Manter individual"

    tabela = models.ForeignKey(
        TabelaPrecoFormaAgro,
        on_delete=models.CASCADE,
        related_name="resolucoes",
    )
    produto_externo_id = models.CharField(max_length=64, db_index=True)
    preferencia = models.CharField(
        max_length=16,
        choices=Preferencia.choices,
        default=Preferencia.INDIVIDUAL,
    )
    atualizado_em = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Resolução tabela % × individual"
        verbose_name_plural = "Resoluções tabela % × individual"
        constraints = [
            models.UniqueConstraint(
                fields=["tabela", "produto_externo_id"],
                name="uniq_tabela_preco_forma_resolucao",
            ),
        ]

    def __str__(self):
        return f"{self.produto_externo_id} · {self.preferencia}"


class ChatLojaMensagemAgro(models.Model):
    """Grupo único Centro ↔ Vila — todos os PCs do PDV no mesmo canal."""

    CANAL_GERAL = "geral"

    canal = models.CharField(max_length=32, default=CANAL_GERAL, db_index=True)
    texto = models.CharField(max_length=500)
    autor_nome = models.CharField(max_length=120, blank=True, default="")
    deposito = models.CharField(max_length=16, blank=True, default="", db_index=True)
    ponto = models.CharField(max_length=32, blank=True, default="")
    origem_rotulo = models.CharField(max_length=80, blank=True, default="")
    device_id = models.CharField(max_length=64, blank=True, default="", db_index=True)
    criado_em = models.DateTimeField(auto_now_add=True, db_index=True)

    class Meta:
        verbose_name = "Mensagem chat loja"
        verbose_name_plural = "Mensagens chat loja"
        ordering = ["id"]
        indexes = [
            models.Index(fields=["canal", "id"], name="chatloja_canal_id_idx"),
        ]

    def __str__(self):
        trecho = (self.texto or "")[:40]
        return f"#{self.pk} {self.autor_nome}: {trecho}"


class PdvTopbarCliqueDiaAgro(models.Model):
    """Contagem diária de cliques na topbar do PDV (quente/frio · base futura)."""

    botao = models.CharField(max_length=40, db_index=True)
    deposito = models.CharField(max_length=16, blank=True, default="", db_index=True)
    data = models.DateField(db_index=True)
    cliques = models.PositiveIntegerField(default=0)
    atualizado_em = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Clique topbar PDV (dia)"
        verbose_name_plural = "Cliques topbar PDV (dia)"
        constraints = [
            models.UniqueConstraint(
                fields=["botao", "deposito", "data"],
                name="pdv_topbar_clique_botao_dep_dia_uq",
            ),
        ]
        indexes = [
            models.Index(fields=["data", "botao"], name="pdv_topbar_clique_data_btn_idx"),
        ]

    def __str__(self):
        return f"{self.data} {self.botao}@{self.deposito or '-'}: {self.cliques}"


class PdvTopbarLayoutAgro(models.Model):
    """Quente/frio da topbar do PDV — loja inteira (Postgres / multi-PC)."""

    chave = models.CharField(max_length=32, unique=True, default="default")
    quente = models.JSONField(default=list, blank=True)
    frio = models.JSONField(default=list, blank=True)
    atualizado_por = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="pdv_topbar_layouts",
    )
    atualizado_em = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Layout topbar PDV"
        verbose_name_plural = "Layouts topbar PDV"

    def __str__(self):
        return f"topbar:{self.chave}"


class WhatsAppPonteEstadoAgro(models.Model):
    """Singleton da ponte QR (PC da loja ↔ Django)."""

    STATUS_DESCONECTADO = "desconectado"
    STATUS_QR = "qr"
    STATUS_CONECTADO = "conectado"
    STATUS_CHOICES = (
        (STATUS_DESCONECTADO, "Desconectado"),
        (STATUS_QR, "Aguardando QR"),
        (STATUS_CONECTADO, "Conectado"),
    )

    chave = models.CharField(max_length=32, unique=True, default="default")
    status = models.CharField(
        max_length=20, choices=STATUS_CHOICES, default=STATUS_DESCONECTADO, db_index=True
    )
    qr_data_url = models.TextField(blank=True, default="")
    numero = models.CharField(max_length=32, blank=True, default="")
    aviso = models.CharField(max_length=240, blank=True, default="")
    pairing_code = models.CharField(max_length=16, blank=True, default="")
    heartbeat_em = models.DateTimeField(null=True, blank=True, db_index=True)
    atualizado_em = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Ponte WhatsApp (estado)"
        verbose_name_plural = "Ponte WhatsApp (estado)"

    def __str__(self):
        return f"{self.chave} · {self.status}"


class WhatsAppBotConfigAgro(models.Model):
    """Textos, ordem e intervalos do bot WhatsApp (Postgres · uma chave por cliente SisVale)."""

    chave = models.CharField(max_length=32, unique=True, default="default")
    dados = models.JSONField(default=dict, blank=True)
    atualizado_por = models.CharField(max_length=120, blank=True, default="")
    atualizado_em = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Config bot WhatsApp"
        verbose_name_plural = "Configs bot WhatsApp"

    def __str__(self):
        return f"bot {self.chave}"


class WhatsAppConversaAgro(models.Model):
    """Conversa de um cliente no número da loja, roteada para Centro ou Vila."""

    LOJA_PENDENTE = "pendente"
    LOJA_CENTRO = "centro"
    LOJA_VILA = "vila"
    LOJA_CHOICES = (
        (LOJA_PENDENTE, "Fila (escolhe loja)"),
        (LOJA_CENTRO, "Centro"),
        (LOJA_VILA, "Vila Elias"),
    )

    jid = models.CharField(max_length=80, unique=True)
    telefone = models.CharField(max_length=32, blank=True, default="", db_index=True)
    nome = models.CharField(max_length=120, blank=True, default="")
    loja = models.CharField(
        max_length=16, choices=LOJA_CHOICES, default=LOJA_PENDENTE, db_index=True
    )
    menu_enviado = models.BooleanField(default=False)
    nao_lidas = models.PositiveIntegerField(default=0)
    aguardando_loja = models.BooleanField(
        default=False,
        help_text="Cliente falou por último e a loja ainda não respondeu / não concluiu.",
    )
    ultima_preview = models.CharField(max_length=160, blank=True, default="")
    ultima_em = models.DateTimeField(null=True, blank=True, db_index=True)
    origem_abertura = models.CharField(max_length=8, default="in", db_index=True)
    jid_lid = models.CharField(max_length=80, blank=True, null=True, unique=True)
    aviso_fora_em = models.DateTimeField(null=True, blank=True)
    criado_em = models.DateTimeField(auto_now_add=True)

    class Meta:
        verbose_name = "Conversa WhatsApp"
        verbose_name_plural = "Conversas WhatsApp"
        ordering = ["-ultima_em", "-id"]
        indexes = [
            models.Index(fields=["loja", "ultima_em"], name="wa_conv_loja_ult_idx"),
        ]

    def __str__(self):
        return f"{self.telefone or self.jid} · {self.loja}"


class WhatsAppMensagemAgro(models.Model):
    DIRECAO_IN = "in"
    DIRECAO_OUT = "out"
    DIRECAO_BOT = "bot"
    DIRECAO_CHOICES = (
        (DIRECAO_IN, "Cliente"),
        (DIRECAO_OUT, "Loja"),
        (DIRECAO_BOT, "Bot"),
    )

    conversa = models.ForeignKey(
        WhatsAppConversaAgro,
        on_delete=models.CASCADE,
        related_name="mensagens",
    )
    direcao = models.CharField(max_length=8, choices=DIRECAO_CHOICES, db_index=True)
    texto = models.TextField()
    wa_id = models.CharField(max_length=80, blank=True, default="", db_index=True)
    pendente_envio = models.BooleanField(default=False, db_index=True)
    enviado_em = models.DateTimeField(null=True, blank=True)
    erro_envio = models.CharField(max_length=200, blank=True, default="")
    tipo_midia = models.CharField(max_length=16, blank=True, default="")
    arquivo = models.FileField(upload_to="whatsapp/%Y/%m/", blank=True)
    autor_nome = models.CharField(max_length=120, blank=True, default="")
    liberar_envio_em = models.DateTimeField(null=True, blank=True, db_index=True)
    criado_em = models.DateTimeField(default=timezone.now, db_index=True)

    class Meta:
        verbose_name = "Mensagem WhatsApp"
        verbose_name_plural = "Mensagens WhatsApp"
        ordering = ["id"]
        indexes = [
            models.Index(fields=["conversa", "id"], name="wa_msg_conv_id_idx"),
        ]

    def __str__(self):
        return f"#{self.pk} {self.direcao} {(self.texto or '')[:40]}"


class WhatsAppAgendaContatoAgro(models.Model):
    """Agenda do Zap / import .vcf — usada na busca para abrir chat."""

    jid = models.CharField(max_length=80, unique=True)
    telefone = models.CharField(max_length=32, blank=True, default="", db_index=True)
    nome = models.CharField(max_length=120, blank=True, default="")
    atualizado_em = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Contato agenda WhatsApp"
        verbose_name_plural = "Contatos agenda WhatsApp"

    def __str__(self):
        return f"{self.nome or self.telefone or self.jid}"


class WhatsAppPontePedidoAgro(models.Model):
    """Pedido da tela para a ponte (agenda / histórico curto)."""

    TIPO_CONTATOS = "contatos"
    TIPO_HISTORICO = "historico"
    TIPO_PAIRING = "pairing"
    TIPO_LOGOUT = "logout"
    TIPO_CHOICES = (
        (TIPO_CONTATOS, "Agenda"),
        (TIPO_HISTORICO, "Histórico"),
        (TIPO_PAIRING, "Código de ligação"),
        (TIPO_LOGOUT, "Trocar WhatsApp"),
    )
    STATUS_PENDENTE = "pendente"
    STATUS_OK = "ok"
    STATUS_ERRO = "erro"
    STATUS_CHOICES = (
        (STATUS_PENDENTE, "Pendente"),
        (STATUS_OK, "Ok"),
        (STATUS_ERRO, "Erro"),
    )

    tipo = models.CharField(max_length=16, choices=TIPO_CHOICES, db_index=True)
    jid = models.CharField(max_length=80, blank=True, default="", db_index=True)
    payload = models.JSONField(default=dict, blank=True)
    status = models.CharField(
        max_length=12, choices=STATUS_CHOICES, default=STATUS_PENDENTE, db_index=True
    )
    erro = models.CharField(max_length=200, blank=True, default="")
    criado_em = models.DateTimeField(auto_now_add=True, db_index=True)

    class Meta:
        verbose_name = "Pedido ponte WhatsApp"
        verbose_name_plural = "Pedidos ponte WhatsApp"

    def __str__(self):
        return f"{self.tipo} {self.status} {self.jid}"
