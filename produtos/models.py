from django.conf import settings
from django.contrib.auth.models import User
from django.db import models


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
    nome = models.CharField(max_length=200)
    categoria = models.CharField(max_length=100, blank=True, null=True)
    marca = models.CharField(max_length=100, blank=True, null=True)
    unidade = models.CharField(max_length=20, default='UN')
    custo = models.DecimalField(max_digits=10, decimal_places=2, default=0)
    preco_venda = models.DecimalField(max_digits=10, decimal_places=2, default=0)
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

    def save(self, *args, **kwargs):
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


class SessaoCaixa(models.Model):
    """Turno de caixa: abertura com fundo de troco; vendas podem ser vinculadas até o fechamento."""

    usuario = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="sessoes_caixa",
    )
    aberto_em = models.DateTimeField(auto_now_add=True)
    valor_abertura = models.DecimalField(max_digits=12, decimal_places=2, default=0)
    observacao_abertura = models.CharField(max_length=500, blank=True, default="")
    fechado_em = models.DateTimeField(null=True, blank=True)
    valor_fechamento = models.DecimalField(
        max_digits=12, decimal_places=2, null=True, blank=True
    )
    observacao_fechamento = models.CharField(max_length=500, blank=True, default="")

    class Meta:
        ordering = ["-aberto_em"]
        verbose_name = "Sessão de caixa"
        verbose_name_plural = "Sessões de caixa"

    def __str__(self):
        dt = self.aberto_em.strftime("%d/%m/%Y %H:%M") if self.aberto_em else ""
        return f"Caixa #{self.pk} — {dt}"


class VendaAgro(models.Model):
    """Venda registrada pelo PDV Agro (fonte local); orçamento pode ser espelhado no ERP."""

    class ErpSyncStatus(models.TextChoices):
        ACEITO = "aceito", "Aceito no ERP"
        RECUSADO_ERP = "recusado_erp", "Recusado pelo ERP"
        FALHA_COMUNICACAO = "falha_comunicacao", "Falha na comunicação"

    cliente_nome = models.CharField(max_length=300, blank=True, default="")
    cliente_id_erp = models.CharField(max_length=32, blank=True, default="")
    cliente_documento = models.CharField(max_length=20, blank=True, default="")
    total = models.DecimalField(max_digits=12, decimal_places=2, default=0)
    forma_pagamento = models.CharField(max_length=80, blank=True, default="")
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

    class Meta:
        ordering = ["-criado_em"]
        verbose_name = "Venda Agro"
        verbose_name_plural = "Vendas Agro"

    def __str__(self):
        return f"Venda #{self.pk} — {self.cliente_nome[:40]} — R$ {self.total}"

    @property
    def erp_sync_efetivo(self) -> str:
        """Valor de exibição para registros sem `erp_sync_status` (legado)."""
        s = (self.erp_sync_status or "").strip()
        if s:
            return s
        return self.ErpSyncStatus.ACEITO if self.enviado_erp else self.ErpSyncStatus.FALHA_COMUNICACAO


class PdvMercadoPagoPointOrder(models.Model):
    """Pedido Point criado a partir do PDV; após pagamento no terminal, dispara Pedidos/Salvar."""

    class Status(models.TextChoices):
        PENDING = "pending", "Aguardando pagamento"
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

    class Meta:
        verbose_name = "Item de venda Agro"
        verbose_name_plural = "Itens de venda Agro"

    def __str__(self):
        return f"{self.descricao[:30]} x {self.quantidade}"


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
    Sobrescritas locais na tela de gestão (nome, preço, etc.) sobre o cadastro espelhado do ERP.
    Campos vazios / nulos = usar valor do Mongo. Não altera o ERP.
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
    preco_venda = models.DecimalField(
        max_digits=12,
        decimal_places=2,
        null=True,
        blank=True,
        verbose_name="Preço de venda (override)",
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