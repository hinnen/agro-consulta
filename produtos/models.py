from django.conf import settings
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

    cliente_nome = models.CharField(max_length=300, blank=True, default="")
    cliente_id_erp = models.CharField(max_length=32, blank=True, default="")
    cliente_documento = models.CharField(max_length=20, blank=True, default="")
    total = models.DecimalField(max_digits=12, decimal_places=2, default=0)
    forma_pagamento = models.CharField(max_length=80, blank=True, default="")
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

    class Meta:
        ordering = ["-criado_em"]
        verbose_name = "Venda Agro"
        verbose_name_plural = "Vendas Agro"

    def __str__(self):
        return f"Venda #{self.pk} — {self.cliente_nome[:40]} — R$ {self.total}"


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