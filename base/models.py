from django.db import models
from django.contrib.auth.models import User

class Empresa(models.Model):
    nome_fantasia = models.CharField("Nome fantasia", max_length=150)
    razao_social = models.CharField("Razão social", max_length=200, blank=True)
    cnpj = models.CharField("CNPJ", max_length=18, blank=True)
    ativo = models.BooleanField("Ativa", default=True)
    criado_em = models.DateTimeField("Criado em", auto_now_add=True)

    class Meta:
        verbose_name = "Empresa"
        verbose_name_plural = "Empresas"
        ordering = ["nome_fantasia"]

    def __str__(self):
        return self.nome_fantasia
    
class PerfilUsuario(models.Model):
    user = models.OneToOneField(User, on_delete=models.CASCADE)
    codigo_vendedor = models.CharField(max_length=4, unique=True) # 0001, 0002...
    senha_rapida = models.CharField(max_length=100) # Senha para movimentações
    primeiro_acesso = models.BooleanField(default=True)

    def __str__(self):
        return f"{self.codigo_vendedor} - {self.user.first_name}"


class Loja(models.Model):
    empresa = models.ForeignKey(
        Empresa,
        on_delete=models.CASCADE,
        related_name="lojas",
        verbose_name="Empresa",
    )
    nome = models.CharField("Nome da loja", max_length=120)
    codigo = models.CharField("Código", max_length=30, blank=True)
    cidade = models.CharField("Cidade", max_length=100, blank=True)
    ativa = models.BooleanField("Ativa", default=True)
    criado_em = models.DateTimeField("Criado em", auto_now_add=True)

    class Meta:
        verbose_name = "Loja"
        verbose_name_plural = "Lojas"
        ordering = ["nome"]
        constraints = [
            models.UniqueConstraint(
                fields=["empresa", "codigo"],
                name="unique_codigo_loja_por_empresa",
            )
        ]

    def __str__(self):
        return f"{self.nome} - {self.empresa.nome_fantasia}"


class IntegracaoERP(models.Model):
    TIPO_ERP_CHOICES = [
        ("venda_erp", "Venda ERP"),
        ("outro", "Outro"),
    ]

    empresa = models.ForeignKey(
        Empresa,
        on_delete=models.CASCADE,
        related_name="integracoes",
        verbose_name="Empresa",
    )
    tipo_erp = models.CharField("Tipo de ERP", max_length=30, choices=TIPO_ERP_CHOICES)
    url_base = models.URLField("URL base", blank=True)
    token = models.TextField("Token", blank=True)
    ativo = models.BooleanField("Ativa", default=True)
    ultima_sincronizacao = models.DateTimeField("Última sincronização", null=True, blank=True)
    criado_em = models.DateTimeField("Criado em", auto_now_add=True)
    # Rótulos enviados em Pedidos/Salvar (Venda ERP). Vazios = padrão no código do Agro.
    pedido_empresa_label = models.CharField(
        "Nome empresa (pedido ERP)",
        max_length=200,
        blank=True,
        help_text="Campo 'empresa' do orçamento. Ex.: nome fantasia na nota.",
    )
    pedido_deposito_label = models.CharField(
        "Nome depósito (pedido ERP)",
        max_length=200,
        blank=True,
        help_text="Campo 'deposito' do orçamento.",
    )
    pedido_vendedor_label = models.CharField(
        "Nome vendedor (pedido ERP)",
        max_length=200,
        blank=True,
        help_text="Campo 'vendedor' do orçamento.",
    )

    class Meta:
        verbose_name = "Integração ERP"
        verbose_name_plural = "Integrações ERP"
        ordering = ["empresa__nome_fantasia", "tipo_erp"]

    def __str__(self):
        return f"{self.get_tipo_erp_display()} - {self.empresa.nome_fantasia}"