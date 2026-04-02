from django.db import models


class GrupoEmpresarial(models.Model):
    nome = models.CharField(max_length=150, unique=True)
    empresa_pai = models.ForeignKey(
        "base.Empresa",
        on_delete=models.PROTECT,
        related_name="grupos_como_pai",
    )
    ativo = models.BooleanField(default=True)
    criado_em = models.DateTimeField(auto_now_add=True)
    atualizado_em = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "grupo_empresarial"
        verbose_name = "Grupo Empresarial"
        verbose_name_plural = "Grupos Empresariais"

    def __str__(self):
        return self.nome


class GrupoEmpresarialEmpresa(models.Model):
    TIPO_PAI = "PAI"
    TIPO_FILHA = "FILHA"
    TIPO_COLIGADA = "COLIGADA"

    TIPOS = [
        (TIPO_PAI, "Pai"),
        (TIPO_FILHA, "Filha"),
        (TIPO_COLIGADA, "Coligada"),
    ]

    grupo = models.ForeignKey(
        GrupoEmpresarial,
        on_delete=models.CASCADE,
        related_name="empresas_vinculadas",
    )
    empresa = models.ForeignKey(
        "base.Empresa",
        on_delete=models.CASCADE,
        related_name="grupos_vinculados",
    )
    tipo = models.CharField(max_length=20, choices=TIPOS, default=TIPO_FILHA)
    ativo = models.BooleanField(default=True)
    criado_em = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "grupo_empresarial_empresa"
        constraints = [
            models.UniqueConstraint(
                fields=["grupo", "empresa"],
                name="uniq_grupo_empresarial_empresa",
            )
        ]
        indexes = [
            models.Index(fields=["grupo", "empresa"]),
            models.Index(fields=["empresa", "ativo"]),
        ]

    def __str__(self):
        return f"{self.grupo.nome} - {self.empresa} ({self.tipo})"


class LancamentoFinanceiro(models.Model):
    NATUREZA_RECEITA_OPERACIONAL = "RECEITA_OPERACIONAL"
    NATUREZA_RECEITA_NAO_OPERACIONAL = "RECEITA_NAO_OPERACIONAL"
    NATUREZA_CMV = "CMV"
    NATUREZA_DESPESA_FIXA = "DESPESA_FIXA"
    NATUREZA_DESPESA_VARIAVEL = "DESPESA_VARIAVEL"
    NATUREZA_DESPESA_FINANCEIRA = "DESPESA_FINANCEIRA"
    NATUREZA_EMPRESTIMO_ENTRADA = "EMPRESTIMO_ENTRADA"
    NATUREZA_EMPRESTIMO_AMORTIZACAO = "EMPRESTIMO_AMORTIZACAO"
    NATUREZA_TRANSFERENCIA_INTERNA = "TRANSFERENCIA_INTERNA"
    NATUREZA_APORTE_SOCIO = "APORTE_SOCIO"
    NATUREZA_RETIRADA_SOCIO = "RETIRADA_SOCIO"

    NATUREZAS = [
        (NATUREZA_RECEITA_OPERACIONAL, "Receita Operacional"),
        (NATUREZA_RECEITA_NAO_OPERACIONAL, "Receita Não Operacional"),
        (NATUREZA_CMV, "CMV"),
        (NATUREZA_DESPESA_FIXA, "Despesa Fixa"),
        (NATUREZA_DESPESA_VARIAVEL, "Despesa Variável"),
        (NATUREZA_DESPESA_FINANCEIRA, "Despesa Financeira"),
        (NATUREZA_EMPRESTIMO_ENTRADA, "Entrada de Empréstimo"),
        (NATUREZA_EMPRESTIMO_AMORTIZACAO, "Amortização de Empréstimo"),
        (NATUREZA_TRANSFERENCIA_INTERNA, "Transferência Interna"),
        (NATUREZA_APORTE_SOCIO, "Aporte de Sócio"),
        (NATUREZA_RETIRADA_SOCIO, "Retirada de Sócio"),
    ]

    ORIGEM_MANUAL = "MANUAL"
    ORIGEM_VENDA_ERP = "VENDA_ERP"
    ORIGEM_COMPRA_ERP = "COMPRA_ERP"
    ORIGEM_TRANSFERENCIA_ERP = "TRANSFERENCIA_ERP"
    ORIGEM_CONCILIACAO = "CONCILIACAO"

    ORIGENS = [
        (ORIGEM_MANUAL, "Manual"),
        (ORIGEM_VENDA_ERP, "Venda ERP"),
        (ORIGEM_COMPRA_ERP, "Compra ERP"),
        (ORIGEM_TRANSFERENCIA_ERP, "Transferência ERP"),
        (ORIGEM_CONCILIACAO, "Conciliação"),
    ]

    empresa = models.ForeignKey("base.Empresa", on_delete=models.PROTECT)
    loja = models.ForeignKey(
        "base.Loja",
        null=True,
        blank=True,
        on_delete=models.PROTECT,
    )

    data_competencia = models.DateField(db_index=True)
    data_movimento = models.DateField(db_index=True)

    descricao = models.CharField(max_length=255)
    valor = models.DecimalField(max_digits=14, decimal_places=2)

    natureza = models.CharField(max_length=40, choices=NATUREZAS, db_index=True)
    origem = models.CharField(max_length=30, choices=ORIGENS, default=ORIGEM_MANUAL)

    plano_conta_codigo = models.CharField(max_length=50, blank=True, default="")
    plano_conta_descricao = models.CharField(max_length=255, blank=True, default="")

    documento_ref = models.CharField(max_length=120, blank=True, default="")
    grupo_ref = models.CharField(max_length=120, blank=True, default="")

    eh_interno_grupo = models.BooleanField(default=False, db_index=True)
    empresa_contraparte = models.ForeignKey(
        "base.Empresa",
        null=True,
        blank=True,
        on_delete=models.PROTECT,
        related_name="lancamentos_como_contraparte",
    )

    observacoes = models.TextField(blank=True, default="")
    criado_em = models.DateTimeField(auto_now_add=True)
    atualizado_em = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "lancamento_financeiro"
        indexes = [
            models.Index(fields=["empresa", "data_competencia"]),
            models.Index(fields=["empresa", "natureza", "data_competencia"]),
            models.Index(fields=["eh_interno_grupo", "data_competencia"]),
            models.Index(fields=["grupo_ref"]),
            models.Index(fields=["empresa", "loja", "data_competencia"]),
        ]
