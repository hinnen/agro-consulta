from django.contrib.auth.models import User
from django.db import models
from django.db.models import Q


class Funcionario(models.Model):
    """
    Perfil complementar de RH: sempre amarrado a uma pessoa base (ClienteAgro / espelho ERP).
    Dados cadastrais (CPF, telefone, nome canônico) vivem em ClienteAgro.
    """

    cliente_agro = models.ForeignKey(
        "produtos.ClienteAgro",
        on_delete=models.PROTECT,
        related_name="perfis_rh",
        verbose_name="Pessoa base (ClienteAgro)",
    )
    empresa = models.ForeignKey(
        "base.Empresa",
        on_delete=models.CASCADE,
        related_name="funcionarios",
    )
    loja = models.ForeignKey(
        "base.Loja",
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="funcionarios",
    )
    nome_cache = models.CharField(
        max_length=200,
        help_text="Cópia para listagens e correspondência com descrições legadas (ex.: caixa). Atualize se o nome no ERP mudar.",
    )
    apelido_interno = models.CharField(max_length=120, blank=True)
    cargo = models.CharField(max_length=120, blank=True)
    data_admissao = models.DateField(null=True, blank=True)
    ativo = models.BooleanField(default=True)
    observacoes = models.TextField(blank=True)
    criado_em = models.DateTimeField(auto_now_add=True)
    atualizado_em = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["empresa_id", "nome_cache"]
        verbose_name = "Funcionário (perfil RH)"
        verbose_name_plural = "Funcionários (perfis RH)"
        indexes = [
            models.Index(fields=["empresa", "ativo", "nome_cache"]),
        ]
        constraints = [
            models.UniqueConstraint(
                fields=["empresa", "cliente_agro"],
                condition=models.Q(ativo=True),
                name="rh_funcionario_unique_cliente_empresa_ativo",
            ),
        ]

    def __str__(self):
        return f"{self.nome_exibicao} ({self.empresa})"

    @property
    def nome_exibicao(self) -> str:
        nc = (self.nome_cache or "").strip()
        if nc:
            return nc
        if self.cliente_agro_id:
            return (self.cliente_agro.nome or "").strip() or f"Cliente #{self.cliente_agro_id}"
        return "—"

    def save(self, *args, **kwargs):
        if self.cliente_agro_id and not (self.nome_cache or "").strip():
            self.nome_cache = (self.cliente_agro.nome or "")[:200]
        super().save(*args, **kwargs)


class HistoricoSalarial(models.Model):
    funcionario = models.ForeignKey(
        Funcionario,
        on_delete=models.CASCADE,
        related_name="historicos_salario",
    )
    salario_base = models.DecimalField(max_digits=12, decimal_places=2)
    data_inicio_vigencia = models.DateField()
    data_fim_vigencia = models.DateField(null=True, blank=True)
    motivo_alteracao = models.CharField(max_length=300, blank=True)
    criado_em = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["-data_inicio_vigencia", "-id"]
        verbose_name = "Histórico salarial"
        verbose_name_plural = "Históricos salariais"

    def __str__(self):
        return f"{self.funcionario} — {self.salario_base} a partir de {self.data_inicio_vigencia}"


class ValeFuncionario(models.Model):
    class TipoOrigem(models.TextChoices):
        MANUAL = "MANUAL", "Manual"
        CAIXAS = "CAIXAS", "Caixas"

    funcionario = models.ForeignKey(
        Funcionario,
        on_delete=models.CASCADE,
        related_name="vales",
    )
    empresa = models.ForeignKey(
        "base.Empresa",
        on_delete=models.CASCADE,
        related_name="vales_funcionario",
    )
    loja = models.ForeignKey(
        "base.Loja",
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="vales_funcionario",
    )
    data = models.DateField()
    valor = models.DecimalField(max_digits=12, decimal_places=2)
    tipo_origem = models.CharField(max_length=20, choices=TipoOrigem.choices)
    observacao = models.TextField(blank=True)
    referencia_externa_tipo = models.CharField(max_length=80, blank=True)
    referencia_externa_id = models.CharField(max_length=64, blank=True)
    criado_por = models.ForeignKey(
        User,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="vales_rh_criados",
    )
    criado_em = models.DateTimeField(auto_now_add=True)
    atualizado_em = models.DateTimeField(auto_now=True)
    cancelado = models.BooleanField(default=False)
    cancelado_em = models.DateTimeField(null=True, blank=True)
    motivo_cancelamento = models.CharField(max_length=400, blank=True)

    class Meta:
        ordering = ["-data", "-id"]
        verbose_name = "Vale / adiantamento"
        verbose_name_plural = "Vales / adiantamentos"
        indexes = [
            models.Index(fields=["empresa", "data"]),
            models.Index(fields=["funcionario", "data"]),
        ]
        constraints = [
            models.UniqueConstraint(
                fields=["referencia_externa_tipo", "referencia_externa_id"],
                condition=models.Q(referencia_externa_id__gt=""),
                name="rh_vale_unico_ref_externa",
            ),
        ]

    def __str__(self):
        return f"{self.funcionario} — {self.valor} em {self.data}"


class FechamentoFolhaSimplificado(models.Model):
    class Status(models.TextChoices):
        ABERTO = "ABERTO", "Aberto"
        FECHADO = "FECHADO", "Fechado"
        PAGO_PARCIAL = "PAGO_PARCIAL", "Pago parcial"
        PAGO = "PAGO", "Pago"

    funcionario = models.ForeignKey(
        Funcionario,
        on_delete=models.CASCADE,
        related_name="fechamentos",
    )
    empresa = models.ForeignKey(
        "base.Empresa",
        on_delete=models.CASCADE,
        related_name="fechamentos_folha",
    )
    competencia = models.DateField(
        help_text="Primeiro dia do mês (ex.: 2025-04-01).",
    )
    salario_base_na_competencia = models.DecimalField(max_digits=12, decimal_places=2)
    total_vales = models.DecimalField(max_digits=12, decimal_places=2, default=0)
    outros_descontos = models.DecimalField(max_digits=12, decimal_places=2, default=0)
    outros_proventos = models.DecimalField(max_digits=12, decimal_places=2, default=0)
    valor_liquido_previsto = models.DecimalField(max_digits=12, decimal_places=2, default=0)
    valor_pago = models.DecimalField(max_digits=12, decimal_places=2, default=0)
    status = models.CharField(
        max_length=20,
        choices=Status.choices,
        default=Status.ABERTO,
    )
    observacoes = models.TextField(blank=True)
    fechado_em = models.DateTimeField(null=True, blank=True)
    data_vencimento_pagamento = models.DateField(
        null=True,
        blank=True,
        help_text="Vencimento do título de salário no financeiro (Mongo).",
    )
    mongo_lancamento_salario_id = models.CharField(
        max_length=32,
        blank=True,
        default="",
        help_text="ObjectId do DtoLancamento único de salário (despesa) no Mongo.",
    )
    criado_em = models.DateTimeField(auto_now_add=True)
    atualizado_em = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-competencia", "funcionario__nome_cache"]
        verbose_name = "Fechamento de folha (simplificado)"
        verbose_name_plural = "Fechamentos de folha"
        indexes = [
            models.Index(fields=["empresa", "competencia", "status"]),
        ]
        constraints = [
            models.UniqueConstraint(
                fields=["funcionario", "competencia"],
                name="rh_fechamento_unico_func_comp",
            ),
        ]

    def __str__(self):
        return f"{self.funcionario} — {self.competencia:%Y-%m}"


class ItemFechamentoFolha(models.Model):
    class Tipo(models.TextChoices):
        SALARIO_BASE = "SALARIO_BASE", "Salário base"
        VALE = "VALE", "Vale"
        DESCONTO = "DESCONTO", "Desconto"
        ACRESCIMO = "ACRESCIMO", "Acréscimo"
        AJUSTE = "AJUSTE", "Ajuste"

    fechamento = models.ForeignKey(
        FechamentoFolhaSimplificado,
        on_delete=models.CASCADE,
        related_name="itens",
    )
    tipo = models.CharField(max_length=20, choices=Tipo.choices)
    descricao = models.CharField(max_length=400)
    valor = models.DecimalField(max_digits=12, decimal_places=2)
    referencia_tipo = models.CharField(max_length=80, blank=True)
    referencia_id = models.CharField(max_length=64, blank=True)
    ordem = models.PositiveSmallIntegerField(default=0)

    class Meta:
        ordering = ["ordem", "id"]
        verbose_name = "Item de fechamento"
        verbose_name_plural = "Itens de fechamento"


class InconsistenciaIntegracaoRh(models.Model):
    class Tipo(models.TextChoices):
        VALE_SEM_FUNCIONARIO = "VALE_SEM_FUNCIONARIO", "Vale (caixa) sem funcionário"
        FUNCIONARIO_INATIVO = "FUNCIONARIO_INATIVO", "Funcionário inativo"
        DIVERGENCIA = "DIVERGENCIA", "Divergência com lançamento"
        LANCAMENTO_REMOVIDO = "LANCAMENTO_REMOVIDO", "Lançamento removido no financeiro"

    empresa = models.ForeignKey(
        "base.Empresa",
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="rh_inconsistencias",
    )
    tipo = models.CharField(max_length=40, choices=Tipo.choices)
    referencia_externa_tipo = models.CharField(max_length=80, blank=True)
    referencia_externa_id = models.CharField(max_length=64, blank=True)
    detalhe = models.TextField(blank=True)
    resolvida = models.BooleanField(default=False)
    resolvida_em = models.DateTimeField(null=True, blank=True)
    criado_em = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["-criado_em"]
        verbose_name = "Inconsistência (integração RH)"
        verbose_name_plural = "Inconsistências (integração RH)"


def total_vales_funcionario_mes(funcionario: Funcionario, ano: int, mes: int) -> "decimal.Decimal":
    from rh.services.fechamento import total_vales_mes

    return total_vales_mes(funcionario, ano, mes)
