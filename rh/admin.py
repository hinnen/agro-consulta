from django.contrib import admin

from .models import (
    FechamentoFolhaSimplificado,
    Funcionario,
    HistoricoSalarial,
    InconsistenciaIntegracaoRh,
    ItemFechamentoFolha,
    ValeFuncionario,
)


class HistoricoSalarialInline(admin.TabularInline):
    model = HistoricoSalarial
    extra = 0
    readonly_fields = ("criado_em",)


@admin.register(Funcionario)
class FuncionarioAdmin(admin.ModelAdmin):
    list_display = ("nome_cache", "cliente_agro", "empresa", "loja", "cargo", "ativo", "atualizado_em")
    list_filter = ("empresa", "ativo", "loja")
    search_fields = ("nome_cache", "apelido_interno", "cliente_agro__nome", "cargo")
    autocomplete_fields = ("cliente_agro",)
    inlines = [HistoricoSalarialInline]


@admin.register(HistoricoSalarial)
class HistoricoSalarialAdmin(admin.ModelAdmin):
    list_display = ("funcionario", "salario_base", "data_inicio_vigencia", "data_fim_vigencia")
    list_filter = ("funcionario__empresa",)


@admin.register(ValeFuncionario)
class ValeFuncionarioAdmin(admin.ModelAdmin):
    list_display = ("funcionario", "data", "valor", "tipo_origem", "cancelado", "empresa")
    list_filter = ("tipo_origem", "cancelado", "empresa")
    search_fields = ("observacao", "referencia_externa_id")
    readonly_fields = ("criado_em", "atualizado_em")


class ItemFechamentoInline(admin.TabularInline):
    model = ItemFechamentoFolha
    extra = 0


@admin.register(FechamentoFolhaSimplificado)
class FechamentoFolhaAdmin(admin.ModelAdmin):
    list_display = (
        "funcionario",
        "competencia",
        "data_vencimento_pagamento",
        "mongo_lancamento_salario_id",
        "salario_base_na_competencia",
        "total_vales",
        "valor_liquido_previsto",
        "valor_pago",
        "status",
    )
    list_filter = ("status", "empresa", "competencia")
    search_fields = ("mongo_lancamento_salario_id", "funcionario__nome_cache")
    inlines = [ItemFechamentoInline]


@admin.register(InconsistenciaIntegracaoRh)
class InconsistenciaRhAdmin(admin.ModelAdmin):
    list_display = ("tipo", "empresa", "resolvida", "criado_em", "referencia_externa_id")
    list_filter = ("tipo", "resolvida", "empresa")
