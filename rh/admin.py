from django.contrib import admin, messages

from .models import (
    FechamentoFolhaSimplificado,
    Funcionario,
    HistoricoSalarial,
    InconsistenciaIntegracaoRh,
    ItemFechamentoFolha,
    PagamentoSalarioFuncionario,
    ValeFuncionario,
)


class HistoricoSalarialInline(admin.TabularInline):
    model = HistoricoSalarial
    extra = 0
    readonly_fields = ("criado_em",)


@admin.register(Funcionario)
class FuncionarioAdmin(admin.ModelAdmin):
    list_display = (
        "nome_cache",
        "cliente_agro",
        "empresa",
        "loja",
        "cargo",
        "dia_envio_cp_auto",
        "dia_vencimento_salario",
        "ativo",
        "atualizado_em",
    )
    list_filter = ("empresa", "ativo", "loja", "dia_envio_cp_auto")
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


@admin.register(PagamentoSalarioFuncionario)
class PagamentoSalarioFuncionarioAdmin(admin.ModelAdmin):
    list_display = (
        "funcionario",
        "data",
        "valor",
        "tipo_origem",
        "fechamento",
        "cancelado",
        "criado_em",
    )
    list_filter = ("tipo_origem", "cancelado", "empresa", "data")
    search_fields = (
        "funcionario__nome_cache",
        "observacao",
        "referencia_externa_id",
        "fechamento__mongo_lancamento_salario_id",
    )
    readonly_fields = ("criado_em", "atualizado_em", "cancelado_em")
    autocomplete_fields = ("funcionario", "fechamento")
    actions = ("cancelar_pagamentos_e_sincronizar_cp",)

    @admin.action(description="Cancelar selecionados e sincronizar CP da folha")
    def cancelar_pagamentos_e_sincronizar_cp(self, request, queryset):
        from rh.services.pagamento_salario import cancelar_pagamento_salario

        ok = 0
        erros: list[str] = []
        for p in queryset.filter(cancelado=False).order_by("id"):
            r = cancelar_pagamento_salario(
                p,
                motivo=f"Cancelado no admin por {request.user}",
                sincronizar_cp=True,
            )
            if r.get("ok"):
                ok += 1
            else:
                erros.append(r.get("erro") or f"Falha id {p.pk}")
        if ok:
            self.message_user(request, f"{ok} pagamento(s) cancelado(s); CP realinhado.", messages.SUCCESS)
        for e in erros[:5]:
            self.message_user(request, e, messages.ERROR)


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
