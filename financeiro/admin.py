from django.contrib import admin

from .models import GrupoEmpresarial, GrupoEmpresarialEmpresa, LancamentoFinanceiro


@admin.register(GrupoEmpresarial)
class GrupoEmpresarialAdmin(admin.ModelAdmin):
    list_display = ("nome", "empresa_pai", "ativo", "atualizado_em")
    list_filter = ("ativo",)
    search_fields = ("nome",)


@admin.register(GrupoEmpresarialEmpresa)
class GrupoEmpresarialEmpresaAdmin(admin.ModelAdmin):
    list_display = ("grupo", "empresa", "tipo", "ativo")
    list_filter = ("tipo", "ativo")


@admin.register(LancamentoFinanceiro)
class LancamentoFinanceiroAdmin(admin.ModelAdmin):
    list_display = (
        "data_competencia",
        "empresa",
        "natureza",
        "valor",
        "descricao",
        "eh_interno_grupo",
    )
    list_filter = ("natureza", "origem", "eh_interno_grupo")
    search_fields = ("descricao", "documento_ref", "grupo_ref")
    date_hierarchy = "data_competencia"
