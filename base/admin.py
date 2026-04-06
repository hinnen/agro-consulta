from django.contrib import admin

from .models import Empresa, IntegracaoERP, Loja, PerfilUsuario


@admin.register(Empresa)
class EmpresaAdmin(admin.ModelAdmin):
    list_display = ("id", "nome_fantasia", "cnpj", "ativo", "criado_em")
    search_fields = ("nome_fantasia", "razao_social", "cnpj")
    list_filter = ("ativo",)


@admin.register(Loja)
class LojaAdmin(admin.ModelAdmin):
    list_display = ("id", "nome", "empresa", "codigo", "cidade", "ativa", "criado_em")
    search_fields = ("nome", "codigo", "cidade", "empresa__nome_fantasia")
    list_filter = ("ativa", "empresa")


@admin.register(IntegracaoERP)
class IntegracaoERPAdmin(admin.ModelAdmin):
    list_display = ("id", "empresa", "tipo_erp", "ativo", "ultima_sincronizacao", "criado_em")
    search_fields = ("empresa__nome_fantasia", "tipo_erp")
    list_filter = ("ativo", "tipo_erp", "empresa")
    fieldsets = (
        (None, {"fields": ("empresa", "tipo_erp", "url_base", "token", "ativo", "ultima_sincronizacao")}),
        (
            "Rótulos do pedido (Venda ERP)",
            {
                "fields": (
                    "pedido_empresa_label",
                    "pedido_deposito_label",
                    "pedido_vendedor_label",
                    "pedido_status_sistema",
                    "pedido_plano_conta",
                    "pedido_plano_conta_id",
                ),
                "description": "Opcional. Se vazio, o sistema usa valores padrão ao enviar o orçamento.",
            },
        ),
    )


@admin.register(PerfilUsuario)
class PerfilUsuarioAdmin(admin.ModelAdmin):
    list_display = ('codigo_vendedor', 'user', 'senha_rapida')
