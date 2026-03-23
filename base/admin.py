from django.contrib import admin
from .models import Empresa, Loja, IntegracaoERP


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
from django.contrib import admin
from .models import PerfilUsuario

@admin.register(PerfilUsuario)
class PerfilUsuarioAdmin(admin.ModelAdmin):
    list_display = ('codigo_vendedor', 'user', 'senha_rapida')
