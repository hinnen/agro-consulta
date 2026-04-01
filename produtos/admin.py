from django.contrib import admin

from .models import (
    ClienteAgro,
    ItemVendaAgro,
    OpcaoBaixaFinanceiroExtra,
    Produto,
    SessaoCaixa,
    VendaAgro,
)


class ItemVendaAgroInline(admin.TabularInline):
    model = ItemVendaAgro
    extra = 0
    can_delete = False
    readonly_fields = (
        "produto_id_externo",
        "codigo",
        "descricao",
        "quantidade",
        "valor_unitario",
        "valor_total",
    )

    def has_add_permission(self, request, obj=None):
        return False


@admin.register(ClienteAgro)
class ClienteAgroAdmin(admin.ModelAdmin):
    list_display = (
        "id",
        "nome",
        "cidade",
        "uf",
        "whatsapp",
        "cpf",
        "externo_id",
        "origem_import",
        "editado_local",
        "ativo",
        "atualizado_em",
    )
    search_fields = (
        "nome",
        "whatsapp",
        "cpf",
        "externo_id",
        "cidade",
        "logradouro",
        "cep",
    )
    list_filter = ("ativo", "origem_import", "editado_local")


@admin.register(SessaoCaixa)
class SessaoCaixaAdmin(admin.ModelAdmin):
    list_display = (
        "id",
        "usuario",
        "aberto_em",
        "valor_abertura",
        "fechado_em",
        "valor_fechamento",
    )
    list_filter = ("fechado_em",)
    readonly_fields = ("aberto_em",)


@admin.register(VendaAgro)
class VendaAgroAdmin(admin.ModelAdmin):
    list_display = (
        "id",
        "criado_em",
        "cliente_nome",
        "total",
        "sessao_caixa",
        "enviado_erp",
        "forma_pagamento",
        "usuario_registro",
    )
    list_filter = ("enviado_erp",)
    search_fields = ("cliente_nome", "cliente_id_erp", "cliente_documento")
    readonly_fields = (
        "cliente_nome",
        "cliente_id_erp",
        "cliente_documento",
        "total",
        "forma_pagamento",
        "enviado_erp",
        "erp_http_status",
        "erp_resposta",
        "usuario_registro",
        "sessao_caixa",
        "criado_em",
    )
    inlines = [ItemVendaAgroInline]

    def has_add_permission(self, request):
        return False


@admin.register(Produto)
class ProdutoAdmin(admin.ModelAdmin):
    list_display = (
        'id',
        'codigo_interno',
        'nome',
        'empresa',
        'marca',
        'categoria',
        'preco_venda',
        'ativo'
    )

    search_fields = (
        'codigo_interno',
        'codigo_barras',
        'nome',
        'marca'
    )

    list_filter = ('empresa', 'categoria', 'marca', 'ativo')


@admin.register(OpcaoBaixaFinanceiroExtra)
class OpcaoBaixaFinanceiroExtraAdmin(admin.ModelAdmin):
    list_display = ("id", "usuario", "tipo", "nome", "id_erp", "criado_em")
    list_filter = ("tipo",)
    search_fields = ("nome", "id_erp", "usuario__username")
    raw_id_fields = ("usuario",)
