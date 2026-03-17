from django.contrib import admin
from .models import Estoque, AjusteRapidoEstoque


@admin.register(Estoque)
class EstoqueAdmin(admin.ModelAdmin):
    list_display = ('produto', 'loja', 'saldo', 'estoque_minimo', 'atualizado_em')
    search_fields = ('produto__nome', 'produto__codigo_interno', 'loja__nome')
    list_filter = ('loja',)


@admin.register(AjusteRapidoEstoque)
class AjusteRapidoEstoqueAdmin(admin.ModelAdmin):
    list_display = (
        'nome_produto',
        'codigo_interno',
        'deposito',
        'saldo_erp_anterior',
        'saldo_ajustado',
        'criado_em',
    )
    search_fields = ('nome_produto', 'codigo_interno', 'produto_externo_id')
    list_filter = ('deposito', 'criado_em')