from django.contrib import admin
from .models import Estoque, AjusteRapidoEstoque


@admin.register(Estoque)
class EstoqueAdmin(admin.ModelAdmin):
    list_display = (
        'id',
        'empresa',
        'loja',
        'produto',
        'saldo',
        'estoque_minimo',
        'atualizado_em',
    )
    search_fields = (
        'produto__nome',
        'produto__codigo_interno',
        'loja__nome',
        'empresa__nome_fantasia',
    )
    list_filter = ('empresa', 'loja')


@admin.register(AjusteRapidoEstoque)
class AjusteRapidoEstoqueAdmin(admin.ModelAdmin):
    list_display = (
        'id',
        'empresa',
        'loja',
        'nome_produto',
        'deposito',
        'saldo_erp_referencia',
        'saldo_informado',
        'diferenca_saldo',
        'criado_em',
    )
    search_fields = (
        'nome_produto',
        'codigo_interno',
        'produto_externo_id',
        'empresa__nome_fantasia',
        'loja__nome',
    )
    list_filter = ('empresa', 'loja', 'deposito', 'criado_em')