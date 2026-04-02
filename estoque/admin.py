from django.contrib import admin
from .models import (
    AjusteRapidoEstoque,
    Estoque,
    IndicadorProdutoLoja,
    PoliticaEstoque,
)


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


@admin.register(PoliticaEstoque)
class PoliticaEstoqueAdmin(admin.ModelAdmin):
    list_display = ("empresa", "loja", "produto", "dias_cobertura", "prioridade_manual")
    list_filter = ("empresa", "loja")


@admin.register(IndicadorProdutoLoja)
class IndicadorProdutoLojaAdmin(admin.ModelAdmin):
    list_display = (
        "data_base",
        "empresa",
        "loja",
        "produto",
        "necessidade",
        "sugestao_acao",
        "score_prioridade",
    )
    list_filter = ("empresa", "loja", "data_base", "classe_abc")
<<<<<<< Current (Your changes)
    search_fields = ("produto__nome", "produto__codigo_interno")

=======
    search_fields = ("produto__nome", "produto__codigo_interno")
>>>>>>> Incoming (Background Agent changes)
