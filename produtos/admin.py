from django.contrib import admin
from .models import Produto
from estoque.models import AjusteRapidoEstoque
from django.contrib import admin


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
