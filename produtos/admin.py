from django.contrib import admin
from .models import Produto


@admin.register(Produto)
class ProdutoAdmin(admin.ModelAdmin):
    list_display = (
        'id',
        'codigo_interno',
        'nome',
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

    list_filter = ('categoria', 'marca', 'ativo')