from django.contrib import admin
from .models import Estoque


@admin.register(Estoque)
class EstoqueAdmin(admin.ModelAdmin):
    list_display = (
        'produto',
        'loja',
        'saldo',
        'estoque_minimo',
        'atualizado_em'
    )

    search_fields = (
        'produto__nome',
        'produto__codigo_interno',
        'loja__nome'
    )

    list_filter = ('loja',)