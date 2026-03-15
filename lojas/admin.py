from django.contrib import admin
from .models import Loja


@admin.register(Loja)
class LojaAdmin(admin.ModelAdmin):
    list_display = ('id', 'nome', 'codigo', 'ativa')
    search_fields = ('nome', 'codigo')
    list_filter = ('ativa',)