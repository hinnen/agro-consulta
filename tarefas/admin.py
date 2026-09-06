from django.contrib import admin

from .models import TarefaAgro, TarefaComentarioAgro, TarefaEventoAgro


@admin.register(TarefaAgro)
class TarefaAgroAdmin(admin.ModelAdmin):
    list_display = ("titulo", "status", "loja", "atualizado_por_nome", "atualizado_em")
    list_filter = ("status", "loja")
    search_fields = ("titulo", "descricao", "seed_key")


@admin.register(TarefaComentarioAgro)
class TarefaComentarioAgroAdmin(admin.ModelAdmin):
    list_display = ("tarefa", "autor_nome", "criado_em")
    search_fields = ("texto", "autor_nome")


@admin.register(TarefaEventoAgro)
class TarefaEventoAgroAdmin(admin.ModelAdmin):
    list_display = ("tarefa", "tipo", "autor_nome", "criado_em")
    list_filter = ("tipo",)
