"""Catálogo de fontes de log / auditoria — painel Gestão → LOG."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class FonteLogGestao:
    id: str
    titulo: str
    descricao: str
    icone: str  # lucide
    admin_model: str | None = None  # "produtos.GmLojasAppLogAgro"
    pagina_url_name: str | None = None  # tela Agro em vez de admin
    contagem_fn: str | None = None  # nome em gestao_logs_util


def catalogo_fontes_log() -> list[FonteLogGestao]:
    return [
        FonteLogGestao(
            id="gm_lojas",
            titulo="App GM Lojas (celular)",
            descricao="PIN, hub, fotos, etiquetas, vendas e tarefas no PWA.",
            icone="smartphone",
            admin_model="produtos.GmLojasAppLogAgro",
            contagem_fn="count_gm_lojas",
        ),
        FonteLogGestao(
            id="ajuste_estoque",
            titulo="Ajustes de estoque",
            descricao="Histórico de ajustes rápidos e correções operacionais.",
            icone="package-search",
            pagina_url_name="historico_ajustes",
        ),
        FonteLogGestao(
            id="etiqueta_impressao",
            titulo="Impressão de etiquetas",
            descricao="Jobs e reimpressões de etiquetas de preço.",
            icone="tags",
            admin_model="produtos.EtiquetaImpressaoHistoricoAgro",
            contagem_fn="count_etiqueta_impressao",
        ),
        FonteLogGestao(
            id="repasse_vila",
            titulo="Repasse Vila (reserva)",
            descricao="Log de configuração e aplicação da reserva Vila → Centro.",
            icone="arrow-right-left",
            admin_model="produtos.RepasseVilaReservaLogAgro",
            contagem_fn="count_repasse_vila",
        ),
        FonteLogGestao(
            id="import_planilha",
            titulo="Importação planilha cadastro",
            descricao="Backups e histórico de imports Excel do cadastro.",
            icone="file-spreadsheet",
            admin_model="produtos.CadastroPlanilhaImportHistoricoAgro",
            contagem_fn="count_import_planilha",
        ),
        FonteLogGestao(
            id="cliente_eventos",
            titulo="Eventos de clientes",
            descricao="Transferências, exclusões, vales e alterações no cadastro.",
            icone="users",
            admin_model="produtos.ClienteAgroEventoAgro",
            contagem_fn="count_cliente_eventos",
        ),
        FonteLogGestao(
            id="fiado_eventos",
            titulo="Eventos fiado",
            descricao="Baixas, conferências e movimentação de títulos fiado.",
            icone="wallet",
            admin_model="produtos.FiadoEventoAgro",
            contagem_fn="count_fiado_eventos",
        ),
        FonteLogGestao(
            id="tarefa_eventos",
            titulo="Pendências (tarefas)",
            descricao="Linha do tempo e comentários do app GM Pendências.",
            icone="list-todo",
            admin_model="tarefas.TarefaEventoAgro",
            contagem_fn="count_tarefa_eventos",
        ),
        FonteLogGestao(
            id="bugs",
            titulo="Bugs / feedback",
            descricao="Relatos enviados pela loja (menu Gestão).",
            icone="bug",
            pagina_url_name="bug_reports_lista",
            contagem_fn="count_bugs",
        ),
    ]


def admin_changelist_url(model_label: str) -> str | None:
    """model_label = app_label.ModelName"""
    try:
        from django.apps import apps

        model = apps.get_model(model_label)
        meta = model._meta
        from django.urls import reverse

        return reverse(f"admin:{meta.app_label}_{meta.model_name}_changelist")
    except Exception:
        return None
