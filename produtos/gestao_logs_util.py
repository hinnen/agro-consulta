"""Consultas do painel Gestão → LOG."""

from __future__ import annotations

from django.apps import apps
from django.db.models import Model, QuerySet

from produtos.gestao_logs_catalog import FonteLogGestao, catalogo_fontes_log


def _model(label: str) -> type[Model]:
    return apps.get_model(label)


def count_gm_lojas() -> int:
    return _model("produtos.GmLojasAppLogAgro").objects.count()


def count_etiqueta_impressao() -> int:
    return _model("produtos.EtiquetaImpressaoHistoricoAgro").objects.count()


def count_repasse_vila() -> int:
    return _model("produtos.RepasseVilaReservaLogAgro").objects.count()


def count_import_planilha() -> int:
    return _model("produtos.CadastroPlanilhaImportHistoricoAgro").objects.count()


def count_cliente_eventos() -> int:
    return _model("produtos.ClienteAgroEventoAgro").objects.count()


def count_fiado_eventos() -> int:
    return _model("produtos.FiadoEventoAgro").objects.count()


def count_tarefa_eventos() -> int:
    return _model("tarefas.TarefaEventoAgro").objects.count()


def count_bugs() -> int:
    return _model("produtos.BugReportAgro").objects.count()


_COUNTERS = {
    "count_gm_lojas": count_gm_lojas,
    "count_etiqueta_impressao": count_etiqueta_impressao,
    "count_repasse_vila": count_repasse_vila,
    "count_import_planilha": count_import_planilha,
    "count_cliente_eventos": count_cliente_eventos,
    "count_fiado_eventos": count_fiado_eventos,
    "count_tarefa_eventos": count_tarefa_eventos,
    "count_bugs": count_bugs,
}


def contagem_fonte(fonte: FonteLogGestao) -> int | None:
    fn_name = fonte.contagem_fn
    if not fn_name:
        return None
    fn = _COUNTERS.get(fn_name)
    if not fn:
        return None
    try:
        return int(fn())
    except Exception:
        return None


def queryset_fonte(fonte_id: str, busca: str = "", limite: int = 150) -> tuple[QuerySet | None, str]:
    busca = (busca or "").strip()[:80]
    limite = max(20, min(int(limite or 150), 500))
    if fonte_id == "gm_lojas":
        qs = _model("produtos.GmLojasAppLogAgro").objects.all().order_by("-criado_em", "-pk")
        if busca:
            qs = qs.filter(operador_nome__icontains=busca) | qs.filter(acao__icontains=busca) | qs.filter(
                path__icontains=busca
            )
        return qs[:limite], "gm_lojas"
    if fonte_id == "etiqueta_impressao":
        qs = _model("produtos.EtiquetaImpressaoHistoricoAgro").objects.all().order_by("-criado_em", "-pk")
        return qs[:limite], "etiqueta_impressao"
    if fonte_id == "repasse_vila":
        qs = _model("produtos.RepasseVilaReservaLogAgro").objects.all().order_by("-criado_em", "-pk")
        return qs[:limite], "repasse_vila"
    if fonte_id == "import_planilha":
        qs = _model("produtos.CadastroPlanilhaImportHistoricoAgro").objects.all().order_by("-criado_em", "-pk")
        return qs[:limite], "import_planilha"
    if fonte_id == "cliente_eventos":
        qs = _model("produtos.ClienteAgroEventoAgro").objects.all().order_by("-criado_em", "-pk")
        if busca:
            qs = qs.filter(cliente_nome_snap__icontains=busca) | qs.filter(usuario__icontains=busca)
        return qs[:limite], "cliente_eventos"
    if fonte_id == "fiado_eventos":
        qs = _model("produtos.FiadoEventoAgro").objects.all().order_by("-criado_em", "-pk")
        return qs[:limite], "fiado_eventos"
    if fonte_id == "tarefa_eventos":
        qs = _model("tarefas.TarefaEventoAgro").objects.all().order_by("-criado_em", "-pk")
        if busca:
            qs = qs.filter(autor_nome__icontains=busca) | qs.filter(descricao__icontains=busca)
        return qs[:limite], "tarefa_eventos"
    return None, fonte_id


def fonte_por_id(fonte_id: str) -> FonteLogGestao | None:
    for f in catalogo_fontes_log():
        if f.id == fonte_id:
            return f
    return None
