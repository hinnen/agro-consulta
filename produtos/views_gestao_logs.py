"""Painel central de logs — Gestão → LOG."""

from __future__ import annotations

from django.contrib.auth.decorators import login_required
from django.shortcuts import render
from django.urls import reverse
from django.utils import timezone
from django.views.decorators.http import require_GET

from produtos.gestao_logs_catalog import admin_changelist_url, catalogo_fontes_log
from produtos.gestao_logs_util import contagem_fonte, fonte_por_id, queryset_fonte


def _fmt_dt(dt) -> str:
    if not dt:
        return "—"
    if timezone.is_naive(dt):
        dt = timezone.make_aware(dt, timezone.get_current_timezone())
    return timezone.localtime(dt).strftime("%d/%m/%Y %H:%M")


def _linhas_de_queryset(qs, tipo: str) -> list[dict]:
    rows: list[dict] = []
    for obj in qs:
        if tipo == "gm_lojas":
            det = obj.detalhe_json if isinstance(getattr(obj, "detalhe_json", None), dict) else {}
            extra = det.get("query") or det.get("pacote_id") or ""
            rows.append(
                {
                    "quando": _fmt_dt(obj.criado_em),
                    "quem": obj.operador_nome or "—",
                    "acao": obj.acao,
                    "detalhe": f"{obj.metodo} {obj.path}".strip(),
                    "status": str(obj.status_http or ""),
                    "extra": str(extra)[:120],
                }
            )
        elif tipo == "etiqueta_impressao":
            rows.append(
                {
                    "quando": _fmt_dt(obj.criado_em),
                    "quem": obj.usuario or "—",
                    "acao": obj.origem or "impressão",
                    "detalhe": f"{obj.total_etiquetas} etiquetas · {obj.preset_nome or obj.preset_id}",
                    "status": "",
                    "extra": (obj.resumo_nomes or "")[:120],
                }
            )
        elif tipo == "repasse_vila":
            rows.append(
                {
                    "quando": _fmt_dt(obj.criado_em),
                    "quem": obj.operador or "—",
                    "acao": obj.tipo,
                    "detalhe": (obj.mensagem or "")[:200],
                    "status": "",
                    "extra": f"{obj.valor_antes} → {obj.valor_depois}",
                }
            )
        elif tipo == "import_planilha":
            u = getattr(getattr(obj, "usuario", None), "username", "") or "—"
            rows.append(
                {
                    "quando": _fmt_dt(obj.criado_em),
                    "quem": u,
                    "acao": f"{obj.tipo} · {obj.status}",
                    "detalhe": obj.nome_arquivo or "",
                    "status": "",
                    "extra": f"{obj.n_produtos} produtos",
                }
            )
        elif tipo == "cliente_eventos":
            rows.append(
                {
                    "quando": _fmt_dt(obj.criado_em),
                    "quem": obj.usuario or obj.pin_operador or "—",
                    "acao": obj.tipo,
                    "detalhe": obj.cliente_nome_snap or "",
                    "status": "",
                    "extra": "",
                }
            )
        elif tipo == "fiado_eventos":
            rows.append(
                {
                    "quando": _fmt_dt(obj.criado_em),
                    "quem": obj.usuario or "—",
                    "acao": obj.tipo,
                    "detalhe": str(obj.payload_json or "")[:200],
                    "status": "",
                    "extra": "",
                }
            )
        elif tipo == "tarefa_eventos":
            rows.append(
                {
                    "quando": _fmt_dt(obj.criado_em),
                    "quem": obj.autor_nome or "—",
                    "acao": obj.tipo,
                    "detalhe": (obj.detalhe or "")[:200],
                    "status": "",
                    "extra": f"Tarefa #{obj.tarefa_id}",
                }
            )
    return rows


@login_required(login_url="/entrar/")
@require_GET
def gestao_logs_painel(request):
    fonte_id = str(request.GET.get("fonte") or "gm_lojas").strip()
    fonte = fonte_por_id(fonte_id)
    if fonte is None:
        fonte_id = "gm_lojas"
        fonte = fonte_por_id("gm_lojas")
    busca = str(request.GET.get("q") or "").strip()
    catalogo = []
    for item in catalogo_fontes_log():
        admin_url = admin_changelist_url(item.admin_model) if item.admin_model else None
        pagina_url = reverse(item.pagina_url_name) if item.pagina_url_name else None
        catalogo.append(
            {
                "id": item.id,
                "titulo": item.titulo,
                "descricao": item.descricao,
                "icone": item.icone,
                "total": contagem_fonte(item),
                "admin_url": admin_url if request.user.is_staff else None,
                "pagina_url": pagina_url,
                "ativo": item.id == fonte_id,
            }
        )

    linhas: list[dict] = []
    qs, tipo = queryset_fonte(fonte_id, busca=busca)
    if qs is not None:
        linhas = _linhas_de_queryset(qs, tipo)

    admin_ativo = None
    if fonte and fonte.admin_model and request.user.is_staff:
        admin_ativo = admin_changelist_url(fonte.admin_model)

    return render(
        request,
        "produtos/gestao_logs.html",
        {
            "fonte_id": fonte_id,
            "fonte_titulo": fonte.titulo if fonte else "Logs",
            "catalogo": catalogo,
            "linhas": linhas,
            "busca": busca,
            "admin_url": admin_ativo,
        },
    )
