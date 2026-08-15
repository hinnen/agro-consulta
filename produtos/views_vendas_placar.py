"""Tela simples: valor de vendas Centro × Vila + soma."""
from __future__ import annotations

from datetime import date
from decimal import Decimal

from django.http import JsonResponse
from django.shortcuts import render
from django.utils import timezone
from django.views.decorators.cache import never_cache
from django.views.decorators.http import require_GET

from produtos.vendas_placar_util import resolver_periodo
from produtos.views import (
    _dashboard_float,
    _dashboard_login_required,
    _dashboard_mongo_vendas_serie,
    _format_moeda_br,
)


def _parse_data(raw) -> date | None:
    s = str(raw or "").strip()[:10]
    if not s:
        return None
    try:
        return date.fromisoformat(s)
    except ValueError:
        return None


def _totais_lojas(data_ini: date, data_fim: date) -> tuple[Decimal, Decimal, Decimal]:
    ser = _dashboard_mongo_vendas_serie(data_ini, data_fim, deposito=None)
    vpl = ser.get("vendas_por_loja") if isinstance(ser, dict) else None
    centro = Decimal("0.00")
    vila = Decimal("0.00")
    if isinstance(vpl, list):
        for row in vpl:
            if not isinstance(row, dict):
                continue
            nome = str(row.get("loja") or "").strip().lower()
            val = Decimal(str(_dashboard_float(row.get("total")))).quantize(Decimal("0.01"))
            if nome.startswith("vila"):
                vila = val
            elif nome.startswith("centro"):
                centro = val
    soma = (centro + vila).quantize(Decimal("0.01"))
    return centro, vila, soma


def montar_contexto_placar(request) -> dict:
    hoje = timezone.localdate()
    recorte = resolver_periodo(
        request.GET.get("periodo"),
        _parse_data(request.GET.get("data")),
        hoje,
    )
    centro, vila, soma = _totais_lojas(recorte["data_ini"], recorte["data_fim"])
    return {
        "periodo": recorte["periodo"],
        "ancora": recorte["ancora"].isoformat(),
        "data_ini": recorte["data_ini"],
        "data_fim": recorte["data_fim"],
        "periodo_label": recorte["label"],
        "prev_data": recorte["prev"].isoformat(),
        "next_data": recorte["next"].isoformat(),
        "pode_avancar": recorte["pode_avancar"],
        "e_hoje": recorte["e_hoje"],
        "hoje": hoje.isoformat(),
        "centro": centro,
        "vila": vila,
        "soma": soma,
        "centro_fmt": _format_moeda_br(centro),
        "vila_fmt": _format_moeda_br(vila),
        "soma_fmt": _format_moeda_br(soma),
    }


def json_placar(ctx: dict) -> dict:
    return {
        "ok": True,
        "periodo": ctx["periodo"],
        "data": ctx["ancora"],
        "data_ini": ctx["data_ini"].isoformat(),
        "data_fim": ctx["data_fim"].isoformat(),
        "label": ctx["periodo_label"],
        "centro": str(ctx["centro"]),
        "vila": str(ctx["vila"]),
        "soma": str(ctx["soma"]),
        "centro_fmt": ctx["centro_fmt"],
        "vila_fmt": ctx["vila_fmt"],
        "soma_fmt": ctx["soma_fmt"],
        "e_hoje": ctx["e_hoje"],
    }


@never_cache
@_dashboard_login_required
@require_GET
def vendas_lojas_placar_view(request):
    ctx = montar_contexto_placar(request)
    if (request.GET.get("fmt") or "").strip().lower() == "json":
        return JsonResponse(json_placar(ctx))
    return render(request, "produtos/vendas_lojas_placar.html", ctx)
