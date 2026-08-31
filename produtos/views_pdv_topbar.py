"""APIs PDV — cliques e layout da topbar (quente/frio)."""
from __future__ import annotations

import json

from django.contrib.auth.decorators import login_required
from django.http import JsonResponse
from django.views.decorators.http import require_GET, require_http_methods, require_POST

from produtos.pdv_topbar_clique_util import registrar_clique, resumo_cliques
from produtos.pdv_topbar_layout_util import payload_api, salvar_layout


def _payload(request) -> dict | None:
    try:
        raw = (request.body or b"").decode("utf-8") or "{}"
        data = json.loads(raw)
        return data if isinstance(data, dict) else None
    except Exception:
        return None


def _deposito_request(request, data: dict | None = None) -> str:
    if data:
        d = str(data.get("deposito") or "").strip()
        if d:
            return d
    return str(
        request.session.get("pdv_deposito")
        or request.session.get("agro_deposito")
        or ""
    ).strip()


@login_required(login_url="/admin/login/")
@require_POST
def api_pdv_topbar_clique(request):
    data = _payload(request)
    if data is None:
        return JsonResponse({"ok": False, "erro": "JSON inválido."}, status=400)
    botao = data.get("botao") or data.get("key") or ""
    ok, err = registrar_clique(botao=str(botao), deposito=_deposito_request(request, data))
    if not ok:
        return JsonResponse({"ok": False, "erro": err or "Falha."}, status=400)
    return JsonResponse({"ok": True})


@login_required(login_url="/admin/login/")
@require_GET
def api_pdv_topbar_cliques_resumo(request):
    try:
        dias = int(request.GET.get("dias") or 14)
    except (TypeError, ValueError):
        dias = 14
    return JsonResponse({"ok": True, "dias": dias, "ranking": resumo_cliques(dias=dias)})


@login_required(login_url="/admin/login/")
@require_http_methods(["GET", "POST"])
def api_pdv_topbar_layout(request):
    if request.method == "GET":
        return JsonResponse(payload_api())
    data = _payload(request)
    if data is None:
        return JsonResponse({"ok": False, "erro": "JSON inválido."}, status=400)
    layout = salvar_layout(
        quente=data.get("quente"),
        frio=data.get("frio"),
        usuario=request.user,
    )
    body = payload_api()
    body["quente"] = layout["quente"]
    body["frio"] = layout["frio"]
    return JsonResponse(body)
