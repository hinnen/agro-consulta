"""APIs dos slots rápidos do PDV."""
from __future__ import annotations

import json

from django.contrib.auth.decorators import login_required
from django.http import JsonResponse
from django.views.decorators.http import require_http_methods

from produtos.pdv_rapidos_util import (
    SLOTS,
    gravar_foto,
    limpar_foto,
    listar_slots,
    resposta_foto,
    salvar_slots,
)


def _json(request) -> dict | None:
    try:
        raw = (request.body or b"").decode("utf-8") or "{}"
        data = json.loads(raw)
        return data if isinstance(data, dict) else None
    except Exception:
        return None


@login_required(login_url="/entrar/")
@require_http_methods(["GET", "POST"])
def api_pdv_rapidos(request):
    if request.method == "GET":
        return JsonResponse({"ok": True, "slots": listar_slots()})
    data = _json(request)
    if data is None:
        return JsonResponse({"ok": False, "erro": "JSON inválido."}, status=400)
    ok, err = salvar_slots(data.get("slots"), usuario=request.user)
    if not ok:
        return JsonResponse({"ok": False, "erro": err or "Falha ao salvar."}, status=400)
    return JsonResponse({"ok": True, "slots": listar_slots()})


@login_required(login_url="/entrar/")
@require_http_methods(["GET", "POST"])
def api_pdv_rapidos_foto(request, posicao: int):
    if posicao < 1 or posicao > SLOTS:
        return JsonResponse({"ok": False, "erro": "Posição inválida."}, status=404)
    if request.method == "GET":
        return resposta_foto(posicao)
    if str(request.POST.get("limpar") or "") == "1":
        ok, err = limpar_foto(posicao, usuario=request.user)
        if not ok:
            return JsonResponse({"ok": False, "erro": err}, status=400)
        return JsonResponse({"ok": True, "slots": listar_slots()})
    arquivo = request.FILES.get("foto")
    if not arquivo:
        return JsonResponse({"ok": False, "erro": "Envie uma foto."}, status=400)
    conteudo = arquivo.read()
    ok, err = gravar_foto(
        posicao,
        conteudo,
        content_type=getattr(arquivo, "content_type", "") or "",
        usuario=request.user,
    )
    if not ok:
        return JsonResponse({"ok": False, "erro": err or "Foto inválida."}, status=400)
    return JsonResponse({"ok": True, "slots": listar_slots()})
