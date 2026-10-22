"""APIs PDV — slots de produtos rápidos."""
from __future__ import annotations

import json

from django.contrib.auth.decorators import login_required
from django.http import HttpResponse, JsonResponse
from django.views.decorators.http import require_GET, require_http_methods

from produtos.pdv_rapido_util import foto_do_slot, limpar_slot, listar_slots, salvar_slot, slot_int


def _payload(request) -> dict | None:
    try:
        raw = (request.body or b"").decode("utf-8") or "{}"
        data = json.loads(raw)
        return data if isinstance(data, dict) else None
    except Exception:
        return None


@login_required(login_url="/entrar/")
@require_GET
def api_pdv_rapido_slots(request):
    return JsonResponse({"ok": True, "slots": listar_slots()})


@login_required(login_url="/entrar/")
@require_http_methods(["POST"])
def api_pdv_rapido_salvar(request):
    data = _payload(request)
    if data is None:
        return JsonResponse({"ok": False, "erro": "JSON inválido."}, status=400)
    n = slot_int(data.get("slot"))
    if n is None:
        return JsonResponse({"ok": False, "erro": "Slot inválido."}, status=400)
    try:
        if data.get("limpar"):
            row = limpar_slot(slot=n, usuario=request.user)
        else:
            row = salvar_slot(
                slot=n,
                produto_id=str(data.get("produto_id") or ""),
                apelido=str(data.get("apelido") or ""),
                foto_b64=data.get("foto_base64") if data.get("foto_base64") else None,
                limpar_foto=bool(data.get("limpar_foto")),
                usuario=request.user,
            )
    except ValueError as exc:
        return JsonResponse({"ok": False, "erro": str(exc)}, status=400)
    except Exception:
        return JsonResponse({"ok": False, "erro": "Falha ao salvar o slot."}, status=500)
    return JsonResponse({"ok": True, "slot": row})


@login_required(login_url="/entrar/")
@require_GET
def api_pdv_rapido_foto(request, slot: int):
    blob, mime = foto_do_slot(slot=slot)
    if not blob:
        return HttpResponse(status=404)
    resp = HttpResponse(blob, content_type=mime or "image/jpeg")
    resp["Cache-Control"] = "private, max-age=120"
    return resp
