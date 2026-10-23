"""APIs do studio Dispenser A6 (biblioteca compartilhada)."""
from __future__ import annotations

import json

from django.contrib.auth.decorators import login_required
from django.http import JsonResponse
from django.views.decorators.http import require_GET, require_http_methods

from produtos import dispenser_a6_util as util
from produtos.models import DispenserMidiaAgro


def _json_body(request):
    try:
        raw = request.body.decode("utf-8") if request.body else "{}"
        data = json.loads(raw or "{}")
        return data if isinstance(data, dict) else {}
    except (UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError):
        return None


@login_required(login_url="/entrar/")
@require_GET
def api_dispenser_biblioteca(request):
    try:
        data = util.listar_biblioteca()
        return JsonResponse({"ok": True, **data})
    except Exception as e:
        return JsonResponse({"ok": False, "erro": str(e)}, status=500)


@login_required(login_url="/entrar/")
@require_http_methods(["POST", "DELETE"])
def api_dispenser_midia(request):
    if request.method == "DELETE":
        tipo = (request.GET.get("tipo") or "").strip()
        item_id = (request.GET.get("item_id") or "").strip()
        ok, err = util.delete_midia(tipo=tipo, item_id=item_id)
        if not ok:
            return JsonResponse({"ok": False, "erro": err}, status=400)
        return JsonResponse({"ok": True})

    body = _json_body(request)
    if body is None:
        return JsonResponse({"ok": False, "erro": "JSON inválido."}, status=400)
    row, err = util.upsert_midia(
        tipo=str(body.get("tipo") or ""),
        item_id=str(body.get("item_id") or body.get("id") or ""),
        label=str(body.get("label") or ""),
        data_url=str(body.get("data_url") or body.get("dataUrl") or ""),
    )
    if not row:
        return JsonResponse({"ok": False, "erro": err}, status=400)
    return JsonResponse({"ok": True, "item": util.midia_to_dict(row)})


@login_required(login_url="/entrar/")
@require_http_methods(["POST", "DELETE"])
def api_dispenser_documento(request):
    if request.method == "DELETE":
        tipo = (request.GET.get("tipo") or "").strip()
        nome = (request.GET.get("nome") or "").strip()
        ok, err = util.delete_documento(tipo=tipo, nome=nome)
        if not ok:
            return JsonResponse({"ok": False, "erro": err}, status=400)
        return JsonResponse({"ok": True})

    body = _json_body(request)
    if body is None:
        return JsonResponse({"ok": False, "erro": "JSON inválido."}, status=400)
    payload = body.get("payload")
    if not isinstance(payload, dict):
        payload = {}
        # aceita body inteiro como payload de layout/folha (exceto meta)
        for k, v in body.items():
            if k in ("tipo", "nome", "thumb", "payload"):
                continue
            payload[k] = v
    row, err = util.upsert_documento(
        tipo=str(body.get("tipo") or ""),
        nome=str(body.get("nome") or ""),
        payload=payload,
        thumb=str(body.get("thumb") or ""),
    )
    if not row:
        return JsonResponse({"ok": False, "erro": err}, status=400)
    return JsonResponse({"ok": True, "nome": row.nome, "tipo": row.tipo})


@login_required(login_url="/entrar/")
@require_http_methods(["POST"])
def api_dispenser_migrar(request):
    body = _json_body(request)
    if body is None:
        return JsonResponse({"ok": False, "erro": "JSON inválido."}, status=400)
    try:
        result = util.migrar_lote(body)
        return JsonResponse({"ok": True, **result})
    except Exception as e:
        return JsonResponse({"ok": False, "erro": str(e)}, status=500)
