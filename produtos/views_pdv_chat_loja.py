"""APIs PDV — chat interno entre lojas (grupo único)."""
from __future__ import annotations

import json

from django.contrib.auth.decorators import login_required
from django.http import JsonResponse
from django.views.decorators.http import require_GET, require_POST

from produtos.pdv_chat_loja_util import (
    criar_mensagem,
    listar_mensagens,
    resolver_autor_chat,
    resolver_origem_chat,
    serializar_mensagem,
)


def _payload(request) -> dict | None:
    try:
        raw = (request.body or b"").decode("utf-8") or "{}"
        data = json.loads(raw)
        return data if isinstance(data, dict) else None
    except Exception:
        return None


@login_required(login_url="/admin/login/")
@require_GET
def api_pdv_chat_loja_lista(request):
    try:
        after_id = int(request.GET.get("after_id") or 0)
    except (TypeError, ValueError):
        after_id = 0
    try:
        limit = int(request.GET.get("limit") or 80)
    except (TypeError, ValueError):
        limit = 80
    msgs = listar_mensagens(after_id=after_id, limit=limit)
    origem = resolver_origem_chat(request)
    ok, label = True, resolver_autor_chat(request, None)
    last_id = msgs[-1]["id"] if msgs else after_id
    return JsonResponse(
        {
            "ok": True,
            "mensagens": msgs,
            "last_id": last_id,
            "eu": {
                "autor": label if ok else "",
                "origem": origem.get("origem_rotulo") or "",
                "deposito": origem.get("deposito") or "",
            },
        }
    )


@login_required(login_url="/admin/login/")
@require_POST
def api_pdv_chat_loja_enviar(request):
    data = _payload(request)
    if data is None:
        return JsonResponse({"ok": False, "erro": "JSON inválido."}, status=400)
    texto = data.get("texto") or ""
    device_id = str(data.get("device_id") or "").strip()
    m, err = criar_mensagem(request, texto=texto, device_id=device_id, payload=data)
    if err or m is None:
        return JsonResponse({"ok": False, "erro": err or "Não enviou."}, status=400)
    return JsonResponse({"ok": True, "mensagem": serializar_mensagem(m)})
