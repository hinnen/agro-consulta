"""Tela e APIs — atendimento WhatsApp (Centro / Vila)."""
from __future__ import annotations

import json

from django.contrib.auth.decorators import login_required
from django.http import JsonResponse
from django.shortcuts import render
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_GET, require_POST

from produtos.atendimento_whatsapp_util import (
    atualizar_ponte,
    contar_nao_lidas,
    definir_loja,
    enviar_loja,
    listar_conversas,
    listar_mensagens,
    listar_saida_pendente,
    marcar_enviadas,
    marcar_lidas,
    processar_entrada,
    serializar_conversa,
    serializar_mensagem,
    serializar_ponte,
    token_ponte_ok,
    toque_heartbeat,
)


def _json_body(request) -> dict | None:
    try:
        raw = (request.body or b"").decode("utf-8") or "{}"
        data = json.loads(raw)
        return data if isinstance(data, dict) else None
    except Exception:
        return None


def _bridge_forbidden():
    return JsonResponse({"ok": False, "erro": "Ponte não autorizada."}, status=403)


@login_required(login_url="/admin/login/")
def atendimento_whatsapp_view(request):
    return render(request, "produtos/atendimento_whatsapp.html", {})


@login_required(login_url="/admin/login/")
@require_GET
def api_atendimento_whatsapp_estado(request):
    return JsonResponse(
        {
            "ok": True,
            "ponte": serializar_ponte(),
            "nao_lidas": contar_nao_lidas(),
        }
    )


@login_required(login_url="/admin/login/")
@require_GET
def api_atendimento_whatsapp_conversas(request):
    loja = (request.GET.get("loja") or "").strip().lower()
    try:
        limit = int(request.GET.get("limit") or 80)
    except (TypeError, ValueError):
        limit = 80
    return JsonResponse({"ok": True, "conversas": listar_conversas(loja=loja, limit=limit)})


@login_required(login_url="/admin/login/")
@require_GET
def api_atendimento_whatsapp_mensagens(request):
    try:
        cid = int(request.GET.get("conversa_id") or 0)
    except (TypeError, ValueError):
        cid = 0
    if cid <= 0:
        return JsonResponse({"ok": False, "erro": "Conversa inválida."}, status=400)
    try:
        after_id = int(request.GET.get("after_id") or 0)
    except (TypeError, ValueError):
        after_id = 0
    try:
        limit = int(request.GET.get("limit") or 120)
    except (TypeError, ValueError):
        limit = 120
    return JsonResponse(
        {"ok": True, "mensagens": listar_mensagens(conversa_id=cid, after_id=after_id, limit=limit)}
    )


@login_required(login_url="/admin/login/")
@require_POST
def api_atendimento_whatsapp_enviar(request):
    data = _json_body(request)
    if data is None:
        return JsonResponse({"ok": False, "erro": "JSON inválido."}, status=400)
    autor = ""
    try:
        if request.user.is_authenticated:
            autor = (request.user.get_full_name() or request.user.get_username() or "")[:120]
    except Exception:
        autor = ""
    try:
        cid = int(data.get("conversa_id") or 0)
    except (TypeError, ValueError):
        cid = 0
    m, err = enviar_loja(
        conversa_id=cid,
        texto=data.get("texto") or "",
        autor=autor,
    )
    if err or m is None:
        return JsonResponse({"ok": False, "erro": err or "Não enviou."}, status=400)
    return JsonResponse({"ok": True, "mensagem": serializar_mensagem(m)})


@login_required(login_url="/admin/login/")
@require_POST
def api_atendimento_whatsapp_marcar_lida(request):
    data = _json_body(request) or {}
    try:
        cid = int(data.get("conversa_id") or request.POST.get("conversa_id") or 0)
    except (TypeError, ValueError):
        cid = 0
    if cid <= 0:
        return JsonResponse({"ok": False, "erro": "Conversa inválida."}, status=400)
    marcar_lidas(cid)
    return JsonResponse({"ok": True})


@login_required(login_url="/admin/login/")
@require_POST
def api_atendimento_whatsapp_definir_loja(request):
    data = _json_body(request)
    if data is None:
        return JsonResponse({"ok": False, "erro": "JSON inválido."}, status=400)
    try:
        cid = int(data.get("conversa_id") or 0)
    except (TypeError, ValueError):
        cid = 0
    conv, err = definir_loja(cid, data.get("loja") or "")
    if err or conv is None:
        return JsonResponse({"ok": False, "erro": err or "Não moveu."}, status=400)
    return JsonResponse({"ok": True, "conversa": serializar_conversa(conv)})


@csrf_exempt
@require_POST
def api_atendimento_whatsapp_bridge_estado(request):
    if not token_ponte_ok(request):
        return _bridge_forbidden()
    data = _json_body(request) or {}
    atualizar_ponte(
        status=str(data.get("status") or ""),
        qr=str(data.get("qr") or ""),
        numero=str(data.get("numero") or ""),
        aviso=str(data.get("aviso") or ""),
    )
    return JsonResponse({"ok": True})


@csrf_exempt
@require_POST
def api_atendimento_whatsapp_bridge_entrada(request):
    if not token_ponte_ok(request):
        return _bridge_forbidden()
    data = _json_body(request) or {}
    _m, err = processar_entrada(
        jid=str(data.get("jid") or ""),
        texto=str(data.get("texto") or ""),
        nome=str(data.get("nome") or ""),
        wa_id=str(data.get("wa_id") or ""),
    )
    if err == "ignorado":
        return JsonResponse({"ok": True, "ignorado": True})
    if err == "duplicada":
        return JsonResponse({"ok": True, "duplicada": True})
    return JsonResponse({"ok": True})


@csrf_exempt
@require_GET
def api_atendimento_whatsapp_bridge_saida(request):
    if not token_ponte_ok(request):
        return _bridge_forbidden()
    toque_heartbeat()
    return JsonResponse({"ok": True, "saida": listar_saida_pendente()})


@csrf_exempt
@require_POST
def api_atendimento_whatsapp_bridge_saida_ok(request):
    if not token_ponte_ok(request):
        return _bridge_forbidden()
    data = _json_body(request) or {}
    ids_raw = data.get("ids") or []
    ids = []
    if isinstance(ids_raw, list):
        for x in ids_raw:
            try:
                ids.append(int(x))
            except (TypeError, ValueError):
                continue
    n = marcar_enviadas(ids, erro=str(data.get("erro") or ""))
    return JsonResponse({"ok": True, "n": n})
