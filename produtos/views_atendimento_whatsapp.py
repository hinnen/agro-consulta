"""Tela e APIs — atendimento WhatsApp (Centro / Vila)."""
from __future__ import annotations

import json

from django.contrib.auth.decorators import login_required
from django.http import FileResponse, Http404, JsonResponse
from django.shortcuts import render
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_GET, require_POST

from produtos.atendimento_whatsapp_bot_config import BOT_DEFAULT, carregar_bot, resetar_bot, salvar_bot
from produtos.atendimento_whatsapp_util import (
    atualizar_ponte,
    abrir_conversa_busca,
    abrir_conversa_saida,
    buscar_contatos_envio,
    contar_nao_lidas,
    definir_loja,
    enviar_loja,
    excluir_conversa,
    transferir_conversa,
    gravar_agenda_zap,
    listar_conversas,
    listar_mensagens,
    listar_pedidos_pendentes,
    listar_saida_pendente,
    marcar_enviadas,
    marcar_lidas,
    marcar_pedido,
    pedir_agenda_zap,
    pedir_codigo_pareamento,
    pedir_historico_conversa,
    processar_entrada,
    serializar_conversa,
    serializar_mensagem,
    serializar_ponte,
    token_ponte_ok,
    toque_heartbeat,
)
from produtos.models import WhatsAppMensagemAgro


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
def atendimento_whatsapp_bot_view(request):
    return render(request, "produtos/atendimento_whatsapp_bot.html", {})


@login_required(login_url="/admin/login/")
@require_GET
def api_atendimento_whatsapp_bot_get(request):
    return JsonResponse({"ok": True, "bot": carregar_bot(), "padrao": BOT_DEFAULT})


@login_required(login_url="/admin/login/")
@require_POST
def api_atendimento_whatsapp_bot_salvar(request):
    data = _json_body(request)
    if data is None:
        return JsonResponse({"ok": False, "erro": "JSON inválido."}, status=400)
    autor = ""
    try:
        if request.user.is_authenticated:
            autor = (request.user.get_full_name() or request.user.get_username() or "")[:120]
    except Exception:
        autor = ""
    if data.get("reset"):
        bot = resetar_bot(usuario=autor)
        return JsonResponse({"ok": True, "bot": bot})
    payload = data.get("bot") if isinstance(data.get("bot"), dict) else data
    bot = salvar_bot(payload, usuario=autor)
    return JsonResponse({"ok": True, "bot": bot})


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


@login_required(login_url="/admin/login/")
@require_POST
def api_atendimento_whatsapp_transferir(request):
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
    conv, err = transferir_conversa(cid, data.get("loja") or "", autor=autor)
    if err or conv is None:
        return JsonResponse({"ok": False, "erro": err or "Não passou."}, status=400)
    return JsonResponse({"ok": True, "conversa": serializar_conversa(conv)})


@login_required(login_url="/admin/login/")
@require_GET
def api_atendimento_whatsapp_contatos(request):
    q = (request.GET.get("q") or "").strip()
    return JsonResponse({"ok": True, "contatos": buscar_contatos_envio(q)})


@login_required(login_url="/admin/login/")
@require_POST
def api_atendimento_whatsapp_abrir(request):
    data = _json_body(request)
    if data is None:
        return JsonResponse({"ok": False, "erro": "JSON inválido."}, status=400)
    conv, err = abrir_conversa_busca(
        telefone=str(data.get("telefone") or data.get("jid") or ""),
        nome=str(data.get("nome") or ""),
    )
    if err or conv is None:
        return JsonResponse({"ok": False, "erro": err or "Não abriu."}, status=400)
    return JsonResponse({"ok": True, "conversa": serializar_conversa(conv)})


@login_required(login_url="/admin/login/")
@require_POST
def api_atendimento_whatsapp_novo(request):
    data = _json_body(request)
    if data is None:
        return JsonResponse({"ok": False, "erro": "JSON inválido."}, status=400)
    autor = ""
    try:
        if request.user.is_authenticated:
            autor = (request.user.get_full_name() or request.user.get_username() or "")[:120]
    except Exception:
        autor = ""
    tel = str(data.get("telefone") or data.get("jid") or "")
    m, err = abrir_conversa_saida(
        telefone=tel,
        loja=str(data.get("loja") or ""),
        texto=str(data.get("texto") or ""),
        nome=str(data.get("nome") or ""),
        autor=autor,
    )
    if err or m is None:
        return JsonResponse({"ok": False, "erro": err or "Não enviou."}, status=400)
    return JsonResponse(
        {
            "ok": True,
            "mensagem": serializar_mensagem(m),
            "conversa": serializar_conversa(m.conversa),
        }
    )


@login_required(login_url="/admin/login/")
@require_POST
def api_atendimento_whatsapp_agenda_zap(request):
    _p, err = pedir_agenda_zap()
    if err:
        return JsonResponse({"ok": False, "erro": err}, status=400)
    return JsonResponse({"ok": True})


@login_required(login_url="/admin/login/")
@require_POST
def api_atendimento_whatsapp_historico(request):
    data = _json_body(request) or {}
    try:
        cid = int(data.get("conversa_id") or 0)
    except (TypeError, ValueError):
        cid = 0
    _p, err = pedir_historico_conversa(cid)
    if err:
        return JsonResponse({"ok": False, "erro": err}, status=400)
    return JsonResponse({"ok": True})


@login_required(login_url="/admin/login/")
@require_POST
def api_atendimento_whatsapp_excluir(request):
    data = _json_body(request) or {}
    try:
        cid = int(data.get("conversa_id") or 0)
    except (TypeError, ValueError):
        cid = 0
    ok, err = excluir_conversa(cid)
    if not ok:
        return JsonResponse({"ok": False, "erro": err or "Não apagou."}, status=400)
    return JsonResponse({"ok": True})


@login_required(login_url="/admin/login/")
@require_POST
def api_atendimento_whatsapp_pairing(request):
    data = _json_body(request) or {}
    _p, err = pedir_codigo_pareamento(str(data.get("telefone") or ""))
    if err:
        return JsonResponse({"ok": False, "erro": err}, status=400)
    return JsonResponse({"ok": True})


@login_required(login_url="/admin/login/")
@require_GET
def api_atendimento_whatsapp_midia(request, pk: int):
    try:
        m = WhatsAppMensagemAgro.objects.get(pk=int(pk))
    except (WhatsAppMensagemAgro.DoesNotExist, TypeError, ValueError) as exc:
        raise Http404("Mídia não encontrada.") from exc
    if not m.arquivo:
        raise Http404("Mídia não encontrada.")
    ctype = "application/octet-stream"
    name = (m.arquivo.name or "").lower()
    if m.tipo_midia in ("image", "sticker") or name.endswith((".jpg", ".jpeg", ".png", ".webp")):
        ctype = "image/jpeg"
        if name.endswith(".png"):
            ctype = "image/png"
        elif name.endswith(".webp"):
            ctype = "image/webp"
    elif m.tipo_midia == "audio" or name.endswith((".ogg", ".opus", ".mp3", ".m4a")):
        ctype = "audio/ogg"
        if name.endswith(".mp3"):
            ctype = "audio/mpeg"
    return FileResponse(m.arquivo.open("rb"), content_type=ctype)


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
        pairing_code=str(data.get("pairing_code") or ""),
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
        historico=bool(data.get("historico")),
        de_mim=bool(data.get("de_mim")),
        ts=data.get("ts"),
        tipo_midia=str(data.get("tipo_midia") or ""),
        midia_b64=str(data.get("midia_b64") or ""),
        mime=str(data.get("mime") or ""),
        nome_arquivo=str(data.get("nome_arquivo") or ""),
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
    return JsonResponse(
        {"ok": True, "saida": listar_saida_pendente(), "pedidos": listar_pedidos_pendentes()}
    )


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


@csrf_exempt
@require_POST
def api_atendimento_whatsapp_bridge_contatos(request):
    if not token_ponte_ok(request):
        return _bridge_forbidden()
    data = _json_body(request) or {}
    try:
        pid = int(data.get("pedido_id") or 0)
    except (TypeError, ValueError):
        pid = 0
    n = gravar_agenda_zap(data.get("itens") or [], pedido_id=pid)
    return JsonResponse({"ok": True, "n": n})


@csrf_exempt
@require_POST
def api_atendimento_whatsapp_bridge_pedido_ok(request):
    if not token_ponte_ok(request):
        return _bridge_forbidden()
    data = _json_body(request) or {}
    try:
        pid = int(data.get("pedido_id") or 0)
    except (TypeError, ValueError):
        pid = 0
    erro = str(data.get("erro") or "")
    marcar_pedido(pid, ok=not bool(erro), erro=erro)
    return JsonResponse({"ok": True})
