"""Tela e APIs — atendimento WhatsApp (Centro / Vila)."""
from __future__ import annotations

import json

from django.contrib.auth.decorators import login_required
from django.http import FileResponse, Http404, HttpResponse, JsonResponse
from django.shortcuts import render
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_GET, require_POST

from produtos.atendimento_whatsapp_bot_config import BOT_DEFAULT, avisos_bot, carregar_bot, resetar_bot, salvar_bot
from produtos.atendimento_whatsapp_util import (
    atualizar_ponte,
    abrir_conversa_busca,
    abrir_conversa_saida,
    buscar_contatos_envio,
    contar_nao_lidas,
    definir_loja,
    enviar_loja,
    excluir_conversa,
    excluir_todas_conversas,
    ficha_contato_conversa,
    transferir_conversa,
    gravar_agenda_zap,
    importar_agenda_vcard,
    listar_conversas,
    listar_mensagens,
    listar_pedidos_pendentes,
    listar_saida_pendente,
    listar_fotos_pendentes,
    marcar_enviadas,
    marcar_lidas,
    concluir_atendimento,
    marcar_pedido,
    pedir_agenda_zap,
    pedir_apagar_mensagem,
    pedir_codigo_pareamento,
    pedir_historico_conversa,
    pedir_trocar_whatsapp,
    processar_entrada,
    processar_status,
    gravar_foto_perfil,
    aplicar_mapa_lid,
    listar_status,
    serializar_conversa,
    serializar_mensagem,
    serializar_ponte,
    token_ponte_ok,
    toque_heartbeat,
)
from produtos.models import WhatsAppConversaAgro, WhatsAppMensagemAgro, WhatsAppStatusAgro


def _json_body(request) -> dict | None:
    try:
        raw = (request.body or b"").decode("utf-8") or "{}"
        data = json.loads(raw)
        return data if isinstance(data, dict) else None
    except Exception:
        return None


def _bridge_forbidden():
    return JsonResponse({"ok": False, "erro": "Ponte não autorizada."}, status=403)


def _autor_wa(request, data: dict | None = None) -> str:
    """
    Assinatura na bolha verde: preferir PIN/operador do PDV na sessão.
    Sem PIN fresco → cai no login do Chrome (admin) só como reserva.
    """
    from produtos.caixa_util import operador_label_request, rotulo_usuario_registro_venda

    rot = (rotulo_usuario_registro_venda(request, data) or "").strip()
    if not rot:
        rot = (operador_label_request(request) or "").strip()
    if rot:
        return rot[:120]
    try:
        if request.user.is_authenticated:
            return (request.user.get_full_name() or request.user.get_username() or "")[:120]
    except Exception:
        pass
    return ""


@login_required(login_url="/entrar/")
def atendimento_whatsapp_view(request):
    return render(request, "produtos/atendimento_whatsapp.html", {})


def atendimento_whatsapp_celular_manifest(request):
    """Manifest PWA do Zap loja (público — Chrome baixa sem login)."""
    icon_192 = request.build_absolute_uri("/static/produtos/pwa/zap-loja-192.png")
    icon_512 = request.build_absolute_uri("/static/produtos/pwa/zap-loja-512.png")
    payload = {
        "id": "/atendimento-whatsapp/celular/",
        "name": "Zap loja",
        "short_name": "Zap loja",
        "description": "SisVale WhatsApp — atendimento Centro e Vila",
        "start_url": "/atendimento-whatsapp/celular/",
        "scope": "/atendimento-whatsapp/celular/",
        "display": "standalone",
        "display_override": ["standalone", "minimal-ui"],
        "orientation": "portrait",
        "lang": "pt-BR",
        "dir": "ltr",
        "background_color": "#075E54",
        "theme_color": "#128C7E",
        "icons": [
            {"src": icon_192, "sizes": "192x192", "type": "image/png", "purpose": "any"},
            {"src": icon_512, "sizes": "512x512", "type": "image/png", "purpose": "any"},
            {"src": icon_512, "sizes": "512x512", "type": "image/png", "purpose": "maskable"},
        ],
    }
    resp = JsonResponse(payload)
    resp["Content-Type"] = "application/manifest+json"
    return resp


def atendimento_whatsapp_celular_sw(request):
    """SW mínimo — instala no celular. Rede nas APIs; não guarda conversa."""
    js = (
        "self.addEventListener('install',function(e){self.skipWaiting();});\n"
        "self.addEventListener('activate',function(e){e.waitUntil(self.clients.claim());});\n"
        "self.addEventListener('fetch',function(e){"
        "var u=String(e.request.url||'');"
        "if(u.indexOf('/api/')!==-1){e.respondWith(fetch(e.request));return;}"
        "e.respondWith(fetch(e.request));"
        "});\n"
    )
    resp = HttpResponse(js, content_type="text/javascript; charset=utf-8")
    resp["Service-Worker-Allowed"] = "/atendimento-whatsapp/celular/"
    resp["Cache-Control"] = "no-cache"
    return resp


@login_required(login_url="/entrar/")
def atendimento_whatsapp_celular_view(request):
    return render(request, "produtos/atendimento_whatsapp_celular.html", {})


@login_required(login_url="/entrar/")
def atendimento_whatsapp_bot_view(request):
    return render(request, "produtos/atendimento_whatsapp_bot.html", {})


@login_required(login_url="/entrar/")
@require_GET
def api_atendimento_whatsapp_bot_get(request):
    bot = carregar_bot()
    return JsonResponse({"ok": True, "bot": bot, "padrao": BOT_DEFAULT, "avisos": avisos_bot(bot)})


@login_required(login_url="/entrar/")
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
        return JsonResponse({"ok": True, "bot": bot, "avisos": avisos_bot(bot)})
    payload = data.get("bot") if isinstance(data.get("bot"), dict) else data
    bot = salvar_bot(payload, usuario=autor)
    return JsonResponse({"ok": True, "bot": bot, "avisos": avisos_bot(bot)})


@login_required(login_url="/entrar/")
@require_GET
def api_atendimento_whatsapp_estado(request):
    from produtos.atendimento_whatsapp_bot_config import carregar_bot, cfg_flag
    from produtos.atendimento_whatsapp_recursos import catalogo_para_api, flags_recursos

    cfg = carregar_bot()
    return JsonResponse(
        {
            "ok": True,
            "ponte": serializar_ponte(),
            "nao_lidas": contar_nao_lidas(),
            "bot": {
                "separar_lojas": cfg_flag(cfg, "separar_lojas"),
                "xfer_avisar_cliente": cfg_flag(cfg, "xfer_avisar_cliente", default=True),
            },
            "recursos": flags_recursos(cfg),
            "recursos_catalogo": catalogo_para_api(cfg),
            "respostas_prontas": str(cfg.get("respostas_prontas") or ""),
        }
    )


@login_required(login_url="/entrar/")
@require_GET
def api_atendimento_whatsapp_conversas(request):
    loja = (request.GET.get("loja") or "").strip().lower()
    try:
        limit = int(request.GET.get("limit") or 80)
    except (TypeError, ValueError):
        limit = 80
    return JsonResponse({"ok": True, "conversas": listar_conversas(loja=loja, limit=limit)})


@login_required(login_url="/entrar/")
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


@login_required(login_url="/entrar/")
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
        tipo_midia=data.get("tipo_midia") or "",
        midia_b64=data.get("midia_b64") or "",
        mime=data.get("mime") or "",
        nome_arquivo=data.get("nome_arquivo") or "",
    )
    if err or m is None:
        return JsonResponse({"ok": False, "erro": err or "Não enviou."}, status=400)
    return JsonResponse({"ok": True, "mensagem": serializar_mensagem(m)})


@login_required(login_url="/entrar/")
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


@login_required(login_url="/entrar/")
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


@login_required(login_url="/entrar/")
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
    conv, err = transferir_conversa(
        cid, data.get("loja") or "", autor=autor, nota=str(data.get("nota") or "")
    )
    if err or conv is None:
        return JsonResponse({"ok": False, "erro": err or "Não passou."}, status=400)
    return JsonResponse({"ok": True, "conversa": serializar_conversa(conv)})


@login_required(login_url="/entrar/")
@require_GET
def api_atendimento_whatsapp_contatos(request):
    q = (request.GET.get("q") or "").strip()
    return JsonResponse({"ok": True, "contatos": buscar_contatos_envio(q)})


@login_required(login_url="/entrar/")
@require_GET
def api_atendimento_whatsapp_ficha(request):
    try:
        cid = int(request.GET.get("conversa_id") or 0)
    except (TypeError, ValueError):
        cid = 0
    ficha, err = ficha_contato_conversa(cid)
    if err or ficha is None:
        return JsonResponse({"ok": False, "erro": err or "Não achou."}, status=404)
    return JsonResponse({"ok": True, "ficha": ficha})


@login_required(login_url="/entrar/")
@require_POST
def api_atendimento_whatsapp_agenda_vcf(request):
    f = request.FILES.get("arquivo") or request.FILES.get("file") or request.FILES.get("vcf")
    if f is None:
        return JsonResponse({"ok": False, "erro": "Escolha o arquivo .vcf da agenda."}, status=400)
    if f.size and f.size > 8 * 1024 * 1024:
        return JsonResponse({"ok": False, "erro": "Arquivo grande demais (máx. 8 MB)."}, status=400)
    nome = (getattr(f, "name", "") or "").lower()
    if nome and not (nome.endswith(".vcf") or nome.endswith(".vcard")):
        return JsonResponse({"ok": False, "erro": "Use um arquivo .vcf (contatos do celular)."}, status=400)
    raw = f.read()
    texto = ""
    for enc in ("utf-8", "utf-8-sig", "latin-1"):
        try:
            texto = raw.decode(enc)
            break
        except UnicodeDecodeError:
            continue
    if not texto.strip():
        return JsonResponse({"ok": False, "erro": "Arquivo vazio."}, status=400)
    if "BEGIN:VCARD" not in texto.upper():
        return JsonResponse({"ok": False, "erro": "Isso não parece um arquivo de contatos (.vcf)."}, status=400)
    out = importar_agenda_vcard(texto)
    return JsonResponse(
        {
            "ok": True,
            "lidos": int(out.get("lidos") or 0),
            "gravados": int(out.get("gravados") or 0),
            "aviso": "Pronto. Busque pelo nome na caixa de pesquisa.",
        }
    )


@login_required(login_url="/entrar/")
@require_POST
def api_atendimento_whatsapp_abrir(request):
    data = _json_body(request)
    if data is None:
        return JsonResponse({"ok": False, "erro": "JSON inválido."}, status=400)
    conv, err = abrir_conversa_busca(
        telefone=str(data.get("telefone") or ""),
        nome=str(data.get("nome") or ""),
        jid=str(data.get("jid") or ""),
    )
    if err or conv is None:
        return JsonResponse({"ok": False, "erro": err or "Não abriu."}, status=400)
    return JsonResponse({"ok": True, "conversa": serializar_conversa(conv)})


@login_required(login_url="/entrar/")
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


@login_required(login_url="/entrar/")
@require_POST
def api_atendimento_whatsapp_agenda_zap(request):
    _p, err = pedir_agenda_zap()
    if err:
        return JsonResponse({"ok": False, "erro": err}, status=400)
    return JsonResponse({"ok": True})


@login_required(login_url="/entrar/")
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


@login_required(login_url="/entrar/")
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


@login_required(login_url="/entrar/")
@require_POST
def api_atendimento_whatsapp_excluir_todas(request):
    n = excluir_todas_conversas()
    return JsonResponse({"ok": True, "apagadas": n})


@login_required(login_url="/entrar/")
@require_POST
def api_atendimento_whatsapp_apagar_mensagem(request):
    data = _json_body(request) or {}
    try:
        mid = int(data.get("mensagem_id") or 0)
    except (TypeError, ValueError):
        mid = 0
    ok, err = pedir_apagar_mensagem(mid)
    if not ok:
        return JsonResponse({"ok": False, "erro": err or "Não apagou."}, status=400)
    return JsonResponse({"ok": True})


@login_required(login_url="/entrar/")
@require_POST
def api_atendimento_whatsapp_concluir(request):
    data = _json_body(request) or {}
    try:
        cid = int(data.get("conversa_id") or 0)
    except (TypeError, ValueError):
        cid = 0
    ok, err = concluir_atendimento(cid)
    if not ok:
        return JsonResponse({"ok": False, "erro": err or "Não concluiu."}, status=400)
    return JsonResponse({"ok": True})


@login_required(login_url="/entrar/")
@require_POST
def api_atendimento_whatsapp_pairing(request):
    data = _json_body(request) or {}
    _p, err = pedir_codigo_pareamento(str(data.get("telefone") or ""))
    if err:
        return JsonResponse({"ok": False, "erro": err}, status=400)
    return JsonResponse({"ok": True})


@login_required(login_url="/entrar/")
@require_POST
def api_atendimento_whatsapp_trocar(request):
    _p, err = pedir_trocar_whatsapp()
    if err:
        return JsonResponse({"ok": False, "erro": err}, status=400)
    return JsonResponse({"ok": True})


def _arquivo_midia_response(m) -> FileResponse:
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
    elif m.tipo_midia == "video" or name.endswith((".mp4", ".webm", ".mov")):
        ctype = "video/mp4"
        if name.endswith(".webm"):
            ctype = "video/webm"
    elif m.tipo_midia == "audio" or name.endswith((".ogg", ".opus", ".mp3", ".m4a", ".webm")):
        ctype = "audio/ogg"
        if name.endswith(".mp3"):
            ctype = "audio/mpeg"
        elif name.endswith(".webm"):
            ctype = "audio/webm"
    return FileResponse(m.arquivo.open("rb"), content_type=ctype)


@login_required(login_url="/entrar/")
@require_GET
def api_atendimento_whatsapp_midia(request, pk: int):
    try:
        m = WhatsAppMensagemAgro.objects.get(pk=int(pk))
    except (WhatsAppMensagemAgro.DoesNotExist, TypeError, ValueError) as exc:
        raise Http404("Mídia não encontrada.") from exc
    return _arquivo_midia_response(m)


@login_required(login_url="/entrar/")
@require_GET
def api_atendimento_whatsapp_foto(request, pk: int):
    try:
        c = WhatsAppConversaAgro.objects.get(pk=int(pk))
    except (WhatsAppConversaAgro.DoesNotExist, TypeError, ValueError) as exc:
        raise Http404("Foto não encontrada.") from exc
    if not c.foto_perfil:
        raise Http404("Foto não encontrada.")
    name = (c.foto_perfil.name or "").lower()
    ctype = "image/jpeg"
    if name.endswith(".png"):
        ctype = "image/png"
    elif name.endswith(".webp"):
        ctype = "image/webp"
    return FileResponse(c.foto_perfil.open("rb"), content_type=ctype)


@csrf_exempt
@require_POST
def api_atendimento_whatsapp_bridge_foto(request):
    if not token_ponte_ok(request):
        return _bridge_forbidden()
    data = _json_body(request) or {}
    ok, err = gravar_foto_perfil(
        jid=str(data.get("jid") or ""),
        telefone=str(data.get("telefone") or ""),
        jid_lid=str(data.get("jid_lid") or data.get("lid") or ""),
        midia_b64=str(data.get("midia_b64") or ""),
        mime=str(data.get("mime") or ""),
        forcar=bool(data.get("forcar")),
    )
    if not ok and err not in ("ignorado",):
        return JsonResponse({"ok": False, "erro": err}, status=400)
    return JsonResponse({"ok": True, "info": err or ""})


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
    lids = data.get("lids")
    if isinstance(lids, dict) and lids:
        aplicar_mapa_lid(lids)
    return JsonResponse({"ok": True})


@csrf_exempt
@require_POST
def api_atendimento_whatsapp_bridge_lids(request):
    if not token_ponte_ok(request):
        return _bridge_forbidden()
    data = _json_body(request) or {}
    n = aplicar_mapa_lid(data.get("lids") or {})
    return JsonResponse({"ok": True, "n": n})


@login_required(login_url="/entrar/")
@require_GET
def api_atendimento_whatsapp_status(request):
    return JsonResponse({"ok": True, "autores": listar_status()})


@login_required(login_url="/entrar/")
@require_GET
def api_atendimento_whatsapp_status_midia(request, pk: int):
    try:
        s = WhatsAppStatusAgro.objects.get(pk=int(pk))
    except (WhatsAppStatusAgro.DoesNotExist, TypeError, ValueError) as exc:
        raise Http404("Mídia não encontrada.") from exc
    return _arquivo_midia_response(s)


@csrf_exempt
@require_POST
def api_atendimento_whatsapp_bridge_status(request):
    if not token_ponte_ok(request):
        return _bridge_forbidden()
    data = _json_body(request) or {}
    _s, err = processar_status(
        jid=str(data.get("jid") or ""),
        texto=str(data.get("texto") or ""),
        nome=str(data.get("nome") or ""),
        wa_id=str(data.get("wa_id") or ""),
        ts=data.get("ts"),
        tipo_midia=str(data.get("tipo_midia") or ""),
        midia_b64=str(data.get("midia_b64") or ""),
        mime=str(data.get("mime") or ""),
        nome_arquivo=str(data.get("nome_arquivo") or ""),
        telefone=str(data.get("telefone") or ""),
        jid_lid=str(data.get("jid_lid") or data.get("lid") or ""),
    )
    if err in ("ignorado", "duplicada"):
        return JsonResponse({"ok": True, err: True})
    if err:
        return JsonResponse({"ok": False, "erro": err}, status=400)
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
        telefone=str(data.get("telefone") or ""),
        jid_lid=str(data.get("jid_lid") or data.get("lid") or ""),
    )
    lids = data.get("lids")
    if isinstance(lids, dict) and lids:
        aplicar_mapa_lid(lids)
    if err == "ignorado":
        return JsonResponse({"ok": True, "ignorado": True})
    if err == "duplicada":
        return JsonResponse({"ok": True, "duplicada": True})
    return JsonResponse({"ok": True})


@csrf_exempt
@require_GET
def api_atendimento_whatsapp_bridge_midia(request, pk: int):
    if not token_ponte_ok(request):
        return _bridge_forbidden()
    try:
        m = WhatsAppMensagemAgro.objects.get(pk=int(pk))
    except (WhatsAppMensagemAgro.DoesNotExist, TypeError, ValueError) as exc:
        raise Http404("Mídia não encontrada.") from exc
    return _arquivo_midia_response(m)


@csrf_exempt
@require_GET
def api_atendimento_whatsapp_bridge_saida(request):
    if not token_ponte_ok(request):
        return _bridge_forbidden()
    toque_heartbeat()
    return JsonResponse(
        {"ok": True, "saida": listar_saida_pendente(), "pedidos": listar_pedidos_pendentes(), "fotos": listar_fotos_pendentes()}
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
    n = marcar_enviadas(ids, erro=str(data.get("erro") or ""), wa_id=str(data.get("wa_id") or ""))
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


@login_required(login_url="/entrar/")
@require_GET
def api_atendimento_whatsapp_recursos(request):
    """Flags públicas pro PDV/chat (tudo off até Renan ligar no Bot)."""
    from produtos.atendimento_whatsapp_bot_config import carregar_bot
    from produtos.atendimento_whatsapp_recursos import catalogo_para_api, flags_recursos, relatorio_dia, recurso_on

    cfg = carregar_bot()
    out = {
        "ok": True,
        "recursos": flags_recursos(cfg),
        "catalogo": catalogo_para_api(cfg),
        "respostas_prontas": str(cfg.get("respostas_prontas") or ""),
    }
    if recurso_on(cfg, "feat_relatorio_dia"):
        out["relatorio_dia"] = relatorio_dia()
    return JsonResponse(out)


@login_required(login_url="/entrar/")
@require_POST
def api_atendimento_whatsapp_recurso_acao(request):
    """Ações dos recursos — recusam se a flag estiver off."""
    from produtos.atendimento_whatsapp_recursos import (
        acao_comprovante_venda,
        acao_entrega_status,
        acao_lembrete_fiado,
        acao_lista_espera_avisar,
        acao_marcar_espera,
        acao_set_vip,
        recurso_on,
    )
    from produtos.atendimento_whatsapp_bot_config import carregar_bot

    data = _json_body(request) or {}
    acao = str(data.get("acao") or "").strip().lower()
    autor = _autor_wa(request, data)
    cfg = carregar_bot()
    try:
        cid = int(data.get("conversa_id") or 0)
    except (TypeError, ValueError):
        cid = 0

    if acao == "comprovante":
        ok, err = acao_comprovante_venda(
            conversa_id=cid,
            venda=str(data.get("venda") or ""),
            total=str(data.get("total") or ""),
            nome=str(data.get("nome") or ""),
            autor=autor,
        )
    elif acao == "entrega":
        ok, err = acao_entrega_status(
            conversa_id=cid,
            status=str(data.get("status") or ""),
            nome=str(data.get("nome") or ""),
            autor=autor,
        )
    elif acao == "lembrete_fiado":
        ok, err = acao_lembrete_fiado(conversa_id=cid, autor=autor)
    elif acao == "espera_marcar":
        ok, err = acao_marcar_espera(conversa_id=cid, produto=str(data.get("produto") or ""))
    elif acao == "espera_avisar":
        ok, err = acao_lista_espera_avisar(
            conversa_id=cid, produto=str(data.get("produto") or ""), autor=autor
        )
    elif acao == "vip":
        tags = data.get("tags") if isinstance(data.get("tags"), list) else None
        ok, err = acao_set_vip(conversa_id=cid, vip=bool(data.get("vip")), tags=tags)
    elif acao == "orcamento":
        if not recurso_on(cfg, "feat_orcamento_zap"):
            return JsonResponse(
                {"ok": False, "erro": "Ligue «Orçamento no Zap» em Bot → Recursos."},
                status=400,
            )
        texto = str(data.get("texto") or "").strip()
        if not texto:
            return JsonResponse({"ok": False, "erro": "Texto do orçamento vazio."}, status=400)
        if cid <= 0:
            conv, err_ab = abrir_conversa_busca(
                telefone=str(data.get("telefone") or ""),
                nome=str(data.get("nome") or ""),
                jid=str(data.get("jid") or ""),
            )
            if err_ab or conv is None:
                return JsonResponse({"ok": False, "erro": err_ab or "Não achou o chat."}, status=400)
            cid = int(conv.pk)
        m, err = enviar_loja(conversa_id=cid, texto=texto, autor=autor)
        ok, err = (m is not None and not err), (err or "")
        if ok:
            return JsonResponse({"ok": True, "conversa_id": cid})
    elif acao == "pedir_loja_aviso":
        if not recurso_on(cfg, "feat_pedir_loja_aviso"):
            return JsonResponse({"ok": False, "erro": "Recurso desligado (Bot → Recursos)."}, status=400)
        texto = str(data.get("texto") or "Atualização do pedido entre lojas.").strip()
        if cid <= 0:
            return JsonResponse({"ok": False, "erro": "Conversa inválida."}, status=400)
        m, err = enviar_loja(conversa_id=cid, texto=texto, autor=autor or "Sistema")
        ok, err = (m is not None and not err), (err or "")
    elif acao == "fornecedor":
        if not recurso_on(cfg, "feat_fornecedor_zap"):
            return JsonResponse({"ok": False, "erro": "Recurso desligado (Bot → Recursos)."}, status=400)
        return JsonResponse(
            {
                "ok": True,
                "aviso": "Recurso ligado — use Compras → WhatsApp; unificação completa no próximo ajuste.",
            }
        )
    elif acao == "audio_texto":
        if not recurso_on(cfg, "feat_audio_texto"):
            return JsonResponse({"ok": False, "erro": "Recurso desligado (Bot → Recursos)."}, status=400)
        return JsonResponse(
            {"ok": True, "texto": "", "aviso": "Transcrição ainda não ativa — flag só prepara o caminho."}
        )
    else:
        return JsonResponse({"ok": False, "erro": "Ação desconhecida."}, status=400)

    if not ok:
        return JsonResponse({"ok": False, "erro": err or "Falhou."}, status=400)
    return JsonResponse({"ok": True})
