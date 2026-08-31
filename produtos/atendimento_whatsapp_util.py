"""Atendimento WhatsApp — roteamento Centro/Vila + fila da ponte QR."""
from __future__ import annotations

import hmac
import re
import unicodedata
from datetime import timedelta

from django.conf import settings
from django.db import transaction
from django.http import HttpRequest
from django.utils import timezone

from produtos.models import (
    WhatsAppConversaAgro,
    WhatsAppMensagemAgro,
    WhatsAppPonteEstadoAgro,
)

CHAVE_PONTE = "default"
TEXTO_MAX = 4000
PREVIEW_MAX = 160
HEARTBEAT_OK_SEG = 45

MSG_MENU = (
    "Olá! Você quer falar com qual loja?\n\n"
    "1 — Centro (Jacupiranga)\n"
    "2 — Vila Elias\n\n"
    "Responda *1* ou *2*."
)
MSG_PEDIR_DE_NOVO = "Responda *1* para o Centro ou *2* para a Vila Elias."
MSG_OK_CENTRO = "Certo! Você está falando com a loja do *Centro*. Em breve alguém atende por aqui."
MSG_OK_VILA = "Certo! Você está falando com a loja da *Vila Elias*. Em breve alguém atende por aqui."


def token_ponte() -> str:
    return (getattr(settings, "AGRO_WA_BRIDGE_TOKEN", "") or "").strip()


def token_ponte_ok(request: HttpRequest) -> bool:
    esperado = token_ponte()
    if not esperado:
        return False
    recebido = (
        (request.headers.get("X-Agro-Wa-Token") or "")
        or (request.GET.get("token") or "")
        or ""
    ).strip()
    if not recebido:
        return False
    return hmac.compare_digest(recebido.encode("utf-8"), esperado.encode("utf-8"))


def _sem_acento(s: str) -> str:
    n = unicodedata.normalize("NFD", s or "")
    return "".join(c for c in n if unicodedata.category(c) != "Mn")


def interpretar_loja(texto: str) -> str:
    t = _sem_acento(" ".join(str(texto or "").strip().lower().split()))
    if not t:
        return ""
    if t in ("1", "centro", "loja centro", "jacupiranga", "c"):
        return WhatsAppConversaAgro.LOJA_CENTRO
    if t in ("2", "vila", "vila elias", "loja vila", "v"):
        return WhatsAppConversaAgro.LOJA_VILA
    if "vila" in t:
        return WhatsAppConversaAgro.LOJA_VILA
    if "centro" in t:
        return WhatsAppConversaAgro.LOJA_CENTRO
    return ""


def jid_para_telefone(jid: str) -> str:
    raw = (jid or "").split("@", 1)[0]
    return re.sub(r"\D+", "", raw)[:32]


def obter_ponte() -> WhatsAppPonteEstadoAgro:
    obj, _ = WhatsAppPonteEstadoAgro.objects.get_or_create(chave=CHAVE_PONTE)
    return obj


def ponte_viva(estado: WhatsAppPonteEstadoAgro | None = None) -> bool:
    estado = estado or obter_ponte()
    hb = estado.heartbeat_em
    if not hb:
        return False
    return timezone.now() - hb <= timedelta(seconds=HEARTBEAT_OK_SEG)


def serializar_ponte(estado: WhatsAppPonteEstadoAgro | None = None) -> dict:
    estado = estado or obter_ponte()
    viva = ponte_viva(estado)
    status = estado.status if viva else WhatsAppPonteEstadoAgro.STATUS_DESCONECTADO
    mostrar_qr = viva and status == WhatsAppPonteEstadoAgro.STATUS_QR and bool(estado.qr_data_url)
    return {
        "status": status,
        "conectada": viva and status == WhatsAppPonteEstadoAgro.STATUS_CONECTADO,
        "ponte_viva": viva,
        "numero": estado.numero or "",
        "aviso": estado.aviso or "",
        "qr": estado.qr_data_url if mostrar_qr else "",
        "heartbeat_em": estado.heartbeat_em.isoformat() if estado.heartbeat_em else "",
    }


def _preview(texto: str) -> str:
    t = " ".join(str(texto or "").split())
    if len(t) > PREVIEW_MAX:
        return t[: PREVIEW_MAX - 1] + "…"
    return t


def serializar_conversa(c: WhatsAppConversaAgro) -> dict:
    ult = c.ultima_em
    try:
        ult_l = timezone.localtime(ult) if ult else None
    except Exception:
        ult_l = ult
    return {
        "id": int(c.pk),
        "jid": c.jid,
        "telefone": c.telefone or "",
        "nome": c.nome or "",
        "loja": c.loja,
        "nao_lidas": int(c.nao_lidas or 0),
        "ultima_preview": c.ultima_preview or "",
        "ultima_em": ult.isoformat() if ult else "",
        "hora": ult_l.strftime("%H:%M") if ult_l else "",
        "data": ult_l.strftime("%d/%m") if ult_l else "",
    }


def serializar_mensagem(m: WhatsAppMensagemAgro) -> dict:
    criado = m.criado_em
    try:
        criado_l = timezone.localtime(criado) if criado else None
    except Exception:
        criado_l = criado
    return {
        "id": int(m.pk),
        "conversa_id": int(m.conversa_id),
        "direcao": m.direcao,
        "texto": m.texto or "",
        "autor": m.autor_nome or "",
        "pendente": bool(m.pendente_envio),
        "hora": criado_l.strftime("%H:%M") if criado_l else "",
        "data": criado_l.strftime("%d/%m") if criado_l else "",
        "criado_em": criado.isoformat() if criado else "",
    }


def listar_conversas(*, loja: str, limit: int = 80) -> list[dict]:
    loja_n = (loja or "").strip().lower()
    qs = WhatsAppConversaAgro.objects.all()
    if loja_n in (
        WhatsAppConversaAgro.LOJA_PENDENTE,
        WhatsAppConversaAgro.LOJA_CENTRO,
        WhatsAppConversaAgro.LOJA_VILA,
    ):
        qs = qs.filter(loja=loja_n)
    lim = max(1, min(int(limit or 80), 200))
    return [serializar_conversa(c) for c in qs.order_by("-ultima_em", "-id")[:lim]]


def listar_mensagens(*, conversa_id: int, after_id: int = 0, limit: int = 120) -> list[dict]:
    lim = max(1, min(int(limit or 120), 300))
    qs = WhatsAppMensagemAgro.objects.filter(conversa_id=int(conversa_id))
    aid = int(after_id or 0)
    if aid > 0:
        qs = qs.filter(id__gt=aid).order_by("id")[:lim]
        return [serializar_mensagem(m) for m in qs]
    ids = list(qs.order_by("-id").values_list("id", flat=True)[:lim])
    if not ids:
        return []
    rows = list(WhatsAppMensagemAgro.objects.filter(id__in=ids).order_by("id"))
    return [serializar_mensagem(m) for m in rows]


def contar_nao_lidas() -> dict:
    from django.db.models import Sum

    base = WhatsAppConversaAgro.objects.values("loja").annotate(n=Sum("nao_lidas"))
    out = {
        WhatsAppConversaAgro.LOJA_PENDENTE: 0,
        WhatsAppConversaAgro.LOJA_CENTRO: 0,
        WhatsAppConversaAgro.LOJA_VILA: 0,
    }
    for row in base:
        k = row.get("loja") or ""
        if k in out:
            out[k] = int(row.get("n") or 0)
    return out


def _enfileirar_saida(conversa: WhatsAppConversaAgro, texto: str, *, direcao: str, autor: str = "") -> WhatsAppMensagemAgro:
    agora = timezone.now()
    m = WhatsAppMensagemAgro.objects.create(
        conversa=conversa,
        direcao=direcao,
        texto=texto[:TEXTO_MAX],
        pendente_envio=True,
        autor_nome=(autor or "")[:120],
    )
    conversa.ultima_preview = _preview(texto)
    conversa.ultima_em = agora
    conversa.save(update_fields=["ultima_preview", "ultima_em"])
    return m


def responder_bot(conversa: WhatsAppConversaAgro, texto: str) -> WhatsAppMensagemAgro:
    return _enfileirar_saida(conversa, texto, direcao=WhatsAppMensagemAgro.DIRECAO_BOT, autor="Bot")


@transaction.atomic
def processar_entrada(
    *,
    jid: str,
    texto: str,
    nome: str = "",
    wa_id: str = "",
) -> tuple[WhatsAppMensagemAgro | None, str]:
    jid_n = (jid or "").strip()
    if not jid_n or jid_n.endswith("@g.us") or jid_n == "status@broadcast":
        return None, "ignorado"
    t = str(texto or "").strip()
    if not t:
        t = "[mensagem sem texto]"
    t = t[:TEXTO_MAX]
    wa = (wa_id or "").strip()[:80]
    if wa and WhatsAppMensagemAgro.objects.filter(wa_id=wa).exists():
        return None, "duplicada"

    conv, _criada = WhatsAppConversaAgro.objects.select_for_update().get_or_create(
        jid=jid_n[:80],
        defaults={
            "telefone": jid_para_telefone(jid_n),
            "nome": (nome or "")[:120],
        },
    )
    if nome and not conv.nome:
        conv.nome = nome[:120]
    if not conv.telefone:
        conv.telefone = jid_para_telefone(jid_n)

    msg = WhatsAppMensagemAgro.objects.create(
        conversa=conv,
        direcao=WhatsAppMensagemAgro.DIRECAO_IN,
        texto=t,
        wa_id=wa,
        pendente_envio=False,
    )
    conv.ultima_preview = _preview(t)
    conv.ultima_em = timezone.now()
    conv.nao_lidas = int(conv.nao_lidas or 0) + 1

    if conv.loja == WhatsAppConversaAgro.LOJA_PENDENTE:
        escolha = interpretar_loja(t)
        if escolha:
            conv.loja = escolha
            conv.save(
                update_fields=["nome", "telefone", "loja", "ultima_preview", "ultima_em", "nao_lidas"]
            )
            ok = MSG_OK_VILA if escolha == WhatsAppConversaAgro.LOJA_VILA else MSG_OK_CENTRO
            responder_bot(conv, ok)
        elif not conv.menu_enviado:
            conv.menu_enviado = True
            conv.save(
                update_fields=[
                    "nome",
                    "telefone",
                    "menu_enviado",
                    "ultima_preview",
                    "ultima_em",
                    "nao_lidas",
                ]
            )
            responder_bot(conv, MSG_MENU)
        else:
            conv.save(update_fields=["nome", "telefone", "ultima_preview", "ultima_em", "nao_lidas"])
            responder_bot(conv, MSG_PEDIR_DE_NOVO)
    else:
        conv.save(update_fields=["nome", "telefone", "ultima_preview", "ultima_em", "nao_lidas"])
    return msg, ""


def enviar_loja(*, conversa_id: int, texto: str, autor: str = "") -> tuple[WhatsAppMensagemAgro | None, str]:
    t = str(texto or "").strip()
    if not t:
        return None, "Digite uma mensagem."
    if len(t) > TEXTO_MAX:
        return None, f"Máximo {TEXTO_MAX} caracteres."
    try:
        conv = WhatsAppConversaAgro.objects.get(pk=int(conversa_id))
    except (WhatsAppConversaAgro.DoesNotExist, TypeError, ValueError):
        return None, "Conversa não encontrada."
    if conv.loja == WhatsAppConversaAgro.LOJA_PENDENTE:
        return None, "Cliente ainda não escolheu a loja."
    m = _enfileirar_saida(
        conv,
        t,
        direcao=WhatsAppMensagemAgro.DIRECAO_OUT,
        autor=autor,
    )
    return m, ""


def marcar_lidas(conversa_id: int) -> None:
    WhatsAppConversaAgro.objects.filter(pk=int(conversa_id)).update(nao_lidas=0)


def definir_loja(conversa_id: int, loja: str) -> tuple[WhatsAppConversaAgro | None, str]:
    loja_n = (loja or "").strip().lower()
    if loja_n not in (WhatsAppConversaAgro.LOJA_CENTRO, WhatsAppConversaAgro.LOJA_VILA):
        return None, "Loja inválida."
    try:
        conv = WhatsAppConversaAgro.objects.get(pk=int(conversa_id))
    except (WhatsAppConversaAgro.DoesNotExist, TypeError, ValueError):
        return None, "Conversa não encontrada."
    conv.loja = loja_n
    conv.save(update_fields=["loja"])
    return conv, ""


def toque_heartbeat() -> WhatsAppPonteEstadoAgro:
    obj = obter_ponte()
    obj.heartbeat_em = timezone.now()
    obj.save(update_fields=["heartbeat_em"])
    return obj


def atualizar_ponte(*, status: str, qr: str = "", numero: str = "", aviso: str = "") -> WhatsAppPonteEstadoAgro:
    st = (status or "").strip().lower()
    if st not in (
        WhatsAppPonteEstadoAgro.STATUS_DESCONECTADO,
        WhatsAppPonteEstadoAgro.STATUS_QR,
        WhatsAppPonteEstadoAgro.STATUS_CONECTADO,
    ):
        st = WhatsAppPonteEstadoAgro.STATUS_DESCONECTADO
    obj = obter_ponte()
    obj.status = st
    obj.heartbeat_em = timezone.now()
    if st == WhatsAppPonteEstadoAgro.STATUS_QR:
        obj.qr_data_url = (qr or "")[:200000]
    else:
        obj.qr_data_url = ""
    if numero:
        obj.numero = str(numero).strip()[:32]
    if st == WhatsAppPonteEstadoAgro.STATUS_DESCONECTADO and not numero:
        pass
    obj.aviso = (aviso or "")[:240]
    obj.save()
    return obj


def listar_saida_pendente(limit: int = 20) -> list[dict]:
    lim = max(1, min(int(limit or 20), 50))
    qs = (
        WhatsAppMensagemAgro.objects.select_related("conversa")
        .filter(pendente_envio=True)
        .order_by("id")[:lim]
    )
    out = []
    for m in qs:
        out.append(
            {
                "id": int(m.pk),
                "jid": m.conversa.jid,
                "texto": m.texto or "",
            }
        )
    return out


def marcar_enviadas(ids: list[int], *, erro: str = "") -> int:
    if not ids:
        return 0
    agora = timezone.now()
    if erro:
        return WhatsAppMensagemAgro.objects.filter(id__in=ids, pendente_envio=True).update(
            pendente_envio=False,
            erro_envio=str(erro)[:200],
        )
    return WhatsAppMensagemAgro.objects.filter(id__in=ids, pendente_envio=True).update(
        pendente_envio=False,
        enviado_em=agora,
        erro_envio="",
    )
