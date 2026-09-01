"""Atendimento WhatsApp — roteamento Centro/Vila + fila da ponte QR."""
from __future__ import annotations

import base64
import hmac
import re
import unicodedata
import uuid
from datetime import datetime, timedelta, timezone as dt_timezone

from django.conf import settings
from django.core.files.base import ContentFile
from django.db import transaction
from django.db.models import Q
from django.http import HttpRequest
from django.utils import timezone

from produtos.cliente_whatsapp_util import cliente_agro_por_whatsapp_flex
from produtos.models import (
    ClienteAgro,
    WhatsAppAgendaContatoAgro,
    WhatsAppConversaAgro,
    WhatsAppMensagemAgro,
    WhatsAppPonteEstadoAgro,
    WhatsAppPontePedidoAgro,
)

MAX_NOVOS_DIA = 20
MAX_HIST_MSGS = 40
MAX_MIDIA_BYTES = 6_000_000
DIAS_HISTORICO = 7

CHAVE_PONTE = "default"
TEXTO_MAX = 4000
PREVIEW_MAX = 160
HEARTBEAT_OK_SEG = 45

MSG_MENU = (
    "Olá! Você quer falar com qual loja?\n\n"
    "1 — Centro (Jacupiranga)\n"
    "2 — Vila Elias\n\n"
    "Responda *1* ou *2*.\n\n"
    "Para ver o fiado em aberto, escreva *fiado*."
)
MSG_PEDIR_DE_NOVO = "Responda *1* para o Centro ou *2* para a Vila Elias. Para o fiado, escreva *fiado*."
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


def interpretar_consulta_fiado(texto: str, cfg: dict | None = None) -> bool:
    from produtos.atendimento_whatsapp_bot_config import BOT_DEFAULT, _casa_palavra, _palavras

    c = cfg if cfg is not None else BOT_DEFAULT
    if not c.get("fiado_ligado", True):
        return False
    t = _sem_acento(" ".join(str(texto or "").strip().lower().split()))
    if not t:
        return False
    if interpretar_loja(t, c):
        return False
    return _casa_palavra(t, _palavras(c.get("fiado_palavras") or ""))


def _fmt_rs(val) -> str:
    from produtos.caixa_util import format_moeda_br

    return f"R$ {format_moeda_br(val)}"


def montar_texto_fiado(telefone: str, cfg: dict | None = None) -> str:
    from decimal import Decimal

    from produtos.atendimento_whatsapp_bot_config import BOT_DEFAULT
    from produtos.cliente_whatsapp_util import cliente_agro_por_whatsapp_flex
    from produtos.models import FiadoTituloAgro

    c = cfg if cfg is not None else BOT_DEFAULT
    cli = cliente_agro_por_whatsapp_flex(telefone)
    if cli == "varios":
        return str(c.get("msg_fiado_varios") or BOT_DEFAULT["msg_fiado_varios"])
    if cli is None:
        return str(c.get("msg_fiado_sem_cadastro") or BOT_DEFAULT["msg_fiado_sem_cadastro"])
    qs = (
        FiadoTituloAgro.objects.filter(cliente_agro_id=cli.pk)
        .exclude(
            situacao__in=(
                FiadoTituloAgro.Situacao.QUITADO,
                FiadoTituloAgro.Situacao.CANCELADO,
            )
        )
        .order_by("vencimento", "pk")
    )
    linhas = []
    total = Decimal("0.00")
    max_p = int(c.get("fiado_max_parcelas") or 8)
    for tit in qs:
        s = tit.saldo_aberto
        if s <= 0:
            continue
        total += s
        venc = tit.vencimento.strftime("%d/%m") if tit.vencimento else "—"
        linhas.append(f"• {venc} · {_fmt_rs(s)}")
    nome = (cli.nome or "cliente").strip()
    if not linhas:
        tpl = str(c.get("msg_fiado_vazio") or BOT_DEFAULT["msg_fiado_vazio"])
        return tpl.replace("{nome}", nome)
    extra = ""
    mostrar = linhas[:max_p]
    if len(linhas) > max_p:
        extra = f"\n… e mais {len(linhas) - max_p} parcela(s)."
    corpo = "\n".join(mostrar) + extra
    tpl = str(c.get("msg_fiado_aberto") or BOT_DEFAULT["msg_fiado_aberto"])
    return (
        tpl.replace("{nome}", nome)
        .replace("{total}", _fmt_rs(total))
        .replace("{linhas}", corpo)
        .replace("{empresa}", str(c.get("nome_empresa") or ""))
    )


def interpretar_loja(texto: str, cfg: dict | None = None) -> str:
    from produtos.atendimento_whatsapp_bot_config import BOT_DEFAULT, _casa_palavra, _palavras

    c = cfg if cfg is not None else BOT_DEFAULT
    t = _sem_acento(" ".join(str(texto or "").strip().lower().split()))
    if not t:
        return ""
    id1 = str(c.get("loja1_id") or WhatsAppConversaAgro.LOJA_CENTRO).strip().lower()
    id2 = str(c.get("loja2_id") or WhatsAppConversaAgro.LOJA_VILA).strip().lower()
    if id1 not in (WhatsAppConversaAgro.LOJA_CENTRO, WhatsAppConversaAgro.LOJA_VILA):
        id1 = WhatsAppConversaAgro.LOJA_CENTRO
    if id2 not in (WhatsAppConversaAgro.LOJA_CENTRO, WhatsAppConversaAgro.LOJA_VILA):
        id2 = WhatsAppConversaAgro.LOJA_VILA
    if _casa_palavra(t, _palavras(c.get("loja1_palavras") or "")):
        return id1
    if _casa_palavra(t, _palavras(c.get("loja2_palavras") or "")):
        return id2
    return ""


def jid_para_telefone(jid: str) -> str:
    raw = (jid or "").split("@", 1)[0]
    return re.sub(r"\D+", "", raw)[:32]


def aplicar_nome_cadastro(conv: WhatsAppConversaAgro) -> None:
    tel = conv.telefone or jid_para_telefone(conv.jid)
    cli = cliente_agro_por_whatsapp_flex(tel)
    if cli is None or cli == "varios":
        return
    n = (getattr(cli, "nome", None) or "").strip()[:120]
    if n:
        conv.nome = n


def _ext_midia(tipo: str, mime: str, nome: str) -> str:
    n = (nome or "").lower()
    for ext in (".jpg", ".jpeg", ".png", ".webp", ".ogg", ".opus", ".mp3", ".m4a", ".mp4"):
        if n.endswith(ext):
            return ext if ext != ".jpeg" else ".jpg"
    m = (mime or "").lower()
    if "png" in m:
        return ".png"
    if "webp" in m:
        return ".webp"
    if "ogg" in m or "opus" in m:
        return ".ogg"
    if "mpeg" in m or "mp3" in m:
        return ".mp3"
    if "mp4" in m:
        return ".mp4"
    if (tipo or "") in ("image", "sticker"):
        return ".jpg"
    if tipo == "audio":
        return ".ogg"
    return ".bin"


def anexar_midia(
    msg: WhatsAppMensagemAgro,
    *,
    tipo_midia: str = "",
    midia_b64: str = "",
    mime: str = "",
    nome_arquivo: str = "",
) -> None:
    tipo = (tipo_midia or "").strip().lower()[:16]
    if tipo:
        msg.tipo_midia = tipo
    raw_b64 = (midia_b64 or "").strip()
    if not raw_b64:
        return
    try:
        raw = base64.b64decode(raw_b64)
    except Exception:
        return
    if not raw or len(raw) > MAX_MIDIA_BYTES:
        return
    ext = _ext_midia(tipo, mime, nome_arquivo)
    fname = f"{uuid.uuid4().hex[:16]}{ext}"
    msg.arquivo.save(fname, ContentFile(raw), save=False)


def jid_eh_chat_privado(jid: str) -> bool:
    """Só conversa 1-a-1 de celular — bloqueia grupo, canal e ID falso."""
    j = (jid or "").strip().lower()
    if not j.endswith("@s.whatsapp.net"):
        return False
    bloqueados = ("@g.us", "@newsletter", "@broadcast", "@lid")
    if any(x in j for x in bloqueados):
        return False
    num = jid_para_telefone(j)
    if not num:
        return False
    # Grupo/canal costuma vir como 120363… (não é celular BR)
    if num.startswith("120") and len(num) >= 15:
        return False
    if len(num) < 10 or len(num) > 13:
        return False
    return True


def telefone_para_jid(tel: str) -> str:
    d = re.sub(r"\D+", "", tel or "")
    if not d:
        return ""
    if d.startswith("55") and len(d) >= 12:
        pass
    elif len(d) in (10, 11):
        d = "55" + d
    elif len(d) < 10:
        return ""
    return f"{d}@s.whatsapp.net"


def _ts_aware(ts) -> datetime:
    if ts in (None, "", 0, "0"):
        return timezone.now()
    try:
        n = float(ts)
    except (TypeError, ValueError):
        return timezone.now()
    if n > 1e12:
        n = n / 1000.0
    try:
        return datetime.fromtimestamp(n, tz=dt_timezone.utc)
    except (OSError, OverflowError, ValueError):
        return timezone.now()


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
        "pairing_code": (estado.pairing_code or "") if viva and status != WhatsAppPonteEstadoAgro.STATUS_CONECTADO else "",
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
        "tipo_midia": m.tipo_midia or "",
        "midia_url": f"/api/atendimento-whatsapp/midia/{int(m.pk)}/" if m.arquivo else "",
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
    ids = list(qs.order_by("-criado_em", "-id").values_list("id", flat=True)[:lim])
    if not ids:
        return []
    rows = list(WhatsAppMensagemAgro.objects.filter(id__in=ids).order_by("criado_em", "id"))
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


def _enfileirar_saida(
    conversa: WhatsAppConversaAgro,
    texto: str,
    *,
    direcao: str,
    autor: str = "",
    delay_seg: int = 0,
) -> WhatsAppMensagemAgro:
    agora = timezone.now()
    liberar = None
    try:
        d = int(delay_seg or 0)
    except (TypeError, ValueError):
        d = 0
    if d > 0:
        liberar = agora + timedelta(seconds=d)
    m = WhatsAppMensagemAgro.objects.create(
        conversa=conversa,
        direcao=direcao,
        texto=texto[:TEXTO_MAX],
        pendente_envio=True,
        autor_nome=(autor or "")[:120],
        liberar_envio_em=liberar,
    )
    conversa.ultima_preview = _preview(texto)
    conversa.ultima_em = agora
    conversa.save(update_fields=["ultima_preview", "ultima_em"])
    return m


def responder_bot(conversa: WhatsAppConversaAgro, texto: str, *, delay_seg: int = 0) -> WhatsAppMensagemAgro:
    return _enfileirar_saida(
        conversa, texto, direcao=WhatsAppMensagemAgro.DIRECAO_BOT, autor="Bot", delay_seg=delay_seg
    )


def _enviar_lote_bot(conversa: WhatsAppConversaAgro, textos: list[str], cfg: dict) -> None:
    from produtos.atendimento_whatsapp_bot_config import delays_bot

    msgs = [str(x).strip() for x in textos if str(x or "").strip()]
    if not msgs:
        return
    empresa = str(cfg.get("nome_empresa") or "")
    ds = delays_bot(cfg, len(msgs))
    for i, txt in enumerate(msgs):
        t = txt.replace("{empresa}", empresa)[:TEXTO_MAX]
        responder_bot(conversa, t, delay_seg=ds[i] if i < len(ds) else 0)


@transaction.atomic
def processar_entrada(
    *,
    jid: str,
    texto: str,
    nome: str = "",
    wa_id: str = "",
    historico: bool = False,
    de_mim: bool = False,
    ts=None,
    tipo_midia: str = "",
    midia_b64: str = "",
    mime: str = "",
    nome_arquivo: str = "",
) -> tuple[WhatsAppMensagemAgro | None, str]:
    jid_n = (jid or "").strip()
    if not jid_eh_chat_privado(jid_n):
        return None, "ignorado"
    t = str(texto or "").strip()
    tipo_n = (tipo_midia or "").strip().lower()[:16]
    if not t:
        t = {
            "image": "[imagem]",
            "audio": "[áudio]",
            "sticker": "[figurinha]",
            "video": "[vídeo]",
            "document": "[arquivo]",
        }.get(tipo_n, "[mensagem sem texto]")
    t = t[:TEXTO_MAX]
    wa = (wa_id or "").strip()[:80]
    if wa and WhatsAppMensagemAgro.objects.filter(wa_id=wa).exists():
        return None, "duplicada"

    quando = _ts_aware(ts)
    if historico:
        limite = timezone.now() - timedelta(days=DIAS_HISTORICO)
        if quando < limite:
            return None, "ignorado"

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
    aplicar_nome_cadastro(conv)

    direcao = WhatsAppMensagemAgro.DIRECAO_OUT if de_mim else WhatsAppMensagemAgro.DIRECAO_IN
    msg = WhatsAppMensagemAgro(
        conversa=conv,
        direcao=direcao,
        texto=t,
        wa_id=wa,
        pendente_envio=False,
        autor_nome="Celular" if de_mim else "",
        criado_em=quando,
        tipo_midia=tipo_n,
    )
    anexar_midia(msg, tipo_midia=tipo_n, midia_b64=midia_b64, mime=mime, nome_arquivo=nome_arquivo)
    msg.save()
    conv.ultima_preview = _preview(t)
    if not historico:
        conv.ultima_em = timezone.now()
        if not de_mim:
            conv.nao_lidas = int(conv.nao_lidas or 0) + 1
    elif not conv.ultima_em:
        conv.ultima_em = quando

    from produtos.atendimento_whatsapp_bot_config import carregar_bot, fora_do_horario

    cfg = carregar_bot()
    campos_base = ["nome", "telefone", "ultima_preview", "ultima_em", "nao_lidas"]
    fone = conv.telefone or jid_para_telefone(jid_n)

    def _ok_loja(escolha: str) -> str:
        if escolha == str(cfg.get("loja2_id") or "vila"):
            return str(cfg.get("msg_ok_loja2") or MSG_OK_VILA)
        return str(cfg.get("msg_ok_loja1") or MSG_OK_CENTRO)

    def _menu_textos() -> list[str]:
        out = []
        if cfg.get("enviar_boas_vindas"):
            bv = str(cfg.get("msg_boas_vindas") or "").strip()
            if bv:
                out.append(bv)
        out.append(str(cfg.get("msg_menu") or MSG_MENU))
        return out

    if historico or de_mim or not cfg.get("bot_ligado"):
        conv.save(update_fields=campos_base)
        return msg, ""

    lote: list[str] = []
    if fora_do_horario(cfg):
        fh = str(cfg.get("msg_fora_horario") or "").strip()
        if fh:
            lote.append(fh)
        if not cfg.get("ainda_atende_fora"):
            conv.save(update_fields=campos_base)
            _enviar_lote_bot(conv, lote, cfg)
            return msg, ""

    eh_fiado = interpretar_consulta_fiado(t, cfg)
    escolha = interpretar_loja(t, cfg) if conv.loja == WhatsAppConversaAgro.LOJA_PENDENTE else ""
    ordem_loja_primeiro = str(cfg.get("ordem") or "") == "loja_primeiro"

    def _fiado_fluxo() -> None:
        lote.append(montar_texto_fiado(fone, cfg))
        if conv.loja == WhatsAppConversaAgro.LOJA_PENDENTE and cfg.get("fiado_manda_menu"):
            if not conv.menu_enviado:
                conv.menu_enviado = True
                lote.extend(_menu_textos())

    if conv.loja == WhatsAppConversaAgro.LOJA_PENDENTE:
        if ordem_loja_primeiro and escolha:
            conv.loja = escolha
            conv.save(update_fields=campos_base + ["loja"])
            lote.append(_ok_loja(escolha))
            if cfg.get("ausencia_ligada"):
                au = str(cfg.get("msg_ausencia") or "").strip()
                if au:
                    lote.append(au)
            _enviar_lote_bot(conv, lote, cfg)
            return msg, ""
        if eh_fiado:
            _fiado_fluxo()
            campos = list(campos_base)
            if conv.menu_enviado:
                campos.append("menu_enviado")
            conv.save(update_fields=campos)
            _enviar_lote_bot(conv, lote, cfg)
            return msg, ""
        if escolha:
            conv.loja = escolha
            conv.save(update_fields=campos_base + ["loja"])
            lote.append(_ok_loja(escolha))
            if cfg.get("ausencia_ligada"):
                au = str(cfg.get("msg_ausencia") or "").strip()
                if au:
                    lote.append(au)
            _enviar_lote_bot(conv, lote, cfg)
            return msg, ""
        if not conv.menu_enviado:
            conv.menu_enviado = True
            conv.save(update_fields=campos_base + ["menu_enviado"])
            lote.extend(_menu_textos())
            _enviar_lote_bot(conv, lote, cfg)
            return msg, ""
        conv.save(update_fields=campos_base)
        if cfg.get("repetir_menu", True):
            lote.append(str(cfg.get("msg_pedir_de_novo") or MSG_PEDIR_DE_NOVO))
        _enviar_lote_bot(conv, lote, cfg)
        return msg, ""

    if eh_fiado:
        conv.save(update_fields=campos_base)
        lote.append(montar_texto_fiado(fone, cfg))
        _enviar_lote_bot(conv, lote, cfg)
        return msg, ""

    conv.save(update_fields=campos_base)
    if lote:
        _enviar_lote_bot(conv, lote, cfg)
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
    if conv.loja == WhatsAppConversaAgro.LOJA_PENDENTE and conv.origem_abertura != "loja":
        return None, "Cliente ainda não escolheu a loja."
    m = _enfileirar_saida(
        conv,
        t,
        direcao=WhatsAppMensagemAgro.DIRECAO_OUT,
        autor=autor,
    )
    return m, ""


def _novos_loja_24h() -> int:
    corte = timezone.now() - timedelta(hours=24)
    return WhatsAppConversaAgro.objects.filter(
        origem_abertura="loja", criado_em__gte=corte
    ).count()


@transaction.atomic
def abrir_conversa_saida(
    *,
    telefone: str,
    loja: str,
    texto: str,
    nome: str = "",
    autor: str = "",
) -> tuple[WhatsAppMensagemAgro | None, str]:
    jid = telefone_para_jid(telefone)
    if not jid:
        return None, "Número inválido."
    loja_n = (loja or "").strip().lower()
    if loja_n not in (WhatsAppConversaAgro.LOJA_CENTRO, WhatsAppConversaAgro.LOJA_VILA):
        return None, "Escolha Centro ou Vila."
    t = str(texto or "").strip()
    if not t:
        return None, "Digite uma mensagem."
    if len(t) > TEXTO_MAX:
        return None, f"Máximo {TEXTO_MAX} caracteres."
    conv = WhatsAppConversaAgro.objects.select_for_update().filter(jid=jid[:80]).first()
    if conv is None:
        if _novos_loja_24h() >= MAX_NOVOS_DIA:
            return None, "Limite de conversas novas hoje (20). Sem disparo em massa."
        conv = WhatsAppConversaAgro.objects.create(
            jid=jid[:80],
            telefone=jid_para_telefone(jid),
            nome=(nome or "")[:120],
            loja=loja_n,
            menu_enviado=True,
            origem_abertura="loja",
        )
        aplicar_nome_cadastro(conv)
        conv.save(update_fields=["nome"])
    else:
        campos = []
        if conv.loja == WhatsAppConversaAgro.LOJA_PENDENTE:
            conv.loja = loja_n
            campos.append("loja")
        if nome and not conv.nome:
            conv.nome = nome[:120]
        aplicar_nome_cadastro(conv)
        campos.append("nome")
        conv.save(update_fields=campos)
    return enviar_loja(conversa_id=int(conv.pk), texto=t, autor=autor)


@transaction.atomic
def abrir_conversa_busca(*, telefone: str, nome: str = "") -> tuple[WhatsAppConversaAgro | None, str]:
    jid = telefone_para_jid(telefone)
    if not jid:
        return None, "Número inválido."
    conv = WhatsAppConversaAgro.objects.select_for_update().filter(jid=jid[:80]).first()
    if conv is None:
        if _novos_loja_24h() >= MAX_NOVOS_DIA:
            return None, "Limite de conversas novas hoje (20)."
        conv = WhatsAppConversaAgro.objects.create(
            jid=jid[:80],
            telefone=jid_para_telefone(jid),
            nome=(nome or "")[:120],
            loja=WhatsAppConversaAgro.LOJA_PENDENTE,
            menu_enviado=True,
            origem_abertura="loja",
        )
    if nome and not conv.nome:
        conv.nome = nome[:120]
    if not conv.telefone:
        conv.telefone = jid_para_telefone(jid)
    aplicar_nome_cadastro(conv)
    conv.save(update_fields=["nome", "telefone"])
    return conv, ""


def buscar_contatos_envio(termo: str, *, limit: int = 20) -> list[dict]:
    lim = max(1, min(int(limit or 20), 40))
    t = (termo or "").strip()
    if len(t) < 1:
        return []
    dig = re.sub(r"\D+", "", t)
    out: list[dict] = []
    seen: set[str] = set()

    def _add(origem: str, nome: str, telefone: str, jid: str) -> None:
        if len(out) >= lim:
            return
        j = (jid or telefone_para_jid(telefone) or "").strip()[:80]
        if not j or j in seen:
            return
        seen.add(j)
        conv = WhatsAppConversaAgro.objects.filter(jid=j).only("id", "loja").first()
        out.append(
            {
                "origem": origem,
                "nome": (nome or "")[:120],
                "telefone": (telefone or jid_para_telefone(j))[:32],
                "jid": j,
                "conversa_id": int(conv.pk) if conv else 0,
                "loja": (conv.loja if conv else "") or "",
            }
        )

    if len(t) >= 2:
        pedir_agenda_zap()

    cli = ClienteAgro.objects.filter(ativo=True).exclude(whatsapp="")
    q = Q(nome__icontains=t)
    if dig:
        q |= Q(whatsapp__icontains=dig)
    for c in cli.filter(q).order_by("nome")[:lim]:
        _add("cadastro", c.nome or "", c.whatsapp or "", telefone_para_jid(c.whatsapp or ""))

    convs = WhatsAppConversaAgro.objects.filter(Q(nome__icontains=t) | Q(telefone__icontains=dig or t))
    for c in convs.order_by("-ultima_em")[:lim]:
        _add("conversa", c.nome or "", c.telefone or "", c.jid)

    ag = WhatsAppAgendaContatoAgro.objects.all()
    qag = Q(nome__icontains=t)
    if dig:
        qag |= Q(telefone__icontains=dig)
    for c in ag.filter(qag).order_by("nome")[:lim]:
        _add("zap", c.nome or "", c.telefone or "", c.jid)

    if dig and len(dig) >= 10:
        _add("número", "", dig, telefone_para_jid(dig))
    return out


def gravar_agenda_zap(itens: list, *, pedido_id: int = 0) -> int:
    n = 0
    if not isinstance(itens, list):
        itens = []
    for raw in itens[:500]:
        if not isinstance(raw, dict):
            continue
        jid = str(raw.get("jid") or "").strip()[:80]
        if not jid.endswith("@s.whatsapp.net"):
            continue
        tel = str(raw.get("telefone") or jid_para_telefone(jid))[:32]
        nome = str(raw.get("nome") or "")[:120]
        WhatsAppAgendaContatoAgro.objects.update_or_create(
            jid=jid, defaults={"telefone": tel, "nome": nome}
        )
        n += 1
    if pedido_id:
        marcar_pedido(int(pedido_id), ok=True)
    return n


def listar_pedidos_pendentes(limit: int = 3) -> list[dict]:
    lim = max(1, min(int(limit or 3), 8))
    qs = WhatsAppPontePedidoAgro.objects.filter(
        status=WhatsAppPontePedidoAgro.STATUS_PENDENTE
    ).order_by("id")[:lim]
    out = []
    for p in qs:
        payload = p.payload if isinstance(p.payload, dict) else {}
        item = {
            "id": int(p.pk),
            "tipo": p.tipo,
            "jid": p.jid or "",
        }
        item.update(payload)
        out.append(item)
    return out


def marcar_pedido(pedido_id: int, *, ok: bool, erro: str = "") -> None:
    try:
        p = WhatsAppPontePedidoAgro.objects.get(pk=int(pedido_id))
    except (WhatsAppPontePedidoAgro.DoesNotExist, TypeError, ValueError):
        return
    p.status = WhatsAppPontePedidoAgro.STATUS_OK if ok else WhatsAppPontePedidoAgro.STATUS_ERRO
    p.erro = (erro or "")[:200]
    p.save(update_fields=["status", "erro"])


def pedir_agenda_zap() -> tuple[WhatsAppPontePedidoAgro | None, str]:
    recente = WhatsAppPontePedidoAgro.objects.filter(
        tipo=WhatsAppPontePedidoAgro.TIPO_CONTATOS,
        status=WhatsAppPontePedidoAgro.STATUS_PENDENTE,
        criado_em__gte=timezone.now() - timedelta(seconds=90),
    ).first()
    if recente:
        return recente, ""
    p = WhatsAppPontePedidoAgro.objects.create(tipo=WhatsAppPontePedidoAgro.TIPO_CONTATOS)
    return p, ""


def pedir_historico_conversa(conversa_id: int) -> tuple[WhatsAppPontePedidoAgro | None, str]:
    try:
        conv = WhatsAppConversaAgro.objects.get(pk=int(conversa_id))
    except (WhatsAppConversaAgro.DoesNotExist, TypeError, ValueError):
        return None, "Conversa não encontrada."
    recente = WhatsAppPontePedidoAgro.objects.filter(
        tipo=WhatsAppPontePedidoAgro.TIPO_HISTORICO,
        jid=conv.jid,
        criado_em__gte=timezone.now() - timedelta(seconds=90),
    ).exclude(status=WhatsAppPontePedidoAgro.STATUS_ERRO).first()
    if recente:
        return recente, ""
    oldest = (
        WhatsAppMensagemAgro.objects.filter(conversa=conv)
        .exclude(wa_id="")
        .order_by("criado_em", "id")
        .first()
    )
    if oldest is None:
        return None, "Ainda não tem mensagem deste chat depois do QR. Sem isso o Zap não libera o passado."
    ts_ms = int(oldest.criado_em.timestamp() * 1000) if oldest.criado_em else int(timezone.now().timestamp() * 1000)
    p = WhatsAppPontePedidoAgro.objects.create(
        tipo=WhatsAppPontePedidoAgro.TIPO_HISTORICO,
        jid=conv.jid,
        payload={
            "count": MAX_HIST_MSGS,
            "oldest_id": oldest.wa_id,
            "oldest_from_me": oldest.direcao != WhatsAppMensagemAgro.DIRECAO_IN,
            "oldest_ts": ts_ms,
        },
    )
    return p, ""


def pedir_codigo_pareamento(telefone: str) -> tuple[WhatsAppPontePedidoAgro | None, str]:
    d = re.sub(r"\D+", "", telefone or "")
    if len(d) in (10, 11):
        d = "55" + d
    if len(d) < 12 or len(d) > 13:
        return None, "Número da loja com DDD (ex.: 13 9xxxx-xxxx)."
    recente = WhatsAppPontePedidoAgro.objects.filter(
        tipo=WhatsAppPontePedidoAgro.TIPO_PAIRING,
        status=WhatsAppPontePedidoAgro.STATUS_PENDENTE,
        criado_em__gte=timezone.now() - timedelta(seconds=20),
    ).first()
    if recente:
        return recente, ""
    p = WhatsAppPontePedidoAgro.objects.create(
        tipo=WhatsAppPontePedidoAgro.TIPO_PAIRING,
        payload={"telefone": d},
    )
    return p, ""


def excluir_conversa(conversa_id: int) -> tuple[bool, str]:
    try:
        cid = int(conversa_id)
    except (TypeError, ValueError):
        return False, "Conversa inválida."
    n, _ = WhatsAppConversaAgro.objects.filter(pk=cid).delete()
    if not n:
        return False, "Conversa não encontrada."
    return True, ""


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


def atualizar_ponte(
    *,
    status: str,
    qr: str = "",
    numero: str = "",
    aviso: str = "",
    pairing_code: str = "",
) -> WhatsAppPonteEstadoAgro:
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
    if st == WhatsAppPonteEstadoAgro.STATUS_CONECTADO:
        obj.pairing_code = ""
    elif pairing_code:
        obj.pairing_code = str(pairing_code).replace(" ", "")[:16]
    obj.aviso = (aviso or "")[:240]
    obj.save()
    return obj


def listar_saida_pendente(limit: int = 20) -> list[dict]:
    lim = max(1, min(int(limit or 20), 50))
    qs = (
        WhatsAppMensagemAgro.objects.select_related("conversa")
        .filter(pendente_envio=True)
        .filter(Q(liberar_envio_em__isnull=True) | Q(liberar_envio_em__lte=timezone.now()))
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


def marcar_lidas(conversa_id: int) -> None:
    WhatsAppConversaAgro.objects.filter(pk=int(conversa_id)).update(nao_lidas=0)
