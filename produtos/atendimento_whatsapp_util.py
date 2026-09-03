"""Atendimento WhatsApp — roteamento Centro/Vila + fila da ponte QR."""
from __future__ import annotations

import base64
import hmac
import json
import re
import unicodedata
import uuid
from datetime import datetime, timedelta, timezone as dt_timezone
from pathlib import Path

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
    WhatsAppStatusAgro,
)

MAX_NOVOS_DIA = 20
MAX_HIST_MSGS = 40
MAX_MIDIA_BYTES = 6_000_000
MAX_SAIDA_MIDIA_BYTES = 3_000_000
DIAS_HISTORICO = 7
STATUS_HORAS = 24
# Msg com ts mais velho que isso não dispara bot (anti-replay no reconnect).
BOT_AO_VIVO_SEG = 5 * 60

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
    from produtos.atendimento_whatsapp_bot_config import BOT_DEFAULT, _casa_palavra, _palavras, cfg_flag

    c = cfg if cfg is not None else BOT_DEFAULT
    if not cfg_flag(c, "fiado_ligado"):
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


def _telefone_real(s: str) -> str:
    """Só número de celular (10–13 dígitos). ID @lid não é telefone."""
    j = (s or "").strip().lower()
    if j.endswith("@lid"):
        return ""
    d = re.sub(r"\D+", "", s or "")
    if 10 <= len(d) <= 13:
        return d
    return ""


def aplicar_nome_cadastro(conv: WhatsAppConversaAgro, *, perfil: str = "", cfg: dict | None = None) -> None:
    from produtos.atendimento_whatsapp_bot_config import BOT_DEFAULT

    tel = _telefone_real(conv.telefone) or _telefone_real(conv.jid)
    raw = str((cfg or {}).get("nome_fontes") or BOT_DEFAULT.get("nome_fontes") or "")
    ordem = []
    for p in raw.replace(";", ",").split(","):
        k = p.strip().lower()
        if k in ("cadastro", "agenda", "perfil", "telefone") and k not in ordem:
            ordem.append(k)
    for k in ("cadastro", "agenda", "perfil", "telefone"):
        if k not in ordem:
            ordem.append(k)
    for fonte in ordem:
        hit = ""
        if fonte == "cadastro" and tel:
            cli = cliente_agro_por_whatsapp_flex(tel)
            if cli is not None and cli != "varios":
                hit = (getattr(cli, "nome", None) or "").strip()
        elif fonte == "agenda":
            ag = WhatsAppAgendaContatoAgro.objects.filter(jid=conv.jid).only("nome").first()
            if ag is None and (conv.jid_lid or ""):
                ag = WhatsAppAgendaContatoAgro.objects.filter(jid=conv.jid_lid).only("nome").first()
            hit = ((ag.nome if ag else "") or "").strip()
        elif fonte == "perfil":
            hit = (perfil or "").strip()
        elif fonte == "telefone":
            hit = tel
        if not hit:
            continue
        if fonte != "telefone" and _nome_parece_telefone(hit, tel):
            continue
        conv.nome = hit[:120]
        return


def aplicar_nome_agenda(conv: WhatsAppConversaAgro) -> None:
    aplicar_nome_cadastro(conv)


def _nome_parece_telefone(nome: str, telefone: str) -> bool:
    n = re.sub(r"\D+", "", nome or "")
    t = re.sub(r"\D+", "", telefone or "")
    if not n or not t:
        return False
    return n == t or n.endswith(t) or t.endswith(n)


def _ext_midia(tipo: str, mime: str, nome: str) -> str:
    n = (nome or "").lower()
    for ext in (".jpg", ".jpeg", ".png", ".webp", ".ogg", ".opus", ".mp3", ".m4a", ".mp4", ".webm"):
        if n.endswith(ext):
            return ext if ext != ".jpeg" else ".jpg"
    m = (mime or "").lower()
    if "png" in m:
        return ".png"
    if "webp" in m:
        return ".webp"
    if "webm" in m:
        return ".webm"
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


def _b64_para_bytes(midia_b64: str, *, teto: int = MAX_MIDIA_BYTES) -> tuple[bytes | None, str]:
    raw_b64 = (midia_b64 or "").strip()
    if not raw_b64:
        return None, ""
    if "," in raw_b64 and raw_b64[:5].lower() == "data:":
        raw_b64 = raw_b64.split(",", 1)[1]
    try:
        raw = base64.b64decode(raw_b64)
    except Exception:
        return None, "Arquivo inválido."
    if not raw:
        return None, "Arquivo vazio."
    if len(raw) > int(teto):
        return None, "Foto ou áudio grande demais (máximo 3 MB). Mande um arquivo menor."
    return raw, ""


def anexar_midia(
    msg: WhatsAppMensagemAgro,
    *,
    tipo_midia: str = "",
    midia_b64: str = "",
    midia_raw: bytes | None = None,
    mime: str = "",
    nome_arquivo: str = "",
    teto: int = MAX_MIDIA_BYTES,
) -> str:
    tipo = (tipo_midia or "").strip().lower()[:16]
    if tipo:
        msg.tipo_midia = tipo
    raw = midia_raw
    if raw is None:
        raw, err = _b64_para_bytes(midia_b64, teto=teto)
        if err:
            return err
    if not raw:
        return ""
    if len(raw) > int(teto):
        return "Foto ou áudio grande demais (máximo 3 MB). Mande um arquivo menor."
    ext = _ext_midia(tipo, mime, nome_arquivo)
    fname = f"{uuid.uuid4().hex[:16]}{ext}"
    msg.arquivo.save(fname, ContentFile(raw), save=False)
    return ""


def jid_eh_chat_privado(jid: str) -> bool:
    """Só conversa 1-a-1 — celular (@s.whatsapp.net) ou ID LID do Zap."""
    j = (jid or "").strip().lower()
    if any(x in j for x in ("@g.us", "@newsletter", "@broadcast")):
        return False
    num = jid_para_telefone(j)
    if not num:
        return False
    if j.endswith("@lid"):
        return 6 <= len(num) <= 22
    if not j.endswith("@s.whatsapp.net"):
        return False
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
    nao = int(c.nao_lidas or 0)
    if nao > 0:
        status = "nova"
    elif bool(c.aguardando_loja):
        status = "espera"
    else:
        status = "ok"
    return {
        "id": int(c.pk),
        "jid": c.jid,
        "telefone": c.telefone or "",
        "nome": c.nome or "",
        "loja": c.loja,
        "nao_lidas": nao,
        "aguardando_loja": bool(c.aguardando_loja),
        "status": status,
        "ultima_preview": c.ultima_preview or "",
        "ultima_em": ult.isoformat() if ult else "",
        "hora": ult_l.strftime("%H:%M") if ult_l else "",
        "data": ult_l.strftime("%d/%m") if ult_l else "",
    }


def ficha_contato_conversa(conversa_id: int) -> tuple[dict | None, str]:
    """Dados do chat + cadastro Agro (quando o número casa)."""
    from produtos.cliente_whatsapp_util import cliente_agro_por_whatsapp_flex

    try:
        conv = WhatsAppConversaAgro.objects.get(pk=int(conversa_id))
    except (WhatsAppConversaAgro.DoesNotExist, TypeError, ValueError):
        return None, "Conversa não encontrada."
    tel = _telefone_real(conv.telefone) or _telefone_real(conv.jid)
    loja_lbl = {
        WhatsAppConversaAgro.LOJA_PENDENTE: "Fila",
        WhatsAppConversaAgro.LOJA_CENTRO: "Centro",
        WhatsAppConversaAgro.LOJA_VILA: "Vila Elias",
    }.get(conv.loja or "", conv.loja or "")
    ag = WhatsAppAgendaContatoAgro.objects.filter(jid=conv.jid).only("nome").first()
    if ag is None and (conv.jid_lid or ""):
        ag = WhatsAppAgendaContatoAgro.objects.filter(jid=conv.jid_lid).only("nome").first()
    cli = None
    varios = False
    if tel:
        hit = cliente_agro_por_whatsapp_flex(tel)
        if hit == "varios":
            varios = True
        elif hit is not None:
            cli = hit
    out = {
        "conversa_id": int(conv.pk),
        "nome": (conv.nome or "")[:120],
        "telefone": tel or (conv.telefone or ""),
        "jid": conv.jid or "",
        "loja": conv.loja or "",
        "loja_label": loja_lbl,
        "agenda_nome": ((ag.nome if ag else "") or "")[:120],
        "cadastro": None,
        "cadastro_varios": varios,
    }
    if cli is not None:
        out["cadastro"] = {
            "id": int(cli.pk),
            "nome": (cli.nome or "")[:200],
            "whatsapp": (cli.whatsapp or "")[:32],
            "cpf": (cli.cpf or "")[:14],
            "endereco": (cli.endereco or "")[:300],
            "cidade": (cli.cidade or "")[:120],
            "url": f"/clientes/{int(cli.pk)}/editar/",
        }
    return out, ""


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
    juntar_conversas_lid_orfas()
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
    tipo_midia: str = "",
    midia_raw: bytes | None = None,
    mime: str = "",
    nome_arquivo: str = "",
) -> WhatsAppMensagemAgro:
    agora = timezone.now()
    liberar = None
    try:
        d = int(delay_seg or 0)
    except (TypeError, ValueError):
        d = 0
    if d > 0:
        liberar = agora + timedelta(seconds=d)
    m = WhatsAppMensagemAgro(
        conversa=conversa,
        direcao=direcao,
        texto=texto[:TEXTO_MAX],
        pendente_envio=True,
        autor_nome=(autor or "")[:120],
        liberar_envio_em=liberar,
        tipo_midia=(tipo_midia or "")[:16],
    )
    if midia_raw:
        anexar_midia(
            m,
            tipo_midia=tipo_midia,
            midia_raw=midia_raw,
            mime=mime,
            nome_arquivo=nome_arquivo,
            teto=MAX_SAIDA_MIDIA_BYTES,
        )
    m.save()
    conversa.ultima_preview = _preview(texto)
    conversa.ultima_em = agora
    campos = ["ultima_preview", "ultima_em"]
    if direcao == WhatsAppMensagemAgro.DIRECAO_OUT:
        conversa.aguardando_loja = False
        campos.append("aguardando_loja")
    conversa.save(update_fields=campos)
    return m


def responder_bot(conversa: WhatsAppConversaAgro, texto: str, *, delay_seg: int = 0) -> WhatsAppMensagemAgro:
    return _enfileirar_saida(
        conversa, texto, direcao=WhatsAppMensagemAgro.DIRECAO_BOT, autor="Bot", delay_seg=delay_seg
    )


def _jid_lid(s: str) -> str:
    j = (s or "").strip().lower()
    return j[:80] if j.endswith("@lid") else ""


def _nome_chave(s: str) -> str:
    t = _sem_acento(s or "").casefold()
    t = re.sub(r"[^a-z0-9 ]+", " ", t)
    return " ".join(t.split())


def _nomes_casam(a: str, b: str) -> bool:
    if not a or not b or min(len(a), len(b)) < 8:
        return False
    return a == b or a.startswith(b + " ") or b.startswith(a + " ")


def _mapa_lid_disco() -> dict:
    p = Path(getattr(settings, "BASE_DIR", ".")) / "whatsapp_atendimento" / "lid_map.json"
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


def _telefone_cadastro_por_nome(nome: str) -> str:
    chave = _nome_chave(nome)
    if len(chave) < 8:
        return ""
    hits = []
    for cli in ClienteAgro.objects.filter(ativo=True).only("nome", "whatsapp"):
        if _nomes_casam(chave, _nome_chave(cli.nome)):
            hits.append(cli)
            if len(hits) > 1:
                return ""
    if len(hits) != 1:
        return ""
    return _telefone_real(hits[0].whatsapp)


def _telefone_mesmo(a: str, b: str) -> bool:
    x, y = _telefone_real(a), _telefone_real(b)
    if not x or not y:
        return False
    return x == y or x[-11:] == y[-11:]


@transaction.atomic
def juntar_conversas_lid_orfas() -> int:
    """Junta chat @lid com o telefone do mesmo cliente (mapa do PC ou nome)."""
    aplicar_mapa_lid(_mapa_lid_disco())
    n = 0
    lids = list(WhatsAppConversaAgro.objects.select_for_update().filter(jid__endswith="@lid"))
    if not lids:
        return 0
    phones = list(
        WhatsAppConversaAgro.objects.select_for_update().filter(jid__endswith="@s.whatsapp.net")
    )
    vivos = {p.pk: p for p in phones}
    for lc in lids:
        if not WhatsAppConversaAgro.objects.filter(pk=lc.pk).exists():
            continue
        chave_l = _nome_chave(lc.nome)
        hits = [p for p in vivos.values() if _nomes_casam(chave_l, _nome_chave(p.nome))]
        if len(hits) != 1:
            tel_cad = _telefone_cadastro_por_nome(lc.nome)
            hits = []
            if tel_cad:
                for p in vivos.values():
                    if _telefone_mesmo(p.telefone, tel_cad) or _telefone_mesmo(p.jid, tel_cad):
                        hits.append(p)
        if len(hits) != 1:
            continue
        manter = _fundir_conversas(hits[0], lc)
        vivos[manter.pk] = manter
        vivos.pop(lc.pk, None)
        n += 1
    return n


def _melhor_conversa(cands: list[WhatsAppConversaAgro]) -> WhatsAppConversaAgro:
    for c in cands:
        if str(c.jid or "").endswith("@s.whatsapp.net"):
            return c
    return cands[0]


def _fundir_conversas(manter: WhatsAppConversaAgro, sobra: WhatsAppConversaAgro) -> WhatsAppConversaAgro:
    if manter.pk == sobra.pk:
        return manter
    if str(sobra.jid or "").endswith("@s.whatsapp.net") and not str(manter.jid or "").endswith(
        "@s.whatsapp.net"
    ):
        manter, sobra = sobra, manter
    WhatsAppMensagemAgro.objects.filter(conversa=sobra).update(conversa=manter)
    if not (manter.nome or "").strip() and (sobra.nome or "").strip():
        manter.nome = sobra.nome
    if sobra.nao_lidas:
        manter.nao_lidas = int(manter.nao_lidas or 0) + int(sobra.nao_lidas or 0)
    if sobra.aguardando_loja:
        manter.aguardando_loja = True
    if sobra.ultima_em and (not manter.ultima_em or sobra.ultima_em > manter.ultima_em):
        manter.ultima_em = sobra.ultima_em
        manter.ultima_preview = sobra.ultima_preview or manter.ultima_preview
        manter.aguardando_loja = bool(sobra.aguardando_loja)
    if manter.loja == WhatsAppConversaAgro.LOJA_PENDENTE and sobra.loja != WhatsAppConversaAgro.LOJA_PENDENTE:
        manter.loja = sobra.loja
    if not (manter.telefone or "").strip() and (sobra.telefone or "").strip():
        manter.telefone = sobra.telefone
    lid_m = (manter.jid_lid or "") or _jid_lid(manter.jid)
    lid_s = (sobra.jid_lid or "") or _jid_lid(sobra.jid)
    sobra.jid_lid = None
    sobra.save(update_fields=["jid_lid"])
    manter.jid_lid = lid_m or lid_s or None
    manter.save()
    sobra.delete()
    return manter


def _achar_ou_criar_conversa(
    *, jid_n: str, telefone: str = "", nome: str = "", jid_lid: str = ""
) -> WhatsAppConversaAgro:
    tel = _telefone_real(telefone) or _telefone_real(jid_n)
    phone_jid = telefone_para_jid(tel) if tel else ""
    lid = _jid_lid(jid_lid) or _jid_lid(jid_n)
    canon = (phone_jid or lid or jid_n)[:80]
    candidatos = []
    seen: set[int] = set()

    def _add(hit: WhatsAppConversaAgro | None) -> None:
        if hit and hit.pk not in seen:
            candidatos.append(hit)
            seen.add(hit.pk)

    qs = WhatsAppConversaAgro.objects.select_for_update()
    for j in (phone_jid, lid, jid_n, canon):
        if j:
            _add(qs.filter(jid=j).first())
    if lid:
        _add(qs.filter(jid_lid=lid).first())
    if tel:
        tels = {tel}
        if len(tel) >= 11:
            tels.add(tel[-11:])
        if tel.startswith("55") and len(tel) >= 12:
            tels.add(tel[2:])
        for extra in qs.filter(telefone__in=list(tels)):
            _add(extra)
    if candidatos:
        base = _melhor_conversa(candidatos)
        for extra in candidatos:
            if extra.pk != base.pk:
                base = _fundir_conversas(base, extra)
        campos = []
        if phone_jid and base.jid != phone_jid:
            outro = qs.filter(jid=phone_jid).exclude(pk=base.pk).first()
            if outro:
                return _fundir_conversas(outro, base)
            base.jid = phone_jid
            campos.append("jid")
        if tel and base.telefone != tel:
            base.telefone = tel[:32]
            campos.append("telefone")
        if lid and (base.jid_lid or "") != lid:
            outro_lid = qs.filter(jid_lid=lid).exclude(pk=base.pk).first()
            if outro_lid:
                return _fundir_conversas(base, outro_lid)
            base.jid_lid = lid
            campos.append("jid_lid")
        if nome and (not base.nome or _nome_parece_telefone(base.nome, base.telefone)):
            base.nome = nome[:120]
            campos.append("nome")
        if campos:
            base.save(update_fields=campos)
        return base
    conv, _criada = WhatsAppConversaAgro.objects.select_for_update().get_or_create(
        jid=canon,
        defaults={
            "telefone": (tel or "")[:32],
            "nome": (nome or "")[:120],
            "jid_lid": lid or None,
        },
    )
    campos = []
    if tel and not conv.telefone:
        conv.telefone = tel[:32]
        campos.append("telefone")
    if lid and not conv.jid_lid:
        conv.jid_lid = lid
        campos.append("jid_lid")
    if campos:
        conv.save(update_fields=campos)
    return conv


@transaction.atomic
def aplicar_mapa_lid(pares: dict) -> int:
    """Recebe { 'xxx@lid': '5513...@s.whatsapp.net' } e junta os chats."""
    if not isinstance(pares, dict):
        return 0
    n = 0
    for lid_raw, phone_raw in pares.items():
        lid = _jid_lid(str(lid_raw or ""))
        tel = _telefone_real(str(phone_raw or ""))
        if not lid or not tel:
            continue
        _achar_ou_criar_conversa(jid_n=telefone_para_jid(tel), telefone=tel, jid_lid=lid)
        n += 1
    return n


def _preencher_msg_bot(txt: str, cfg: dict, conv: WhatsAppConversaAgro | None = None) -> str:
    empresa = str((cfg or {}).get("nome_empresa") or "")
    nome = ((conv.nome if conv else "") or "").strip()
    tel = _telefone_real(conv.telefone) if conv else ""
    if nome and _nome_parece_telefone(nome, tel):
        nome = ""
    t = str(txt or "")
    t = t.replace("{empresa}", empresa)
    t = t.replace("{cliente}", nome or "cliente")
    t = t.replace("{nome}", nome or "cliente")
    return t[:TEXTO_MAX]


def _enviar_lote_bot(conversa: WhatsAppConversaAgro, textos: list[str], cfg: dict) -> None:
    from produtos.atendimento_whatsapp_bot_config import delays_bot

    msgs = [str(x).strip() for x in textos if str(x or "").strip()]
    if not msgs:
        return
    ds = delays_bot(cfg, len(msgs))
    for i, txt in enumerate(msgs):
        t = _preencher_msg_bot(txt, cfg, conversa)
        responder_bot(conversa, t, delay_seg=ds[i] if i < len(ds) else 0)


def _texto_eh_so_midia(texto: str, tipo_midia: str) -> bool:
    tipo = (tipo_midia or "").strip().lower()
    if tipo not in ("image", "audio", "sticker", "video", "document"):
        return False
    t = (texto or "").strip().lower()
    return t in ("", "[imagem]", "[áudio]", "[audio]", "[figurinha]", "[vídeo]", "[video]", "[arquivo]")


def _pode_aviso_fora(conv: WhatsAppConversaAgro, cfg: dict, *, texto: str = "", tipo_midia: str = "") -> bool:
    from produtos.atendimento_whatsapp_bot_config import cfg_flag

    if not cfg_flag(cfg, "aviso_fora_ligado"):
        return False
    if cfg_flag(cfg, "aviso_fora_so_texto") and _texto_eh_so_midia(texto, tipo_midia):
        return False
    last = conv.aviso_fora_em
    if cfg_flag(cfg, "aviso_fora_uma_vez") and last:
        return False
    try:
        mins = max(0, min(1440, int(cfg.get("aviso_fora_minutos") or 0)))
    except (TypeError, ValueError):
        mins = 60
    if mins > 0 and last and timezone.now() - last < timedelta(minutes=mins):
        return False
    return True


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
    telefone: str = "",
    jid_lid: str = "",
) -> tuple[WhatsAppMensagemAgro | None, str]:
    jid_n = (jid or "").strip()
    bruto = f"{jid_n} {jid_lid or ''}".lower()
    if any(x in bruto for x in ("@broadcast", "@g.us", "@newsletter", "status@")):
        return None, "ignorado"
    lid = _jid_lid(jid_lid) or _jid_lid(jid_n)
    tel_limpo = _telefone_real(telefone) or _telefone_real(jid_n)
    if lid and not tel_limpo:
        pares = _mapa_lid_disco()
        tel_limpo = _telefone_real(str(pares.get(lid) or pares.get(jid_n) or ""))
    juntar_conversas_lid_orfas()
    tel_extra = telefone_para_jid(tel_limpo) if tel_limpo else ""
    if tel_extra:
        jid_n = tel_extra
    if not jid_eh_chat_privado(jid_n) and not lid:
        return None, "ignorado"
    if lid and not jid_eh_chat_privado(jid_n):
        jid_n = lid
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
    # Rede/reconnect do Zap manda msgs antigas como “ao vivo” → bot disparava sozinho.
    if not historico and ts not in (None, "", 0, "0"):
        idade = (timezone.now() - quando).total_seconds()
        if idade > BOT_AO_VIVO_SEG:
            historico = True
    if historico:
        limite = timezone.now() - timedelta(days=DIAS_HISTORICO)
        if quando < limite:
            return None, "ignorado"

    conv = _achar_ou_criar_conversa(jid_n=jid_n, telefone=tel_limpo, nome=nome, jid_lid=lid)
    if not conv.telefone and tel_limpo:
        conv.telefone = tel_limpo[:32]
    if not _telefone_real(conv.telefone):
        tel_cad = _telefone_cadastro_por_nome(conv.nome) or _telefone_cadastro_por_nome(nome)
        if tel_cad:
            conv.telefone = tel_cad[:32]
            tel_limpo = tel_cad
            phone_jid = telefone_para_jid(tel_cad)
            outro = (
                WhatsAppConversaAgro.objects.select_for_update()
                .filter(jid=phone_jid)
                .exclude(pk=conv.pk)
                .first()
            )
            if outro:
                conv = _fundir_conversas(outro, conv)
    from produtos.atendimento_whatsapp_bot_config import carregar_bot, cfg_flag, fora_do_horario

    cfg = carregar_bot()
    aplicar_nome_cadastro(conv, perfil=nome, cfg=cfg)

    if de_mim and not historico:
        corte = timezone.now() - timedelta(seconds=45)
        eco = WhatsAppMensagemAgro.objects.filter(
            conversa=conv,
            direcao__in=[WhatsAppMensagemAgro.DIRECAO_OUT, WhatsAppMensagemAgro.DIRECAO_BOT],
            criado_em__gte=corte,
        )
        if tipo_n in ("image", "audio", "sticker") and eco.filter(tipo_midia=tipo_n).exists():
            return None, "duplicada"
        if tipo_n not in ("image", "audio", "sticker") and eco.filter(texto=t).exists():
            return None, "duplicada"

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
            conv.aguardando_loja = True
        else:
            conv.aguardando_loja = False
    elif not conv.ultima_em:
        conv.ultima_em = quando

    campos_base = [
        "nome",
        "telefone",
        "ultima_preview",
        "ultima_em",
        "nao_lidas",
        "jid_lid",
        "aviso_fora_em",
        "aguardando_loja",
    ]
    if not fora_do_horario(cfg) and conv.aviso_fora_em:
        conv.aviso_fora_em = None
    fone = _telefone_real(conv.telefone) or tel_limpo or _telefone_real(conv.jid)
    if not fone:
        fone = _telefone_cadastro_por_nome(conv.nome)
    eh_fiado = interpretar_consulta_fiado(t, cfg)

    def _ok_loja(escolha: str) -> str:
        if escolha == str(cfg.get("loja2_id") or "vila"):
            return str(cfg.get("msg_ok_loja2") or MSG_OK_VILA)
        return str(cfg.get("msg_ok_loja1") or MSG_OK_CENTRO)

    def _menu_textos() -> list[str]:
        out = []
        if cfg_flag(cfg, "enviar_boas_vindas"):
            bv = str(cfg.get("msg_boas_vindas") or "").strip()
            if bv:
                out.append(bv)
        out.append(str(cfg.get("msg_menu") or MSG_MENU))
        return out

    if historico or de_mim or not cfg_flag(cfg, "bot_ligado"):
        conv.save(update_fields=campos_base)
        return msg, ""

    lote: list[str] = []
    if fora_do_horario(cfg):
        if eh_fiado and cfg_flag(cfg, "fiado_ligado"):
            conv.save(update_fields=campos_base)
            _enviar_lote_bot(conv, [montar_texto_fiado(fone, cfg)], cfg)
            return msg, ""
        if _pode_aviso_fora(conv, cfg, texto=t, tipo_midia=tipo_n):
            fh = str(cfg.get("msg_fora_horario") or "").strip()
            if fh:
                lote.append(fh)
                conv.aviso_fora_em = timezone.now()
        if not cfg_flag(cfg, "ainda_atende_fora"):
            conv.save(update_fields=campos_base)
            _enviar_lote_bot(conv, lote, cfg)
            return msg, ""

    if not cfg_flag(cfg, "separar_lojas"):
        campos = list(campos_base)
        if conv.loja == WhatsAppConversaAgro.LOJA_PENDENTE:
            conv.loja = WhatsAppConversaAgro.LOJA_CENTRO
            campos.append("loja")
        if eh_fiado and cfg_flag(cfg, "fiado_ligado"):
            conv.save(update_fields=campos)
            _enviar_lote_bot(conv, [montar_texto_fiado(fone, cfg)], cfg)
            return msg, ""
        if cfg_flag(cfg, "enviar_boas_vindas") and not conv.menu_enviado:
            bv = str(cfg.get("msg_boas_vindas") or "").strip()
            if bv:
                lote.append(bv)
            conv.menu_enviado = True
            campos.append("menu_enviado")
        conv.save(update_fields=campos)
        if lote:
            _enviar_lote_bot(conv, lote, cfg)
        return msg, ""

    escolha = interpretar_loja(t, cfg) if conv.loja == WhatsAppConversaAgro.LOJA_PENDENTE else ""
    ordem_loja_primeiro = str(cfg.get("ordem") or "") == "loja_primeiro"

    def _fiado_fluxo() -> None:
        lote.append(montar_texto_fiado(fone, cfg))
        if conv.loja == WhatsAppConversaAgro.LOJA_PENDENTE and cfg_flag(cfg, "fiado_manda_menu"):
            if not conv.menu_enviado:
                conv.menu_enviado = True
                lote.extend(_menu_textos())

    if conv.loja == WhatsAppConversaAgro.LOJA_PENDENTE:
        if ordem_loja_primeiro and escolha:
            conv.loja = escolha
            conv.save(update_fields=campos_base + ["loja"])
            lote.append(_ok_loja(escolha))
            if cfg_flag(cfg, "ausencia_ligada"):
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
            if cfg_flag(cfg, "ausencia_ligada"):
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
        if cfg_flag(cfg, "repetir_menu"):
            lote.append(str(cfg.get("msg_pedir_de_novo") or MSG_PEDIR_DE_NOVO))
        _enviar_lote_bot(conv, lote, cfg)
        return msg, ""

    if eh_fiado and cfg_flag(cfg, "fiado_ligado"):
        conv.save(update_fields=campos_base)
        lote.append(montar_texto_fiado(fone, cfg))
        _enviar_lote_bot(conv, lote, cfg)
        return msg, ""

    conv.save(update_fields=campos_base)
    if lote:
        _enviar_lote_bot(conv, lote, cfg)
    return msg, ""


def enviar_loja(
    *,
    conversa_id: int,
    texto: str,
    autor: str = "",
    tipo_midia: str = "",
    midia_b64: str = "",
    mime: str = "",
    nome_arquivo: str = "",
) -> tuple[WhatsAppMensagemAgro | None, str]:
    t = str(texto or "").strip()
    tipo_n = (tipo_midia or "").strip().lower()[:16]
    if tipo_n and tipo_n not in ("image", "audio"):
        return None, "Só foto ou áudio."
    raw = None
    if midia_b64 or tipo_n:
        raw, err = _b64_para_bytes(midia_b64, teto=MAX_SAIDA_MIDIA_BYTES)
        if err:
            return None, err
        if tipo_n and raw is None:
            return None, "Arquivo da foto/áudio não chegou."
    if not t and raw is None:
        return None, "Digite uma mensagem ou envie foto/áudio."
    if not t and raw is not None:
        t = "[imagem]" if tipo_n == "image" else "[áudio]"
    if len(t) > TEXTO_MAX:
        return None, f"Máximo {TEXTO_MAX} caracteres."
    try:
        conv = WhatsAppConversaAgro.objects.get(pk=int(conversa_id))
    except (WhatsAppConversaAgro.DoesNotExist, TypeError, ValueError):
        return None, "Conversa não encontrada."
    if conv.loja == WhatsAppConversaAgro.LOJA_PENDENTE and conv.origem_abertura != "loja":
        from produtos.atendimento_whatsapp_bot_config import carregar_bot, cfg_flag

        if cfg_flag(carregar_bot(), "separar_lojas"):
            return None, "Cliente ainda não escolheu a loja."
        conv.loja = WhatsAppConversaAgro.LOJA_CENTRO
        conv.save(update_fields=["loja"])
    m = _enfileirar_saida(
        conv,
        t,
        direcao=WhatsAppMensagemAgro.DIRECAO_OUT,
        autor=autor,
        tipo_midia=tipo_n,
        midia_raw=raw,
        mime=mime,
        nome_arquivo=nome_arquivo,
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
def abrir_conversa_busca(*, telefone: str, nome: str = "", jid: str = "") -> tuple[WhatsAppConversaAgro | None, str]:
    raw_jid = (jid or "").strip()
    lid = _jid_lid(raw_jid) or _jid_lid(telefone)
    phone_jid = telefone_para_jid(telefone)
    if not phone_jid and raw_jid.endswith("@s.whatsapp.net"):
        phone_jid = raw_jid[:80]
    if lid and not phone_jid:
        phone_jid = _phone_jid_de_lid(lid)
    if not phone_jid and lid:
        conv = (
            WhatsAppConversaAgro.objects.select_for_update()
            .filter(Q(jid_lid=lid) | Q(jid=lid[:80]))
            .first()
        )
        if conv is None:
            if _novos_loja_24h() >= MAX_NOVOS_DIA:
                return None, "Limite de conversas novas hoje (20)."
            conv = WhatsAppConversaAgro.objects.create(
                jid=lid[:80],
                jid_lid=lid[:80],
                telefone="",
                nome=(nome or "")[:120],
                loja=WhatsAppConversaAgro.LOJA_PENDENTE,
                menu_enviado=True,
                origem_abertura="loja",
            )
        if nome and not conv.nome:
            conv.nome = nome[:120]
        aplicar_nome_cadastro(conv)
        aplicar_nome_agenda(conv)
        conv.save(update_fields=["nome", "telefone", "jid_lid"])
        return conv, ""
    jid_n = phone_jid
    if not jid_n:
        return None, "Número inválido."
    conv = WhatsAppConversaAgro.objects.select_for_update().filter(jid=jid_n[:80]).first()
    if conv is None:
        if _novos_loja_24h() >= MAX_NOVOS_DIA:
            return None, "Limite de conversas novas hoje (20)."
        conv = WhatsAppConversaAgro.objects.create(
            jid=jid_n[:80],
            telefone=jid_para_telefone(jid_n),
            nome=(nome or "")[:120],
            loja=WhatsAppConversaAgro.LOJA_PENDENTE,
            menu_enviado=True,
            origem_abertura="loja",
        )
    if nome and not conv.nome:
        conv.nome = nome[:120]
    if not conv.telefone:
        conv.telefone = jid_para_telefone(jid_n)
    aplicar_nome_cadastro(conv)
    aplicar_nome_agenda(conv)
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
        conv = WhatsAppConversaAgro.objects.filter(Q(jid=j) | Q(jid_lid=j)).only("id", "loja").first()
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
        pedir_agenda_zap(t)

    cli = ClienteAgro.objects.filter(ativo=True).exclude(whatsapp="")
    q = Q(nome__icontains=t)
    if dig:
        q |= Q(whatsapp__icontains=dig)
    for c in cli.filter(q).order_by("nome")[:lim]:
        _add("cadastro", c.nome or "", c.whatsapp or "", telefone_para_jid(c.whatsapp or ""))

    convs = WhatsAppConversaAgro.objects.filter(Q(nome__icontains=t) | Q(telefone__icontains=dig or t))
    for c in convs.order_by("-ultima_em")[:lim]:
        _add("conversa", c.nome or "", c.telefone or "", c.jid)

    ag = WhatsAppAgendaContatoAgro.objects.exclude(nome="").order_by("nome")
    tn = _sem_acento(t.lower())
    for c in ag[:2500]:
        if len(out) >= lim:
            break
        cn = _sem_acento((c.nome or "").lower())
        if (tn and tn in cn) or (dig and dig in re.sub(r"\D+", "", c.telefone or "")):
            _add("zap", c.nome or "", c.telefone or "", c.jid)

    if dig and len(dig) >= 10:
        _add("número", "", dig, telefone_para_jid(dig))
    return out


def _phone_jid_de_lid(lid: str) -> str:
    lid_n = _jid_lid(lid)
    if not lid_n:
        return ""
    conv = (
        WhatsAppConversaAgro.objects.filter(jid_lid=lid_n)
        .exclude(jid__endswith="@lid")
        .only("jid")
        .first()
    )
    if conv and (conv.jid or "").endswith("@s.whatsapp.net"):
        return conv.jid
    try:
        data = json.loads(_mapa_lid_disco().read_text(encoding="utf-8"))
    except Exception:
        data = {}
    if isinstance(data, dict):
        for k, v in data.items():
            if _jid_lid(str(k)) == lid_n:
                j = telefone_para_jid(str(v))
                if j:
                    return j
    return ""


def _vcard_desdobrar(texto: str) -> str:
    t = re.sub(r"=\r?\n", "", texto or "")
    linhas: list[str] = []
    for ln in t.replace("\r\n", "\n").replace("\r", "\n").split("\n"):
        if linhas and (ln.startswith(" ") or ln.startswith("\t")):
            linhas[-1] += ln[1:]
        else:
            linhas.append(ln)
    return "\n".join(linhas)


def _vcard_decode_valor(valor: str, meta: str) -> str:
    v = (valor or "").strip()
    m = (meta or "").upper()
    if "QUOTED-PRINTABLE" in m:
        try:
            import quopri

            v = quopri.decodestring(v.encode("utf-8", errors="replace")).decode("utf-8", errors="replace")
        except Exception:
            pass
    return re.sub(r"\s+", " ", v).strip()


def _vcard_tel_digits(raw: str) -> str:
    d = re.sub(r"\D+", "", raw or "")
    while d.startswith("0") and len(d) > 11:
        d = d[1:]
    if d.startswith("55") and len(d) >= 12:
        return d
    if len(d) in (10, 11):
        return "55" + d
    if 12 <= len(d) <= 15:
        return d
    return ""


def parse_agenda_vcard(texto: str) -> list[dict]:
    """Extrai nome + telefone de um .vcf (agenda do celular)."""
    bloco = _vcard_desdobrar(texto)
    itens: list[dict] = []
    seen: set[str] = set()
    cards = re.split(r"(?i)BEGIN:VCARD", bloco)
    for card in cards:
        if not card.strip():
            continue
        nome = ""
        tels: list[tuple[int, str]] = []
        for ln in card.split("\n"):
            if ":" not in ln:
                continue
            meta, val = ln.split(":", 1)
            key = meta.split(";", 1)[0].upper()
            mu = meta.upper()
            if key == "FN":
                nome = _vcard_decode_valor(val, meta) or nome
            elif key == "N" and not nome:
                parts = [_vcard_decode_valor(p, meta) for p in val.split(";")]
                nome = " ".join(p for p in parts if p).strip()
            elif key == "TEL":
                dig = _vcard_tel_digits(val)
                if not dig:
                    continue
                score = 0
                if "WHATSAPP" in mu:
                    score += 30
                if "PREF" in mu:
                    score += 10
                if "CELL" in mu or "MOBILE" in mu or "VOICE" in mu:
                    score += 5
                tels.append((score, dig))
        if not tels:
            continue
        tels.sort(key=lambda x: (-x[0], x[1]))
        dig = tels[0][1]
        jid = telefone_para_jid(dig)
        if not jid or jid in seen:
            continue
        seen.add(jid)
        itens.append({"jid": jid, "nome": (nome or "")[:120], "telefone": dig[:32]})
        if len(itens) >= 5000:
            break
    return itens


def importar_agenda_vcard(texto: str) -> dict:
    itens = parse_agenda_vcard(texto)
    n = 0
    for i in range(0, len(itens), 500):
        n += gravar_agenda_zap(itens[i : i + 500])
    return {"ok": True, "lidos": len(itens), "gravados": n}


def gravar_agenda_zap(itens: list, *, pedido_id: int = 0) -> int:
    n = 0
    if not isinstance(itens, list):
        itens = []
    for raw in itens[:2000]:
        if not isinstance(raw, dict):
            continue
        jid_raw = str(raw.get("jid") or "").strip()[:80]
        lid = _jid_lid(str(raw.get("jid_lid") or "")) or _jid_lid(jid_raw)
        tel = str(raw.get("telefone") or "")[:32]
        dig = re.sub(r"\D+", "", tel)
        phone = ""
        if dig and 10 <= len(dig) <= 13:
            phone = telefone_para_jid(tel)
        if jid_raw.endswith("@s.whatsapp.net"):
            phone = jid_raw
        if lid and not phone:
            phone = _phone_jid_de_lid(lid)
        key = (phone or lid or jid_raw)[:80]
        if not (key.endswith("@s.whatsapp.net") or key.endswith("@lid")):
            continue
        tel_ok = str(raw.get("telefone") or jid_para_telefone(key))[:32]
        if key.endswith("@lid"):
            tel_ok = str(raw.get("telefone") or "")[:32]
        nome = str(raw.get("nome") or "")[:120]
        prev = WhatsAppAgendaContatoAgro.objects.filter(jid=key).only("nome").first()
        if prev and prev.nome and not nome:
            nome = prev.nome
        WhatsAppAgendaContatoAgro.objects.update_or_create(
            jid=key, defaults={"telefone": tel_ok, "nome": nome}
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


def pedir_agenda_zap(termo: str = "") -> tuple[WhatsAppPontePedidoAgro | None, str]:
    q = (termo or "").strip()[:80]
    recente = WhatsAppPontePedidoAgro.objects.filter(
        tipo=WhatsAppPontePedidoAgro.TIPO_CONTATOS,
        status=WhatsAppPontePedidoAgro.STATUS_PENDENTE,
        criado_em__gte=timezone.now() - timedelta(seconds=8),
    ).first()
    if recente:
        prev = recente.payload if isinstance(recente.payload, dict) else {}
        if str(prev.get("q") or "") == q:
            return recente, ""
    p = WhatsAppPontePedidoAgro.objects.create(
        tipo=WhatsAppPontePedidoAgro.TIPO_CONTATOS,
        payload={"q": q} if q else {},
    )
    return p, ""


def pedir_historico_conversa(conversa_id: int) -> tuple[WhatsAppPontePedidoAgro | None, str]:
    try:
        conv = WhatsAppConversaAgro.objects.get(pk=int(conversa_id))
    except (WhatsAppConversaAgro.DoesNotExist, TypeError, ValueError):
        return None, "Conversa não encontrada."
    envio = _jid_envio(conv)
    recente = WhatsAppPontePedidoAgro.objects.filter(
        tipo=WhatsAppPontePedidoAgro.TIPO_HISTORICO,
        jid=envio,
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
        jid=envio,
        payload={
            "count": MAX_HIST_MSGS,
            "oldest_id": oldest.wa_id,
            "oldest_from_me": oldest.direcao != WhatsAppMensagemAgro.DIRECAO_IN,
            "oldest_ts": ts_ms,
            "jid_phone": (conv.jid if (conv.jid or "").endswith("@s.whatsapp.net") else "")[:80],
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


def pedir_trocar_whatsapp() -> tuple[WhatsAppPontePedidoAgro | None, str]:
    """Desliga a sessão neste PC → novo QR / código."""
    recente = WhatsAppPontePedidoAgro.objects.filter(
        tipo=WhatsAppPontePedidoAgro.TIPO_LOGOUT,
        status=WhatsAppPontePedidoAgro.STATUS_PENDENTE,
        criado_em__gte=timezone.now() - timedelta(seconds=15),
    ).first()
    if recente:
        return recente, ""
    p = WhatsAppPontePedidoAgro.objects.create(tipo=WhatsAppPontePedidoAgro.TIPO_LOGOUT, payload={})
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


@transaction.atomic
def transferir_conversa(
    conversa_id: int, loja: str, *, autor: str = ""
) -> tuple[WhatsAppConversaAgro | None, str]:
    dest = (loja or "").strip().lower()
    if dest not in (WhatsAppConversaAgro.LOJA_CENTRO, WhatsAppConversaAgro.LOJA_VILA):
        return None, "Escolha Centro ou Vila."
    try:
        conv = WhatsAppConversaAgro.objects.select_for_update().get(pk=int(conversa_id))
    except (WhatsAppConversaAgro.DoesNotExist, TypeError, ValueError):
        return None, "Conversa não encontrada."
    if conv.loja == dest:
        return None, "Já está nessa loja."
    conv.loja = dest
    conv.nao_lidas = int(conv.nao_lidas or 0) + 1
    conv.save(update_fields=["loja", "nao_lidas"])
    rotulo = "Centro" if dest == WhatsAppConversaAgro.LOJA_CENTRO else "Vila Elias"
    txt = (
        f"Seu atendimento foi passado para a loja *{rotulo}*. "
        "Eles continuam falando com você por aqui."
    )
    _enfileirar_saida(
        conv,
        txt,
        direcao=WhatsAppMensagemAgro.DIRECAO_BOT,
        autor=(autor or "Loja")[:120],
    )
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
    if st == WhatsAppPonteEstadoAgro.STATUS_CONECTADO:
        if numero:
            obj.numero = str(numero).strip()[:32]
        obj.pairing_code = ""
    else:
        obj.numero = str(numero).strip()[:32] if numero else ""
        if pairing_code:
            obj.pairing_code = str(pairing_code).replace(" ", "")[:16]
        elif st == WhatsAppPonteEstadoAgro.STATUS_DESCONECTADO:
            obj.pairing_code = ""
    obj.aviso = (aviso or "")[:240]
    obj.save()
    return obj


def _mime_saida(m: WhatsAppMensagemAgro) -> str:
    name = (getattr(m.arquivo, "name", "") or "").lower()
    tipo = (m.tipo_midia or "").strip().lower()
    if tipo == "image":
        if name.endswith(".png"):
            return "image/png"
        if name.endswith(".webp"):
            return "image/webp"
        return "image/jpeg"
    if tipo == "audio":
        if name.endswith(".webm"):
            return "audio/webm; codecs=opus"
        if name.endswith(".mp4") or name.endswith(".m4a"):
            return "audio/mp4"
        if name.endswith(".mp3"):
            return "audio/mpeg"
        return "audio/ogg; codecs=opus"
    return ""


def _arquivo_b64(m: WhatsAppMensagemAgro) -> str:
    if not m.arquivo:
        return ""
    try:
        fh = m.arquivo.open("rb")
        try:
            raw = fh.read()
        finally:
            fh.close()
    except Exception:
        return ""
    if not raw or len(raw) > MAX_SAIDA_MIDIA_BYTES:
        return ""
    return base64.b64encode(raw).decode("ascii")


def _jid_envio(conv: WhatsAppConversaAgro) -> str:
    """Zap cifra a sessão no @lid — mandar no telefone deixa o cliente em 'aguardando mensagem'."""
    lid = _jid_lid(getattr(conv, "jid_lid", "") or "") or _jid_lid(conv.jid)
    if lid:
        return lid
    return (conv.jid or "")[:80]


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
        tipo = (m.tipo_midia or "").strip().lower()
        item = {
            "id": int(m.pk),
            "jid": _jid_envio(m.conversa),
            "jid_lid": _jid_lid(getattr(m.conversa, "jid_lid", "") or "") or _jid_lid(m.conversa.jid),
            "texto": m.texto or "",
            "tipo_midia": tipo,
            "mime": _mime_saida(m) if tipo else "",
            "midia_b64": _arquivo_b64(m) if tipo in ("image", "audio") else "",
        }
        out.append(item)
    return out


def marcar_enviadas(ids: list[int], *, erro: str = "", wa_id: str = "") -> int:
    if not ids:
        return 0
    agora = timezone.now()
    if erro:
        return WhatsAppMensagemAgro.objects.filter(id__in=ids, pendente_envio=True).update(
            pendente_envio=False,
            erro_envio=str(erro)[:200],
        )
    campos = {
        "pendente_envio": False,
        "enviado_em": agora,
        "erro_envio": "",
    }
    wid = (wa_id or "").strip()[:80]
    if wid and len(ids) == 1:
        campos["wa_id"] = wid
    return WhatsAppMensagemAgro.objects.filter(id__in=ids, pendente_envio=True).update(**campos)


def marcar_lidas(conversa_id: int) -> None:
    WhatsAppConversaAgro.objects.filter(pk=int(conversa_id)).update(nao_lidas=0)


def concluir_atendimento(conversa_id: int) -> tuple[bool, str]:
    """✓ — tira a cor de espera mesmo se a última msg for do cliente."""
    try:
        cid = int(conversa_id)
    except (TypeError, ValueError):
        return False, "Conversa inválida."
    n = WhatsAppConversaAgro.objects.filter(pk=cid).update(aguardando_loja=False, nao_lidas=0)
    if not n:
        return False, "Conversa não encontrada."
    return True, ""


def _limpar_status_expirados() -> int:
    agora = timezone.now()
    n, _ = WhatsAppStatusAgro.objects.filter(expira_em__lt=agora).delete()
    return int(n or 0)


def serializar_status_item(s: WhatsAppStatusAgro) -> dict:
    criado = s.criado_em
    try:
        criado_l = timezone.localtime(criado) if criado else None
    except Exception:
        criado_l = criado
    return {
        "id": int(s.pk),
        "texto": s.texto or "",
        "tipo_midia": s.tipo_midia or "",
        "midia_url": f"/api/atendimento-whatsapp/status/midia/{int(s.pk)}/" if s.arquivo else "",
        "hora": criado_l.strftime("%H:%M") if criado_l else "",
        "criado_em": criado.isoformat() if criado else "",
    }


def listar_status(*, limit_autores: int = 40) -> list[dict]:
    """Agrupa status ativos por contato (mais recente primeiro)."""
    _limpar_status_expirados()
    agora = timezone.now()
    lim = max(1, min(int(limit_autores or 40), 80))
    rows = list(
        WhatsAppStatusAgro.objects.filter(expira_em__gte=agora)
        .order_by("-criado_em", "-id")[:400]
    )
    por_autor: dict[str, dict] = {}
    ordem: list[str] = []
    for s in rows:
        chave = (s.autor_jid or s.telefone or str(s.pk)).strip().lower()
        if not chave:
            continue
        if chave not in por_autor:
            if len(ordem) >= lim:
                continue
            ordem.append(chave)
            por_autor[chave] = {
                "autor_jid": s.autor_jid,
                "telefone": s.telefone or "",
                "nome": s.nome or "",
                "ultima_em": s.criado_em.isoformat() if s.criado_em else "",
                "itens": [],
            }
        bucket = por_autor[chave]
        if not bucket["nome"] and s.nome:
            bucket["nome"] = s.nome
        if not bucket["telefone"] and s.telefone:
            bucket["telefone"] = s.telefone
        bucket["itens"].append(serializar_status_item(s))
    out = []
    for chave in ordem:
        bucket = por_autor.get(chave)
        if not bucket or not bucket["itens"]:
            continue
        bucket["itens"].sort(key=lambda x: x.get("criado_em") or "")
        out.append(bucket)
    return out


def processar_status(
    *,
    jid: str,
    texto: str,
    nome: str = "",
    wa_id: str = "",
    ts=None,
    tipo_midia: str = "",
    midia_b64: str = "",
    mime: str = "",
    nome_arquivo: str = "",
    telefone: str = "",
    jid_lid: str = "",
) -> tuple[WhatsAppStatusAgro | None, str]:
    jid_n = (jid or "").strip()
    lid = _jid_lid(jid_lid) or _jid_lid(jid_n)
    tel_limpo = _telefone_real(telefone) or _telefone_real(jid_n)
    if lid and not tel_limpo:
        pares = _mapa_lid_disco()
        tel_limpo = _telefone_real(str(pares.get(lid) or pares.get(jid_n) or ""))
    tel_extra = telefone_para_jid(tel_limpo) if tel_limpo else ""
    if tel_extra:
        jid_n = tel_extra
    elif lid and not jid_eh_chat_privado(jid_n):
        jid_n = lid
    if not jid_eh_chat_privado(jid_n) and not lid:
        return None, "ignorado"

    t = str(texto or "").strip()
    tipo_n = (tipo_midia or "").strip().lower()[:16]
    if not t and not tipo_n:
        return None, "ignorado"
    if not tipo_n and t:
        tipo_n = "text"
    t = t[:TEXTO_MAX]
    wa = (wa_id or "").strip()[:80]
    if wa and WhatsAppStatusAgro.objects.filter(wa_id=wa).exists():
        return None, "duplicada"

    quando = _ts_aware(ts)
    if (timezone.now() - quando).total_seconds() > STATUS_HORAS * 3600:
        return None, "ignorado"

    expira = quando + timedelta(hours=STATUS_HORAS)
    if expira <= timezone.now():
        return None, "ignorado"

    nome_n = (nome or "").strip()[:120]
    if not nome_n and tel_limpo:
        ag = WhatsAppAgendaContatoAgro.objects.filter(
            Q(telefone__endswith=tel_limpo[-10:]) | Q(jid=jid_n)
        ).first()
        if ag and ag.nome:
            nome_n = ag.nome[:120]
    if not nome_n and tel_limpo:
        cli = cliente_agro_por_whatsapp_flex(tel_limpo)
        if cli and cli.nome:
            nome_n = cli.nome[:120]

    st = WhatsAppStatusAgro(
        autor_jid=jid_n[:80],
        telefone=(tel_limpo or "")[:32],
        nome=nome_n,
        jid_lid=(lid or "")[:80],
        wa_id=wa,
        texto=t,
        tipo_midia=tipo_n,
        criado_em=quando,
        expira_em=expira,
    )
    if midia_b64 or tipo_n in ("image", "video"):
        err = anexar_midia(
            st,  # type: ignore[arg-type]
            tipo_midia=tipo_n,
            midia_b64=midia_b64,
            mime=mime,
            nome_arquivo=nome_arquivo,
        )
        if err:
            return None, err
    st.save()
    return st, ""
