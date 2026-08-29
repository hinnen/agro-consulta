"""Chat interno PDV — grupo único entre todos os PCs (Centro + Vila)."""
from __future__ import annotations

from django.http import HttpRequest
from django.utils import timezone

from produtos.models import ChatLojaMensagemAgro
from produtos.pdv_deposito_util import bootstrap_deposito, normalizar_deposito, rotulo_deposito

TEXTO_MAX = 500
LISTA_MAX = 80
CANAL = ChatLojaMensagemAgro.CANAL_GERAL


def _ponto_browser(request: HttpRequest) -> str:
    try:
        from produtos.caixa_util import ponto_operacao_browser

        return str(ponto_operacao_browser(request) or "").strip().lower()
    except Exception:
        return ""


def _rotulo_ponto_curto(ponto: str, deposito: str) -> str:
    p = (ponto or "").strip().lower()
    dep = normalizar_deposito(deposito)
    loja = "Vila" if dep == "vila" else "Centro"
    if p == "gaveta":
        return f"{loja} · Gaveta"
    if p == "notebook":
        return f"{loja} · Notebook"
    if p == "vila":
        return "Vila · Caixa"
    if p == "teste":
        return f"{loja} · Teste"
    return loja


def resolver_origem_chat(request: HttpRequest) -> dict:
    boot = bootstrap_deposito(request) or {}
    dep = normalizar_deposito(boot.get("deposito") or "centro")
    ponto = _ponto_browser(request)
    return {
        "deposito": dep,
        "deposito_label": rotulo_deposito(dep),
        "ponto": ponto,
        "origem_rotulo": _rotulo_ponto_curto(ponto, dep),
    }


def resolver_autor_chat(request: HttpRequest, payload: dict | None = None) -> str:
    payload = payload or {}
    pin = str(payload.get("pin") or "").strip()
    try:
        from produtos.pdv_transf_loja_util import resolver_operador_pdv

        ok, label, _user, _err = resolver_operador_pdv(request, pin)
        if ok and label:
            return str(label).strip()[:120]
    except Exception:
        pass
    u = getattr(request, "user", None)
    if u is not None and getattr(u, "is_authenticated", False):
        nome = (u.get_full_name() or "").strip() or (
            u.get_username() if hasattr(u, "get_username") else ""
        )
        if nome:
            return str(nome).strip()[:120]
    return "Alguém"


def serializar_mensagem(m: ChatLojaMensagemAgro) -> dict:
    criado = m.criado_em
    try:
        criado_local = timezone.localtime(criado) if criado else None
    except Exception:
        criado_local = criado
    hora = criado_local.strftime("%H:%M") if criado_local else ""
    data = criado_local.strftime("%d/%m") if criado_local else ""
    return {
        "id": int(m.pk),
        "texto": m.texto or "",
        "autor": m.autor_nome or "",
        "deposito": m.deposito or "",
        "ponto": m.ponto or "",
        "origem": m.origem_rotulo or "",
        "device_id": m.device_id or "",
        "hora": hora,
        "data": data,
        "criado_em": criado.isoformat() if criado else "",
    }


def listar_mensagens(*, after_id: int = 0, limit: int = LISTA_MAX) -> list[dict]:
    lim = max(1, min(int(limit or LISTA_MAX), 200))
    qs = ChatLojaMensagemAgro.objects.filter(canal=CANAL)
    aid = int(after_id or 0)
    if aid > 0:
        qs = qs.filter(id__gt=aid).order_by("id")[:lim]
        return [serializar_mensagem(m) for m in qs]
    # Histórico recente: últimos N, ordem cronológica
    ids = list(qs.order_by("-id").values_list("id", flat=True)[:lim])
    if not ids:
        return []
    rows = list(ChatLojaMensagemAgro.objects.filter(id__in=ids).order_by("id"))
    return [serializar_mensagem(m) for m in rows]


def criar_mensagem(
    request: HttpRequest,
    *,
    texto: str,
    device_id: str = "",
    payload: dict | None = None,
) -> tuple[ChatLojaMensagemAgro | None, str]:
    t = " ".join(str(texto or "").split())
    if not t:
        return None, "Digite uma mensagem."
    if len(t) > TEXTO_MAX:
        return None, f"Máximo {TEXTO_MAX} caracteres."
    origem = resolver_origem_chat(request)
    autor = resolver_autor_chat(request, payload)
    m = ChatLojaMensagemAgro.objects.create(
        canal=CANAL,
        texto=t[:TEXTO_MAX],
        autor_nome=autor,
        deposito=origem["deposito"],
        ponto=(origem["ponto"] or "")[:32],
        origem_rotulo=(origem["origem_rotulo"] or "")[:80],
        device_id=str(device_id or "").strip()[:64],
    )
    return m, ""
