"""Depósito operacional do PDV (Centro × Vila Elias) por aparelho/sessão."""

from __future__ import annotations

from django.conf import settings
from django.http import HttpRequest, HttpResponse

SESSION_KEY = "pdv_deposito"
COOKIE_NAME = "agro_pdv_deposito"
LS_LOJA_ID = "agro_pdv_loja_id"  # espelho JS (1=Centro, 2=Vila)

DEPOSITO_CENTRO = "centro"
DEPOSITO_VILA = "vila"
DEPOSITOS_VALIDOS = frozenset({DEPOSITO_CENTRO, DEPOSITO_VILA})

LOJA_ID_PARA_DEPOSITO = {
    "1": DEPOSITO_CENTRO,
    "2": DEPOSITO_VILA,
    1: DEPOSITO_CENTRO,
    2: DEPOSITO_VILA,
}

DEPOSITO_PARA_LOJA_ID = {
    DEPOSITO_CENTRO: "1",
    DEPOSITO_VILA: "2",
}

ROTULO_DEPOSITO = {
    DEPOSITO_CENTRO: "Centro",
    DEPOSITO_VILA: "Vila Elias",
}


def normalizar_deposito(valor) -> str:
    d = str(valor or "").strip().lower()
    if d in DEPOSITOS_VALIDOS:
        return d
    if d in ("2", "vila elias", "vila_elias"):
        return DEPOSITO_VILA
    return DEPOSITO_CENTRO


def deposito_padrao_env() -> str:
    return normalizar_deposito(
        getattr(settings, "PDV_VENDA_ESTOQUE_DEPOSITO", DEPOSITO_CENTRO) or DEPOSITO_CENTRO
    )


def deposito_de_loja_id(loja_id) -> str:
    if loja_id in LOJA_ID_PARA_DEPOSITO:
        return LOJA_ID_PARA_DEPOSITO[loja_id]
    s = str(loja_id or "").strip()
    if s in LOJA_ID_PARA_DEPOSITO:
        return LOJA_ID_PARA_DEPOSITO[s]
    return normalizar_deposito(s)


def loja_id_de_deposito(deposito: str) -> str:
    return DEPOSITO_PARA_LOJA_ID.get(normalizar_deposito(deposito), "1")


def rotulo_deposito(deposito: str) -> str:
    return ROTULO_DEPOSITO.get(normalizar_deposito(deposito), "Centro")


def resolver_deposito_request(request: HttpRequest | None) -> str:
    """Sessão → cookie → env (fallback histórico = Centro)."""
    if request is None:
        return deposito_padrao_env()
    raw_sess = str(request.session.get(SESSION_KEY) or "").strip().lower()
    if raw_sess in DEPOSITOS_VALIDOS:
        return raw_sess
    try:
        cookie_raw = str(request.COOKIES.get(COOKIE_NAME) or "").strip().lower()
    except Exception:
        cookie_raw = ""
    if cookie_raw in DEPOSITOS_VALIDOS:
        return cookie_raw
    return deposito_padrao_env()


def deposito_escolhido_explicitamente(request: HttpRequest | None) -> bool:
    """True se o aparelho já gravou Centro/Vila (sessão ou cookie) — não só o padrão env."""
    if request is None:
        return False
    raw_sess = str(request.session.get(SESSION_KEY) or "").strip().lower()
    if raw_sess in DEPOSITOS_VALIDOS:
        return True
    try:
        cookie_raw = str(request.COOKIES.get(COOKIE_NAME) or "").strip().lower()
    except Exception:
        cookie_raw = ""
    return cookie_raw in DEPOSITOS_VALIDOS

def gravar_deposito_request(request: HttpRequest, deposito: str) -> str:
    dep = normalizar_deposito(deposito)
    request.session[SESSION_KEY] = dep
    request.session.modified = True
    return dep


def anexar_cookie_deposito(response: HttpResponse, deposito: str) -> HttpResponse:
    dep = normalizar_deposito(deposito)
    response.set_cookie(
        COOKIE_NAME,
        dep,
        max_age=60 * 60 * 24 * 400,
        samesite="Lax",
        path="/",
    )
    return response


def deposito_da_venda(venda) -> str:
    """Depósito gravado na venda; vendas antigas sem campo → env (Centro histórico)."""
    raw = getattr(venda, "deposito", None)
    if raw is not None and str(raw).strip():
        return normalizar_deposito(raw)
    return deposito_padrao_env()


def normalizar_confirmacao_loja(texto) -> str | None:
    """Texto digitado pelo operador → centro | vila | None."""
    import re

    t = str(texto or "").strip().lower()
    t = re.sub(r"[^a-z0-9\s]", "", t)
    t = " ".join(t.split())
    if t in ("centro", "central", "loja centro"):
        return DEPOSITO_CENTRO
    if t in ("vila", "vila elias", "vilas", "vilaelias"):
        return DEPOSITO_VILA
    return None


def confirmacao_loja_bate(texto, deposito_esperado: str) -> bool:
    got = normalizar_confirmacao_loja(texto)
    return got is not None and got == normalizar_deposito(deposito_esperado)


def palavra_confirmacao_loja(deposito: str) -> str:
    return "vila" if normalizar_deposito(deposito) == DEPOSITO_VILA else "centro"


def trava_loja_por_caixa(request: HttpRequest | None) -> dict | None:
    """
    Com caixa operacional aberto neste navegador, a loja fica travada no depósito do turno.
    Caixa Teste não trava (turno isolado).
    """
    if request is None:
        return None
    try:
        from produtos.caixa_util import (
            PONTO_CAIXA_GAVETA,
            PONTO_CAIXA_NOTEBOOK,
            PONTO_CAIXA_TESTE,
            PONTO_CAIXA_VILA,
            normalizar_ponto_caixa,
            obter_sessao_caixa_aberta_request,
        )
    except Exception:
        return None
    s = obter_sessao_caixa_aberta_request(request)
    if not s:
        return None
    p = normalizar_ponto_caixa(getattr(s, "ponto_caixa", None))
    if p == PONTO_CAIXA_TESTE:
        return None
    if p == PONTO_CAIXA_VILA:
        dep = DEPOSITO_VILA
    elif p == PONTO_CAIXA_GAVETA:
        dep = DEPOSITO_CENTRO
    elif p == PONTO_CAIXA_NOTEBOOK:
        dep = resolver_deposito_request(request)
    else:
        dep = DEPOSITO_CENTRO
    return {
        "travado": True,
        "deposito": dep,
        "depositoLabel": rotulo_deposito(dep),
        "lojaId": loja_id_de_deposito(dep),
        "sessaoPk": int(s.pk),
        "rotulo": f"Travado: {rotulo_deposito(dep)}",
        "estoqueAtivoLabel": f"Travado: {rotulo_deposito(dep)}",
    }


def bootstrap_deposito(request: HttpRequest | None) -> dict:
    dep = resolver_deposito_request(request)
    trava = trava_loja_por_caixa(request)
    if trava and trava.get("deposito") in DEPOSITOS_VALIDOS:
        dep = trava["deposito"]
    out = {
        "deposito": dep,
        "depositoLabel": rotulo_deposito(dep),
        "lojaId": loja_id_de_deposito(dep),
        "estoqueAtivoLabel": f"Estoque: {rotulo_deposito(dep)}",
        "caixaTravado": bool(trava),
        "trava": trava,
    }
    if trava:
        out["estoqueAtivoLabel"] = trava.get("estoqueAtivoLabel") or out["estoqueAtivoLabel"]
    return out
