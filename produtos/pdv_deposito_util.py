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


def bootstrap_deposito(request: HttpRequest | None) -> dict:
    dep = resolver_deposito_request(request)
    return {
        "deposito": dep,
        "depositoLabel": rotulo_deposito(dep),
        "lojaId": loja_id_de_deposito(dep),
        "estoqueAtivoLabel": f"Estoque: {rotulo_deposito(dep)}",
    }
