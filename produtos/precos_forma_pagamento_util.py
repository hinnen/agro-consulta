"""Preço de venda por forma de pagamento ou por 2 grupos (overlay Agro / PDV)."""
from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any

from produtos.caixa_util import FORMAS_PAGAMENTO_CAIXA, normalizar_forma_pagamento_caixa


def _forma_canonica(raw: str) -> str:
    txt = str(raw or "").strip()
    if not txt:
        return ""
    return normalizar_forma_pagamento_caixa(txt)


def _dec_pos(v: Any) -> float | None:
    if v is None or str(v).strip() == "":
        return None
    try:
        n = Decimal(str(v).replace(",", ".").strip())
    except (InvalidOperation, ValueError, TypeError):
        return None
    if n < 0:
        return None
    return float(n.quantize(Decimal("0.01")))


def normalizar_precos_por_forma_payload(raw: Any) -> dict[str, float]:
    """Aceita dict ou lista [{forma, valor}] e retorna só formas válidas com valor > 0."""
    out: dict[str, float] = {}
    if isinstance(raw, list):
        for it in raw:
            if not isinstance(it, dict):
                continue
            forma = _forma_canonica(str(it.get("forma") or ""))
            if not forma:
                continue
            val = _dec_pos(it.get("valor"))
            if val is not None and val > 0:
                out[forma] = val
        return out
    if not isinstance(raw, dict):
        return out
    for k, v in raw.items():
        forma = _forma_canonica(str(k or ""))
        if not forma:
            continue
        val = _dec_pos(v)
        if val is not None and val > 0:
            out[forma] = val
    return out


def normalizar_precos_modo(raw: Any) -> str:
    m = str(raw or "").strip().lower().replace("-", "_").replace(" ", "_")
    if m in ("grupos", "grupo", "2_grupos", "dois_grupos", "ab", "a_b"):
        return "grupos"
    return "por_forma"


def _formas_lista_payload(raw: Any) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    if not isinstance(raw, (list, tuple)):
        return out
    for it in raw:
        forma = _forma_canonica(str(it or ""))
        if not forma or forma in seen:
            continue
        seen.add(forma)
        out.append(forma)
    return out


def normalizar_precos_grupos_payload(raw: Any) -> dict[str, Any] | None:
    """
    Retorna dict canônico ou None se vazio (sem preços e sem formas).
    Forma em A e B ao mesmo tempo fica só em A.
    """
    if not isinstance(raw, dict):
        return None
    preco_a = _dec_pos(raw.get("preco_a"))
    preco_b = _dec_pos(raw.get("preco_b"))
    formas_a = _formas_lista_payload(raw.get("formas_a"))
    formas_b = _formas_lista_payload(raw.get("formas_b"))
    # Remove colisão: prioridade A
    set_a = set(formas_a)
    formas_b = [f for f in formas_b if f not in set_a]
    if not ((preco_a is not None and preco_a > 0) or (preco_b is not None and preco_b > 0) or formas_a or formas_b):
        return None
    return {
        "preco_a": round(preco_a, 2) if preco_a is not None and preco_a > 0 else None,
        "preco_b": round(preco_b, 2) if preco_b is not None and preco_b > 0 else None,
        "formas_a": formas_a,
        "formas_b": formas_b,
    }


def extrair_precos_por_forma_cadastro_extras(ex: dict | None) -> dict[str, float]:
    if not isinstance(ex, dict):
        return {}
    return normalizar_precos_por_forma_payload(ex.get("precos_por_forma"))


def extrair_precos_por_forma_overlay(ov) -> dict[str, float]:
    if ov is None:
        return {}
    ce = getattr(ov, "cadastro_extras", None)
    if not isinstance(ce, dict):
        return {}
    return extrair_precos_por_forma_cadastro_extras(ce)


def extrair_precos_modo_cadastro_extras(ex: dict | None) -> str:
    if not isinstance(ex, dict):
        return "por_forma"
    return normalizar_precos_modo(ex.get("precos_modo"))


def extrair_precos_grupos_cadastro_extras(ex: dict | None) -> dict[str, Any] | None:
    if not isinstance(ex, dict):
        return None
    return normalizar_precos_grupos_payload(ex.get("precos_grupos"))


def extrair_precos_modo_overlay(ov) -> str:
    if ov is None:
        return "por_forma"
    ce = getattr(ov, "cadastro_extras", None)
    return extrair_precos_modo_cadastro_extras(ce if isinstance(ce, dict) else None)


def extrair_precos_grupos_overlay(ov) -> dict[str, Any] | None:
    if ov is None:
        return None
    ce = getattr(ov, "cadastro_extras", None)
    return extrair_precos_grupos_cadastro_extras(ce if isinstance(ce, dict) else None)


def preco_venda_para_forma(
    preco_base: float,
    precos_por_forma: dict[str, float] | None,
    forma: str | None,
    *,
    precos_modo: str | None = None,
    precos_grupos: dict[str, Any] | None = None,
) -> float:
    base = _dec_pos(preco_base) or 0.0
    forma_n = _forma_canonica(str(forma or ""))
    if not forma_n:
        return base
    modo = normalizar_precos_modo(precos_modo)
    if modo == "grupos":
        g = precos_grupos if isinstance(precos_grupos, dict) else None
        if not g:
            return base
        formas_a = set(_formas_lista_payload(g.get("formas_a")))
        formas_b = set(_formas_lista_payload(g.get("formas_b")))
        if forma_n in formas_a:
            pa = _dec_pos(g.get("preco_a"))
            if pa is not None and pa > 0:
                return pa
        if forma_n in formas_b:
            pb = _dec_pos(g.get("preco_b"))
            if pb is not None and pb > 0:
                return pb
        return base
    if not isinstance(precos_por_forma, dict):
        return base
    if forma_n in precos_por_forma:
        pf = _dec_pos(precos_por_forma.get(forma_n))
        if pf is not None and pf > 0:
            return pf
    return base


def formas_pagamento_lista() -> list[str]:
    return list(FORMAS_PAGAMENTO_CAIXA)
