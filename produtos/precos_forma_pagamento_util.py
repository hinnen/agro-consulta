"""Preço de venda por forma de pagamento (overlay Agro / PDV)."""
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


def preco_venda_para_forma(
    preco_base: float,
    precos_por_forma: dict[str, float] | None,
    forma: str | None,
) -> float:
    base = _dec_pos(preco_base) or 0.0
    forma_n = _forma_canonica(str(forma or ""))
    if not forma_n or not isinstance(precos_por_forma, dict):
        return base
    if forma_n in precos_por_forma:
        pf = _dec_pos(precos_por_forma.get(forma_n))
        if pf is not None and pf > 0:
            return pf
    return base


def formas_pagamento_lista() -> list[str]:
    return list(FORMAS_PAGAMENTO_CAIXA)
