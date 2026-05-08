"""
Padrões fiscais paliativos para produto cadastrado só no Agro (espelho Mongo),
alinhados a: Simples Nacional, venda ao consumidor final, operação interna em SP.

Não substitui conferência fiscal: o NCM genérico (2309.90.20) serve de preenchimento
mínimo para ERP antigo emitir NFC; ajuste por produto quando necessário.
"""

from __future__ import annotations

import re
from typing import Any

# CFOP 5102: venda de mercadoria adquirida de terceiros, não contribuinte, mesma UF.
# (5405 seria cenário com ST específica — não assumimos aqui.)
_DEFAULTS_SP_SN_CONSUMIDOR: dict[str, str] = {
    "ncm": "23099020",
    "cest": "",
    "cfop": "5102",
    "csosn": "102",
    "origem": "0",
}


def normalizar_ncm_somente_digitos(ncm_raw: Any) -> str:
    """NCM com 8 dígitos (sem pontos/tracos)."""
    d = re.sub(r"\D", "", str(ncm_raw or ""))
    return d[:8] if d else ""


def merge_fiscal_padrao_cadastro_manual_sp_sn(fiscal: dict[str, Any] | None) -> dict[str, str]:
    """
    Preenche chaves vazias com ``_DEFAULTS_SP_SN_CONSUMIDOR``.
    Mantém valores já informados pelo usuário.
    """
    fin = dict(fiscal) if isinstance(fiscal, dict) else {}
    out: dict[str, str] = {}
    for k, defv in _DEFAULTS_SP_SN_CONSUMIDOR.items():
        raw = fin.get(k)
        usr = str(raw).strip() if raw is not None else ""
        if k == "ncm":
            digits = normalizar_ncm_somente_digitos(usr)
            out[k] = digits if digits else defv
        elif usr:
            out[k] = usr
        else:
            out[k] = defv
    return out
