"""Tributos aproximados — Lei Federal 12.741/2012 (IBPT) para DANFE NFC-e."""
from __future__ import annotations

import logging
import re
from decimal import Decimal, ROUND_HALF_UP
from functools import lru_cache
from typing import Any

import requests
from decouple import config

from produtos.caixa_util import format_moeda_br
from produtos.nfce_fiscal_produto_util import fiscal_por_produto_id

logger = logging.getLogger(__name__)


def _cfg(name: str, default: str = "") -> str:
    return (config(name, default=default) or default).strip()


def _dec(v: Any) -> Decimal:
    try:
        return Decimal(str(v or 0)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    except Exception:
        return Decimal("0.00")


@lru_cache(maxsize=4096)
def _ibpt_aliquotas_ncm(ncm: str, uf: str = "SP") -> tuple[Decimal, Decimal, Decimal]:
    """Percentuais IBPT (nacional, estadual, municipal) por NCM."""
    ncm8 = re.sub(r"\D", "", ncm)[:8].zfill(8)
    token = _cfg("NFC_E_IBPT_TOKEN")
    cnpj = re.sub(r"\D", "", _cfg("NFC_E_IBPT_CNPJ") or _cfg("NFC_E_CNPJ"))[:14]
    if token and len(cnpj) == 14:
        try:
            r = requests.get(
                "https://apidoni.ibpt.org.br/api/v1/produtos",
                params={
                    "token": token,
                    "cnpj": cnpj,
                    "codigo": ncm8,
                    "uf": uf.upper()[:2],
                    "ex": "0",
                    "descricao": "produto",
                },
                timeout=8,
            )
            if r.ok:
                data = r.json()
                return (
                    _dec(data.get("Nacional")),
                    _dec(data.get("Estadual")),
                    _dec(data.get("Municipal")),
                )
        except Exception:
            logger.debug("IBPT API indisponível para NCM %s", ncm8, exc_info=True)
    try:
        nac = _dec(_cfg("NFC_E_IBPT_ALIQ_NAC", "13.45"))
        est = _dec(_cfg("NFC_E_IBPT_ALIQ_EST", "18.00"))
        mun = _dec(_cfg("NFC_E_IBPT_ALIQ_MUN", "0"))
    except Exception:
        nac, est, mun = Decimal("13.45"), Decimal("18.00"), Decimal("0")
    return nac, est, mun


def calcular_ibpt_venda_itens(
    itens,
    *,
    db=None,
    col_p: str | None = None,
    uf: str = "SP",
) -> dict[str, Any]:
    """Soma tributos aproximados por item (NCM) e monta texto legal do cupom."""
    fed = est = mun = Decimal("0")
    for it in itens:
        sub = _dec(getattr(it, "valor_total", 0))
        if sub <= 0:
            continue
        pid = str(getattr(it, "produto_id_externo", "") or "")
        fis = fiscal_por_produto_id(pid, db=db, col_p=col_p)
        nac_p, est_p, mun_p = _ibpt_aliquotas_ncm(fis.get("ncm") or "23099020", uf)
        fed += (sub * nac_p / Decimal("100")).quantize(Decimal("0.01"), ROUND_HALF_UP)
        est += (sub * est_p / Decimal("100")).quantize(Decimal("0.01"), ROUND_HALF_UP)
        mun += (sub * mun_p / Decimal("100")).quantize(Decimal("0.01"), ROUND_HALF_UP)
    texto = (
        "Tributos Totais Incidentes (Lei Federal 12.741/2012) — "
        f"Federal {format_moeda_br(fed)} · Estadual {format_moeda_br(est)} · "
        f"Municipal {format_moeda_br(mun)}. Fonte: IBPT"
    )
    return {
        "ibpt_federal": float(fed),
        "ibpt_estadual": float(est),
        "ibpt_municipal": float(mun),
        "ibpt_total": float(fed + est + mun),
        "ibpt_texto": texto,
    }
