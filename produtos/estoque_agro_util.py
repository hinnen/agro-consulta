"""Saldo operacional Agro — fórmula legacy (Mongo+ajuste) vs ledger (ajuste informado)."""
from __future__ import annotations

from typing import Any


def agro_estoque_ledger_ativo() -> bool:
    """
    Ledger no staging: saldo exibido = ``saldo_informado`` do último ajuste (sem delta Mongo).
    Liga com ``AGRO_FONTE_ESTOQUE=ledger`` ou Fase B PDV no teste (``AGRO_PDV_CATALOGO_SOMENTE_POSTGRES``).
    """
    from produtos.agro_fonte_config import agro_estoque_usa_ledger, agro_pdv_catalogo_somente_postgres

    return agro_estoque_usa_ledger() or agro_pdv_catalogo_somente_postgres()


def calcular_saldo_operacional_deposito(
    ajuste: Any | None,
    saldo_erp: float,
    *,
    ledger: bool | None = None,
) -> float:
    """Saldo final centro/vila para PDV, gestão e APIs."""
    if ledger is None:
        ledger = agro_estoque_ledger_ativo()
    erp = float(saldo_erp or 0.0)
    if ajuste is None:
        return erp
    informado = float(getattr(ajuste, "saldo_informado", 0) or 0)
    if ledger:
        return informado
    ref = float(getattr(ajuste, "saldo_erp_referencia", 0) or 0)
    return informado + (erp - ref)
