"""
Agregações financeiras a partir do Mongo (DtoLancamento), alinhadas ao ERP.
"""
from __future__ import annotations

import logging
from datetime import datetime, time as dtime
from decimal import Decimal

from django.utils import timezone

logger = logging.getLogger(__name__)

_SENTINEL = datetime(1, 1, 1, 0, 0)


def _dec(v) -> Decimal:
    if v is None:
        return Decimal("0")
    try:
        return Decimal(str(v))
    except Exception:
        return Decimal("0")


def _restante_a_pagar(doc: dict) -> Decimal:
    """
    Valor ainda não pago no título (alinha ao card «Não pago» do ERP).

    A «Previsão» usa o valor bruto (Saida); após baixas parciais/totais o ERP mostra
    Previsão − Realizado; aqui usamos Saida − ValorPago quando aplicável.
    """
    saida = _dec(doc.get("Saida"))
    valor_pago = _dec(doc.get("ValorPago"))
    r = saida - valor_pago
    return r if r > 0 else Decimal("0")


def _restante_a_receber(doc: dict) -> Decimal:
    """Saldo a receber: Entrada − Recebido (equivalente ao «não recebido» no ERP)."""
    entrada = _dec(doc.get("Entrada"))
    recebido = _dec(doc.get("Recebido"))
    r = entrada - recebido
    return r if r > 0 else Decimal("0")


def _filtro_sem_quitacao_registrada():
    """
    Título ainda não quitado no Mongo: sem DataPagamento ou data “sentinel” do ERP.

    Evita duplicar o que o ERP já baixou quando `Pago` ainda não sincronizou após
    uma quitação (ex.: pagamento de manhã).
    """
    return {
        "$or": [
            {"DataPagamento": {"$exists": False}},
            {"DataPagamento": None},
            {"DataPagamento": {"$lte": _SENTINEL}},
        ]
    }


def obter_vencimentos_abertos_dia_mongo(db, dia=None) -> tuple[Decimal, Decimal]:
    """
    Soma **saldo não quitado** dos títulos com DataVencimento no dia civil (timezone Django).

    Critérios: `Pago=False` e **sem** `DataPagamento` efetiva (quitação já gravada no
    documento some do total).

    - **A pagar** (Despesa=True): soma ``Saida - ValorPago`` (não o bruto «Previsão»).
    - **A receber** (Despesa=False): soma ``Entrada - Recebido``.
    """
    if db is None:
        return Decimal("0"), Decimal("0")

    dia = dia or timezone.localdate()
    tz = timezone.get_current_timezone()
    inicio = timezone.make_aware(datetime.combine(dia, dtime.min), tz)
    fim = timezone.make_aware(datetime.combine(dia, dtime.max), tz)

    q_base = {
        "DataVencimento": {"$gte": inicio, "$lte": fim, "$gt": _SENTINEL},
        "Pago": False,
        **_filtro_sem_quitacao_registrada(),
    }

    total_pagar = Decimal("0")
    total_receber = Decimal("0")

    try:
        for doc in db["DtoLancamento"].find({**q_base, "Despesa": True}):
            total_pagar += _restante_a_pagar(doc)
        for doc in db["DtoLancamento"].find({**q_base, "Despesa": False}):
            total_receber += _restante_a_receber(doc)
    except Exception as exc:
        logger.exception("obter_vencimentos_abertos_dia_mongo: %s", exc)
        return Decimal("0"), Decimal("0")

    return total_pagar.quantize(Decimal("0.01")), total_receber.quantize(Decimal("0.01"))
