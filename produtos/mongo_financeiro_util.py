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
    Soma títulos em aberto com DataVencimento no dia civil (timezone Django).

    Critérios: `Pago=False` e **sem** `DataPagamento` efetiva (quitação já gravada no
    documento some do total, alinhando ao ERP após pagamentos).

    - **A pagar** (Despesa=True): soma Saida.
    - **A receber** (Despesa=False): soma Entrada.
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
            total_pagar += Decimal(str(doc.get("Saida") or 0))
        for doc in db["DtoLancamento"].find({**q_base, "Despesa": False}):
            total_receber += Decimal(str(doc.get("Entrada") or 0))
    except Exception as exc:
        logger.exception("obter_vencimentos_abertos_dia_mongo: %s", exc)
        return Decimal("0"), Decimal("0")

    return total_pagar.quantize(Decimal("0.01")), total_receber.quantize(Decimal("0.01"))
