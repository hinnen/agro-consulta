"""
Agregações financeiras a partir do Mongo (DtoLancamento), alinhadas ao ERP.
"""
from __future__ import annotations

import logging
import re
from datetime import date, datetime, time as dtime
from decimal import Decimal
from typing import Any

from django.utils import timezone

logger = logging.getLogger(__name__)

_SENTINEL = datetime(1, 1, 1, 0, 0)
COL_DTO_LANCAMENTO = "DtoLancamento"


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


def _filtro_quitado():
    """Título quitado conforme Mongo (Pago ou DataPagamento efetiva)."""
    return {
        "$or": [
            {"Pago": True},
            {"DataPagamento": {"$exists": True, "$gt": _SENTINEL}},
        ]
    }


def _dt_efetiva(v) -> bool:
    return v is not None and isinstance(v, datetime) and v > _SENTINEL


def contas_pagar_montar_query_mongo(
    *,
    status: str = "abertos",
    vencimento_de: date | None = None,
    vencimento_ate: date | None = None,
    texto: str | None = None,
) -> dict[str, Any]:
    """
    Filtro Mongo para **contas a pagar** (Despesa=True), espelhando o ERP.

    status: ``abertos`` | ``quitados`` | ``todos``
    """
    base: dict[str, Any] = {"Despesa": True}

    st = (status or "abertos").strip().lower()
    if st == "abertos":
        base["Pago"] = False
        base.update(_filtro_sem_quitacao_registrada())
    elif st == "quitados":
        base.update(_filtro_quitado())
    elif st != "todos":
        st = "abertos"
        base["Pago"] = False
        base.update(_filtro_sem_quitacao_registrada())

    tz = timezone.get_current_timezone()
    if vencimento_de is not None:
        ini = timezone.make_aware(datetime.combine(vencimento_de, dtime.min), tz)
        base.setdefault("DataVencimento", {})
        if not isinstance(base["DataVencimento"], dict):
            base["DataVencimento"] = {}
        base["DataVencimento"]["$gte"] = ini
    if vencimento_ate is not None:
        fim = timezone.make_aware(datetime.combine(vencimento_ate, dtime.max), tz)
        base.setdefault("DataVencimento", {})
        if not isinstance(base["DataVencimento"], dict):
            base["DataVencimento"] = {}
        base["DataVencimento"]["$lte"] = fim

    if base.get("DataVencimento") == {}:
        del base["DataVencimento"]

    t = (texto or "").strip()
    if t:
        esc = re.escape(t[:120])
        rx = re.compile(esc, re.IGNORECASE)
        texto_or = {
            "$or": [
                {"Descricao": rx},
                {"Cliente": rx},
                {"NumeroDocumento": rx},
                {"Observacoes": rx},
                {"PlanoDeConta": rx},
                {"LancamentoGrupo": rx},
                {"FormaPagamento": rx},
            ]
        }
        return {"$and": [base, texto_or]}

    return base


def contas_pagar_totais_filtrados(db, query: dict) -> dict[str, float]:
    """Totais sobre o conjunto filtrado (uma agregação)."""
    if db is None:
        return {"quantidade": 0, "previsto": 0.0, "pago": 0.0, "a_pagar": 0.0}
    try:
        pipe = [
            {"$match": query},
            {
                "$group": {
                    "_id": None,
                    "n": {"$sum": 1},
                    "previsto": {"$sum": {"$ifNull": ["$Saida", 0]}},
                    "pago": {"$sum": {"$ifNull": ["$ValorPago", 0]}},
                    "a_pagar": {
                        "$sum": {
                            "$max": [
                                0,
                                {
                                    "$subtract": [
                                        {"$ifNull": ["$Saida", 0]},
                                        {"$ifNull": ["$ValorPago", 0]},
                                    ]
                                },
                            ]
                        }
                    },
                }
            },
        ]
        agg = list(db[COL_DTO_LANCAMENTO].aggregate(pipe))
        if not agg:
            return {"quantidade": 0, "previsto": 0.0, "pago": 0.0, "a_pagar": 0.0}
        a = agg[0]
        return {
            "quantidade": int(a.get("n") or 0),
            "previsto": round(float(a.get("previsto") or 0), 2),
            "pago": round(float(a.get("pago") or 0), 2),
            "a_pagar": round(float(a.get("a_pagar") or 0), 2),
        }
    except Exception as exc:
        logger.exception("contas_pagar_totais_filtrados: %s", exc)
        return {"quantidade": 0, "previsto": 0.0, "pago": 0.0, "a_pagar": 0.0}


def _serializar_dt(v) -> str | None:
    if v is None or not isinstance(v, datetime):
        return None
    if v.replace(tzinfo=None) <= _SENTINEL:
        return None
    try:
        if timezone.is_naive(v):
            v = timezone.make_aware(v, timezone.get_current_timezone())
        return timezone.localtime(v).isoformat()
    except Exception:
        return v.isoformat(sep=" ")


def lancamento_contas_pagar_para_api(doc: dict) -> dict[str, Any]:
    """DTO enxuto para a UI (somente leitura)."""
    restante = _restante_a_pagar(doc)
    dp = doc.get("DataPagamento")
    quitado = bool(doc.get("Pago")) or _dt_efetiva(dp)
    return {
        "id": str(doc.get("_id", "")),
        "descricao": doc.get("Descricao") or "",
        "cliente": doc.get("Cliente") or "",
        "numero_documento": str(doc.get("NumeroDocumento") or ""),
        "parcela": int(doc.get("NumeroParcela") or 0),
        "plano_conta": doc.get("PlanoDeConta") or "",
        "grupo": doc.get("LancamentoGrupo") or "",
        "forma_pagamento": doc.get("FormaPagamento") or "",
        "centro_custo": doc.get("CentroDeCusto") or "",
        "empresa": doc.get("Empresa") or "",
        "observacoes": (doc.get("Observacoes") or "")[:500],
        "valor_previsto": round(float(doc.get("Saida") or 0), 2),
        "valor_pago": round(float(doc.get("ValorPago") or 0), 2),
        "restante": float(restante.quantize(Decimal("0.01"))),
        "pago": quitado,
        "data_vencimento": _serializar_dt(doc.get("DataVencimento")),
        "data_competencia": _serializar_dt(doc.get("DataCompetencia")),
        "data_fluxo": _serializar_dt(doc.get("DataFluxo")),
        "data_pagamento": _serializar_dt(dp) if quitado else None,
    }


def contas_pagar_buscar_pagina(
    db,
    query: dict,
    *,
    page: int = 1,
    page_size: int = 50,
    ordenacao: str = "vencimento_asc",
) -> tuple[list[dict], int, dict[str, float]]:
    """
    Retorna (linhas_serializadas, total_documentos, totais_agregados).
    """
    if db is None:
        return [], 0, {"quantidade": 0, "previsto": 0.0, "pago": 0.0, "a_pagar": 0.0}

    page = max(1, page)
    page_size = min(200, max(1, page_size))
    skip = (page - 1) * page_size

    ord = (ordenacao or "vencimento_asc").strip().lower()
    if ord == "vencimento_desc":
        sort_spec = [("DataVencimento", -1), ("_id", -1)]
    elif ord == "fluxo_desc":
        sort_spec = [("DataFluxo", -1), ("_id", -1)]
    else:
        sort_spec = [("DataVencimento", 1), ("_id", 1)]

    try:
        col = db[COL_DTO_LANCAMENTO]
        total = col.count_documents(query)
        totais = contas_pagar_totais_filtrados(db, query)
        cur = (
            col.find(query)
            .sort(sort_spec)
            .skip(skip)
            .limit(page_size)
        )
        linhas = [lancamento_contas_pagar_para_api(d) for d in cur]
        return linhas, total, totais
    except Exception as exc:
        logger.exception("contas_pagar_buscar_pagina: %s", exc)
        return [], 0, {"quantidade": 0, "previsto": 0.0, "pago": 0.0, "a_pagar": 0.0}
