"""
Agregações de vendas a partir do Mongo (DtoVenda / DtoVendaProduto), alinhadas ao uso em views.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, time as dtime
from decimal import Decimal
from typing import Any

from django.utils import timezone

logger = logging.getLogger(__name__)


def _filtro_venda_ativa_mongo():
    """
    Exclui vendas canceladas e status que não entram no faturamento do dia
    (orçamento, cancelado em qualquer redação — ex.: "Pedido Cancelado", "Orçamento" com ç).
    Alinha o total ao relatório Pedidos Faturados do ERP.
    """
    return {
        "Cancelada": {"$ne": True},
        "$nor": [
            {"Status": {"$regex": r"cancel", "$options": "i"}},
            {"Status": {"$regex": r"orçamento", "$options": "i"}},
            {"Status": {"$regex": r"orcamento", "$options": "i"}},
        ],
    }


def _decimal_seguro(v) -> Decimal:
    if v is None:
        return Decimal("0")
    try:
        return Decimal(str(v))
    except Exception:
        return Decimal("0")


def _valor_cabecalho_venda(v: dict) -> Decimal | None:
    for k in (
        "ValorTotal",
        "Total",
        "ValorLiquido",
        "valorTotal",
        "total",
        "TotalGeral",
        "ValorFinal",
    ):
        if v.get(k) is not None:
            return _decimal_seguro(v.get(k))
    return None


def _valor_linha_item(item: dict) -> Decimal:
    for k in (
        "ValorTotal",
        "Total",
        "SubTotal",
        "ValorLiquido",
        "valorTotal",
        "TotalLinha",
    ):
        if item.get(k) is not None:
            d = _decimal_seguro(item.get(k))
            if d != 0:
                return d
    pu = (
        item.get("PrecoUnitario")
        or item.get("ValorUnitario")
        or item.get("precoUnitario")
        or item.get("Preco")
    )
    qtd = item.get("Quantidade") or item.get("quantidade") or 0
    try:
        if pu is not None:
            return _decimal_seguro(pu) * _decimal_seguro(qtd)
    except Exception:
        pass
    return Decimal("0")


def obter_valor_total_vendas_dia_mongo(db, dia=None) -> Decimal:
    """
    Soma o faturamento do dia (timezone Django) a partir de DtoVenda / DtoVendaProduto.
    Usa valor do cabeçalho quando existir; senão soma linhas de DtoVendaProduto.
    """
    if db is None:
        return Decimal("0")

    dia = dia or timezone.localdate()
    tz = timezone.get_current_timezone()
    inicio = timezone.make_aware(datetime.combine(dia, dtime.min), tz)
    fim = timezone.make_aware(datetime.combine(dia, dtime.max), tz)

    q = {"Data": {"$gte": inicio, "$lte": fim}, **_filtro_venda_ativa_mongo()}

    try:
        vendas = list(db["DtoVenda"].find(q))
    except Exception as exc:
        logger.exception("obter_valor_total_vendas_dia_mongo: find DtoVenda: %s", exc)
        return Decimal("0")

    if not vendas:
        return Decimal("0")

    venda_ids_obj = []
    venda_ids_str = []
    total = Decimal("0")
    precisa_itens: list[str] = []

    for v in vendas:
        vt = _valor_cabecalho_venda(v)
        if vt is not None:
            total += vt
            continue
        vid = str(v.get("Id") or v.get("_id"))
        precisa_itens.append(vid)
        venda_ids_str.append(vid)
        if len(vid) == 24:
            try:
                from bson import ObjectId

                venda_ids_obj.append(ObjectId(vid))
            except Exception:
                pass

    if not precisa_itens:
        return total.quantize(Decimal("0.01"))

    query_itens = {
        "$or": [
            {"VendaID": {"$in": venda_ids_obj}},
            {"VendaID": {"$in": venda_ids_str}},
        ]
    }
    try:
        itens = db["DtoVendaProduto"].find(query_itens)
    except Exception as exc:
        logger.exception("obter_valor_total_vendas_dia_mongo: itens: %s", exc)
        return total.quantize(Decimal("0.01"))

    soma_por_venda: dict[str, Decimal] = {}
    for item in itens:
        vid_raw = item.get("VendaID")
        vid = str(vid_raw) if vid_raw is not None else ""
        if not vid or vid == "None":
            continue
        linha = _valor_linha_item(item)
        soma_por_venda[vid] = soma_por_venda.get(vid, Decimal("0")) + linha

    for vid in precisa_itens:
        for k in (vid, str(vid)):
            if k in soma_por_venda:
                total += soma_por_venda[k]
                break

    return total.quantize(Decimal("0.01"))


def media_vendas_diaria_ultimos_n_dias(db, n: int = 30) -> Decimal:
    """
    Média diária = soma do faturamento de cada um dos últimos n dias corridos (incluindo hoje) ÷ n.
    Dias sem venda entram com zero no numerador.
    """
    if db is None or n < 1:
        return Decimal("0")
    n = min(int(n), 365)
    hoje = timezone.localdate()
    total = Decimal("0")
    for k in range(n):
        d = hoje - timedelta(days=k)
        total += obter_valor_total_vendas_dia_mongo(db, d)
    return (total / Decimal(n)).quantize(Decimal("0.01"))


def faixa_dia_mes_mongo(day_of_month: int) -> str:
    """Segmenta o mês: início (1–10), meio (11–20), final (21+)."""
    d = int(day_of_month)
    if d <= 10:
        return "inicio"
    if d <= 20:
        return "meio"
    return "final"


def fatores_vendas_por_calendario(db, dias_lookback: int = 84) -> dict[str, Any]:
    """
    Multiplicadores em relação à média diária global do período:
    - por dia da semana (0=segunda … 6=domingo);
    - por faixa do mês (início / meio / final).

    Usa histórico de faturamento por dia civil (Mongo). Poucas amostras → fator 1.0.
    """
    nomes_curto = ("Seg", "Ter", "Qua", "Qui", "Sex", "Sáb", "Dom")
    faixa_labels = {
        "inicio": "Início do mês (1–10)",
        "meio": "Meio do mês (11–20)",
        "final": "Final do mês (21+)",
    }
    out: dict[str, Any] = {
        "lookback_dias": dias_lookback,
        "media_global_dia": 0.0,
        "mult_dow": [1.0] * 7,
        "mult_faixa": {"inicio": 1.0, "meio": 1.0, "final": 1.0},
        "n_amostras_dow": [0] * 7,
        "n_amostras_faixa": {"inicio": 0, "meio": 0, "final": 0},
        "suficiente": False,
        "dia_semana_nomes_curto": nomes_curto,
        "faixa_labels": faixa_labels,
    }
    if db is None:
        return out
    dias_lookback = max(21, min(int(dias_lookback or 84), 366))
    hoje = timezone.localdate()
    totais_dia: list[float] = []
    by_dow_sum = [0.0] * 7
    by_dow_cnt = [0] * 7
    by_fx_sum = {"inicio": 0.0, "meio": 0.0, "final": 0.0}
    by_fx_cnt = {"inicio": 0, "meio": 0, "final": 0}

    for k in range(dias_lookback):
        d = hoje - timedelta(days=k)
        val = float(obter_valor_total_vendas_dia_mongo(db, d))
        totais_dia.append(val)
        wd = d.weekday()
        by_dow_sum[wd] += val
        by_dow_cnt[wd] += 1
        fx = faixa_dia_mes_mongo(d.day)
        by_fx_sum[fx] += val
        by_fx_cnt[fx] += 1

    media_g = sum(totais_dia) / dias_lookback if dias_lookback else 0.0
    out["media_global_dia"] = round(media_g, 4)
    if media_g <= 0:
        return out

    min_wd = 3
    min_fx = 5
    mult_dow: list[float] = []
    for w in range(7):
        c = by_dow_cnt[w]
        out["n_amostras_dow"][w] = c
        if c >= min_wd:
            m = by_dow_sum[w] / c
            r = m / media_g
            mult_dow.append(round(max(0.55, min(1.5, r)), 4))
        else:
            mult_dow.append(1.0)
    out["mult_dow"] = mult_dow

    mult_fx: dict[str, float] = {}
    for fx in ("inicio", "meio", "final"):
        c = by_fx_cnt[fx]
        out["n_amostras_faixa"][fx] = c
        if c >= min_fx:
            m = by_fx_sum[fx] / c
            r = m / media_g
            mult_fx[fx] = round(max(0.55, min(1.5, r)), 4)
        else:
            mult_fx[fx] = 1.0
    out["mult_faixa"] = mult_fx
    out["suficiente"] = True
    return out
