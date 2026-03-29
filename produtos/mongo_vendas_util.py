"""
Agregações de vendas a partir do Mongo (DtoVenda / DtoVendaProduto), alinhadas ao uso em views.
"""
from __future__ import annotations

import logging
from datetime import datetime, time as dtime
from decimal import Decimal

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
