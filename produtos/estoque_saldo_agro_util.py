"""Saldos operacionais Agro (ajuste + ledger) com ou sem espelho Mongo."""
from __future__ import annotations

from decimal import Decimal

from produtos.estoque_agro_util import agro_estoque_ledger_ativo, calcular_saldo_operacional_deposito


def _mapear_estoques_mongo_por_produto(estoques, client) -> dict[str, dict[str, float]]:
    mapa: dict[str, dict[str, float]] = {}
    for e in estoques:
        pid = str(e.get("ProdutoID"))
        dep = str(e.get("DepositoID") or "")
        saldo = float(e.get("Saldo", 0) or 0)
        if pid not in mapa:
            mapa[pid] = {"centro": 0.0, "vila": 0.0}
        if dep == client.DEPOSITO_CENTRO:
            mapa[pid]["centro"] += saldo
        elif dep == client.DEPOSITO_VILA_ELIAS:
            mapa[pid]["vila"] += saldo
    return mapa


def ajustes_mais_recentes_por_produtos(produto_ids: list[str] | None = None) -> dict:
    """Último ajuste por (produto, depósito).

    Com lista grande (>900 IDs) **não** faz ``.all()`` — isso varria a tabela inteira
    no Postgres e derrubava o PDV (pico CPU + worker OOM). Em vez disso, busca em
    fatias de 900.
    """
    from estoque.models import AjusteRapidoEstoque

    mapa = {}
    order = ("produto_externo_id", "deposito", "-criado_em")

    def _absorver(qs) -> None:
        for ajuste in qs.order_by(*order).iterator(chunk_size=2000):
            chave = (ajuste.produto_externo_id, ajuste.deposito)
            if chave not in mapa:
                mapa[chave] = ajuste

    if produto_ids is None:
        _absorver(AjusteRapidoEstoque.objects.all())
        return mapa

    ids = [str(x).strip() for x in produto_ids if x is not None and str(x).strip()]
    if not ids:
        return mapa

    _chunk = 900
    for i in range(0, len(ids), _chunk):
        fatia = ids[i : i + _chunk]
        _absorver(AjusteRapidoEstoque.objects.filter(produto_externo_id__in=fatia))
    return mapa


def mapa_saldos_operacionais_agro(
    produto_ids: list[str],
    *,
    db=None,
    client=None,
) -> dict[str, dict[str, float]]:
    """
    Saldo final centro/vila por produto_externo_id.
    Com Mongo: ERP + ajuste (+ ledger se ativo). Sem Mongo: só ajuste/ledger.
    """
    p_ids = [str(x) for x in produto_ids if x is not None and str(x).strip()]
    if not p_ids:
        return {}

    ajustes_map = ajustes_mais_recentes_por_produtos(p_ids)
    ledger = agro_estoque_ledger_ativo()

    estoque_map: dict[str, dict[str, float]] = {}
    if db is not None and client is not None:
        _chunk = 800
        for i in range(0, len(p_ids), _chunk):
            slice_ids = p_ids[i : i + _chunk]
            try:
                estoques = list(
                    db[client.col_e].find(
                        {"ProdutoID": {"$in": slice_ids}},
                        {"ProdutoID": 1, "DepositoID": 1, "Saldo": 1, "_id": 0},
                    )
                )
            except Exception:
                estoques = []
            partial = _mapear_estoques_mongo_por_produto(estoques, client)
            for pid, dep in partial.items():
                if pid not in estoque_map:
                    estoque_map[pid] = {"centro": 0.0, "vila": 0.0}
                estoque_map[pid]["centro"] += dep["centro"]
                estoque_map[pid]["vila"] += dep["vila"]

    out: dict[str, dict[str, float]] = {}
    for pid in p_ids:
        s_c = float(estoque_map.get(pid, {}).get("centro", 0.0))
        s_v = float(estoque_map.get(pid, {}).get("vila", 0.0))
        aj_c = ajustes_map.get((pid, "centro"))
        aj_v = ajustes_map.get((pid, "vila"))
        saldo_f_c = calcular_saldo_operacional_deposito(aj_c, s_c, ledger=ledger)
        saldo_f_v = calcular_saldo_operacional_deposito(aj_v, s_v, ledger=ledger)
        out[pid] = {
            "saldo_centro": round(saldo_f_c, 2),
            "saldo_vila": round(saldo_f_v, 2),
            "saldo_erp_centro": s_c,
            "saldo_erp_vila": s_v,
        }
    return out


def produto_ids_saldo_deposito_positivo(deposito: str = "vila") -> list[str]:
    """IDs com saldo operacional > 0 no depósito (ledger ou ERP+ajuste)."""
    from estoque.models import AjusteRapidoEstoque

    dep = (deposito or "vila").strip().lower()
    ledger = agro_estoque_ledger_ativo()
    ajustes = AjusteRapidoEstoque.objects.filter(deposito=dep).order_by(
        "produto_externo_id", "-criado_em"
    )
    seen: set[str] = set()
    out: list[str] = []
    for aj in ajustes:
        pid = str(aj.produto_externo_id or "").strip()
        if not pid or pid in seen:
            continue
        seen.add(pid)
        saldo = calcular_saldo_operacional_deposito(aj, 0.0, ledger=ledger)
        if saldo > 0:
            out.append(pid)
    return out


def mapa_produtos_info_por_externo_ids(p_ids: list[str]) -> dict[str, dict]:
    from produtos.catalogo_agro import produto_agro_para_row
    from produtos.models import Produto

    out: dict[str, dict] = {}
    chunk = 500
    for i in range(0, len(p_ids), chunk):
        slice_ids = p_ids[i : i + chunk]
        for p in Produto.objects.filter(produto_externo_id__in=slice_ids):
            pid = str(p.produto_externo_id or "").strip()
            if not pid:
                continue
            row = produto_agro_para_row(p)
            out[pid] = {
                "nome": row.get("nome") or f"Produto {pid}",
                "codigo": row.get("codigo_nfe") or row.get("codigo") or pid,
                "codigo_barras": row.get("codigo_barras") or "",
            }
    return out


def saldos_transferencia_de_mapa(
    saldos_map: dict[str, dict[str, float]],
    pid: str,
    ajustes: dict,
) -> tuple[Decimal, Decimal, Decimal, Decimal]:
    """Retorna (saldo_centro, saldo_vila, saldo_centro_erp, saldo_vila_erp)."""
    ledger = agro_estoque_ledger_ativo()
    info = saldos_map.get(pid) or {}
    saldo_centro_erp = Decimal(str(info.get("saldo_erp_centro", 0.0)))
    saldo_vila_erp = Decimal(str(info.get("saldo_erp_vila", 0.0)))
    ajuste_centro = ajustes.get((pid, "centro"))
    ajuste_vila = ajustes.get((pid, "vila"))
    if ledger:
        saldo_centro = Decimal(
            str(
                calcular_saldo_operacional_deposito(
                    ajuste_centro, float(saldo_centro_erp), ledger=True
                )
            )
        )
        saldo_vila = Decimal(
            str(
                calcular_saldo_operacional_deposito(
                    ajuste_vila, float(saldo_vila_erp), ledger=True
                )
            )
        )
    else:
        saldo_centro = saldo_centro_erp + (
            ajuste_centro.diferenca_saldo if ajuste_centro else Decimal("0")
        )
        saldo_vila = saldo_vila_erp + (
            ajuste_vila.diferenca_saldo if ajuste_vila else Decimal("0")
        )
    return saldo_centro, saldo_vila, saldo_centro_erp, saldo_vila_erp
