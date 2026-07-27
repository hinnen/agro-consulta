"""Saldos operacionais Agro (ajuste + ledger) com ou sem espelho Mongo."""
from __future__ import annotations

from decimal import Decimal

from django.db import connection

from produtos.estoque_agro_util import agro_estoque_ledger_ativo, calcular_saldo_operacional_deposito

_AJUSTES_IN_CHUNK = 800


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
    from estoque.models import AjusteRapidoEstoque

    mapa = {}
    if produto_ids is None:
        ajustes = AjusteRapidoEstoque.objects.all().order_by(
            "produto_externo_id", "deposito", "-criado_em"
        )
        for ajuste in ajustes:
            chave = (ajuste.produto_externo_id, ajuste.deposito)
            if chave not in mapa:
                mapa[chave] = ajuste
        return mapa

    p_ids = [str(x) for x in produto_ids if x is not None and str(x).strip()]
    if not p_ids:
        return {}

    # Nunca carregar a tabela inteira: filtra em fatias (Postgres/SQLite).
    for i in range(0, len(p_ids), _AJUSTES_IN_CHUNK):
        slice_ids = p_ids[i : i + _AJUSTES_IN_CHUNK]
        ajustes = (
            AjusteRapidoEstoque.objects.filter(produto_externo_id__in=slice_ids)
            .order_by("produto_externo_id", "deposito", "-criado_em")
            .only(
                "produto_externo_id",
                "deposito",
                "saldo_informado",
                "saldo_erp_referencia",
                "diferenca_saldo",
                "criado_em",
            )
        )
        for ajuste in ajustes:
            chave = (ajuste.produto_externo_id, ajuste.deposito)
            if chave not in mapa:
                mapa[chave] = ajuste
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


def _saldo_expr_sql(ledger: bool) -> str:
    if ledger:
        return "saldo_informado"
    return "(saldo_informado - COALESCE(saldo_erp_referencia, 0))"


def _mapa_ultimo_saldo_ajuste_por_deposito(
    deposito: str,
    *,
    limite: int = 20000,
    so_positivo: bool = False,
) -> dict[str, float]:
    """Último saldo operacional por produto em um depósito (só tabela de ajuste)."""
    from estoque.models import AjusteRapidoEstoque

    dep = (deposito or "").strip().lower()
    if dep not in ("centro", "vila"):
        return {}
    lim = max(50, min(int(limite or 20000), 50000))
    ledger = agro_estoque_ledger_ativo()
    out: dict[str, float] = {}

    if connection.vendor == "postgresql":
        expr = _saldo_expr_sql(ledger)
        where_pos = "WHERE saldo > 0" if so_positivo else ""
        sql = f"""
            SELECT produto_externo_id, saldo FROM (
              SELECT produto_externo_id, {expr} AS saldo FROM (
                SELECT DISTINCT ON (produto_externo_id)
                  produto_externo_id,
                  saldo_informado,
                  saldo_erp_referencia
                FROM estoque_ajusterapidoestoque
                WHERE lower(deposito) = %s
                  AND produto_externo_id IS NOT NULL
                  AND produto_externo_id <> ''
                ORDER BY produto_externo_id, criado_em DESC
              ) u
            ) t
            {where_pos}
            ORDER BY produto_externo_id
            LIMIT %s
        """
        with connection.cursor() as cur:
            cur.execute(sql, [dep, lim])
            for row in cur.fetchall():
                if not row or not row[0]:
                    continue
                try:
                    out[str(row[0]).strip()] = float(row[1] or 0)
                except (TypeError, ValueError):
                    out[str(row[0]).strip()] = 0.0
        return out

    ajustes = (
        AjusteRapidoEstoque.objects.filter(deposito=dep)
        .order_by("produto_externo_id", "-criado_em")
        .only(
            "produto_externo_id",
            "saldo_informado",
            "saldo_erp_referencia",
            "diferenca_saldo",
        )
    )
    seen: set[str] = set()
    for aj in ajustes.iterator(chunk_size=500):
        pid = str(aj.produto_externo_id or "").strip()
        if not pid or pid in seen:
            continue
        seen.add(pid)
        saldo = float(calcular_saldo_operacional_deposito(aj, 0.0, ledger=ledger))
        if so_positivo and not (saldo > 0):
            continue
        out[pid] = saldo
        if len(out) >= lim:
            break
    return out


def filtro_ids_estoque_sinal(
    *,
    loja: str = "total",
    sinal: str,
    limite: int = 20000,
) -> tuple[str, set[str]]:
    """
    Prepara filtro de estoque para o QS do cadastro.

    Retorno:
      - ``(\"in\", ids)`` — manter só esses IDs (positivo/negativo)
      - ``(\"exclude\", ids_nonzero)`` — zerado: excluir quem não é zero no escopo
      - ``(\"noop\", set())`` — sem filtro
    """
    sinal = (sinal or "").strip().lower()
    loja = (loja or "total").strip().lower()
    if sinal not in ("positivo", "negativo", "zero"):
        return "noop", set()
    if loja not in ("total", "centro", "vila"):
        loja = "total"

    so_pos = sinal == "positivo" and loja in ("centro", "vila")
    if so_pos:
        # Um depósito + só positivo: SQL já corta — não perde item por LIMIT cedo.
        m = _mapa_ultimo_saldo_ajuste_por_deposito(loja, limite=limite, so_positivo=True)
        return "in", set(m.keys())

    centro = _mapa_ultimo_saldo_ajuste_por_deposito("centro", limite=limite)
    vila = _mapa_ultimo_saldo_ajuste_por_deposito("vila", limite=limite)
    pids = set(centro) | set(vila)

    saldos: dict[str, float] = {}
    for pid in pids:
        sc = float(centro.get(pid, 0.0))
        sv = float(vila.get(pid, 0.0))
        if loja == "centro":
            saldos[pid] = sc
        elif loja == "vila":
            saldos[pid] = sv
        else:
            saldos[pid] = sc + sv

    if sinal == "positivo":
        return "in", {pid for pid, s in saldos.items() if s > 0}
    if sinal == "negativo":
        return "in", {pid for pid, s in saldos.items() if s < 0}

    nonzero = {pid for pid, s in saldos.items() if abs(s) >= 1e-9}
    return "exclude", nonzero


def produto_ids_saldo_deposito_positivo(
    deposito: str = "vila",
    *,
    limite: int = 1500,
) -> list[str]:
    """IDs com saldo operacional > 0 no depósito (ledger ou ERP+ajuste)."""
    dep = (deposito or "vila").strip().lower()
    lim = max(50, min(int(limite or 1500), 20000))
    loja = dep if dep in ("centro", "vila") else "vila"
    modo, ids = filtro_ids_estoque_sinal(loja=loja, sinal="positivo", limite=lim)
    if modo != "in":
        return []
    return list(ids)[:lim]


def mapa_produtos_info_por_externo_ids(p_ids: list[str]) -> dict[str, dict]:
    """Nome / código / barras leves — overlay em lote (sem N+1 nem row completa do catálogo)."""
    from produtos.catalogo_agro import _overlay_mapa_por_ids
    from produtos.models import Produto

    out: dict[str, dict] = {}
    chunk = 500
    for i in range(0, len(p_ids), chunk):
        slice_ids = [str(x).strip() for x in p_ids[i : i + chunk] if x is not None and str(x).strip()]
        if not slice_ids:
            continue
        ov_map = _overlay_mapa_por_ids(slice_ids)
        for p in Produto.objects.filter(produto_externo_id__in=slice_ids).only(
            "produto_externo_id",
            "nome",
            "codigo_interno",
            "codigo_nfe",
            "codigo_barras",
        ):
            pid = str(p.produto_externo_id or "").strip()
            if not pid:
                continue
            ov = ov_map.get(pid[:64]) or ov_map.get(pid)
            nome = (p.nome or "").strip()
            codigo = (p.codigo_nfe or p.codigo_interno or pid).strip()
            barras = (p.codigo_barras or "").strip()
            if ov:
                if (ov.nome or "").strip():
                    nome = ov.nome.strip()
                if (ov.codigo_nfe or "").strip():
                    codigo = ov.codigo_nfe.strip()
                if (ov.codigo_barras or "").strip():
                    barras = ov.codigo_barras.strip()
            out[pid] = {
                "nome": nome or f"Produto {pid}",
                "codigo": codigo or pid,
                "codigo_barras": barras,
            }
    return out


def saldos_transferencia_de_mapa(
    saldos_map: dict[str, dict[str, float]],
    pid: str,
    ajustes: dict,
) -> tuple[Decimal, Decimal, Decimal, Decimal]:
    """Retorna (saldo_centro, saldo_vila, saldo_centro_erp, saldo_vila_erp)."""
    info = saldos_map.get(pid) or {}
    # Já calculado por mapa_saldos_operacionais_agro — não recalcular.
    if "saldo_centro" in info and "saldo_vila" in info:
        return (
            Decimal(str(info.get("saldo_centro", 0.0))),
            Decimal(str(info.get("saldo_vila", 0.0))),
            Decimal(str(info.get("saldo_erp_centro", 0.0))),
            Decimal(str(info.get("saldo_erp_vila", 0.0))),
        )

    ledger = agro_estoque_ledger_ativo()
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
