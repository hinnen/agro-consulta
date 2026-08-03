"""Relatórios de inventário / estoque valorizado (Central de Relatórios).

Saldo = Agro operacional (`mapa_saldos_operacionais_agro`).
Custo / venda = cadastro PG + overlay (`produto_agro_para_row`).
"""
from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)

TELA_LIMITE = 500
EXPORT_MAX = 12000
CHUNK_SALDO = 600
CHUNK_OV = 500


def _f(v: Any) -> float:
    try:
        return float(v or 0)
    except (TypeError, ValueError):
        return 0.0


def _norm(s: Any) -> str:
    return str(s or "").strip()


def parse_filtros_inventario(request_get) -> dict:
    """Lê GET: deposito, categoria, marca, so_saldo, ativos, q."""
    deposito = _norm(request_get.get("deposito") or "ambos").lower()
    if deposito not in ("centro", "vila", "ambos"):
        deposito = "ambos"
    ativos = _norm(request_get.get("ativos") or "ativos").lower()
    if ativos not in ("ativos", "inativos", "todos"):
        ativos = "ativos"
    so_saldo = _norm(request_get.get("so_saldo") or "1") not in ("0", "false", "nao", "não")
    return {
        "deposito": deposito,
        "categoria": _norm(request_get.get("categoria")),
        "marca": _norm(request_get.get("marca")),
        "ativos": ativos,
        "so_saldo": so_saldo,
        "q": _norm(request_get.get("q")),
    }


def _saldo_relevante(row: dict, deposito: str) -> float:
    if deposito == "centro":
        return _f(row.get("saldo_centro"))
    if deposito == "vila":
        return _f(row.get("saldo_vila"))
    return _f(row.get("saldo_total"))


def _overlay_mapa(pids: list[str]) -> dict:
    from produtos.models import ProdutoGestaoOverlayAgro

    out = {}
    for i in range(0, len(pids), CHUNK_OV):
        chunk = [p[:64] for p in pids[i : i + CHUNK_OV] if p]
        if not chunk:
            continue
        for ov in ProdutoGestaoOverlayAgro.objects.filter(produto_externo_id__in=chunk):
            out[_norm(ov.produto_externo_id)] = ov
    return out


def _ov_float(ov, attr: str) -> float | None:
    if ov is None:
        return None
    raw = getattr(ov, attr, None)
    if raw is None:
        return None
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None


def coletar_linhas_inventario(
    *,
    deposito: str = "ambos",
    categoria: str = "",
    marca: str = "",
    ativos: str = "ativos",
    so_saldo: bool = True,
    q: str = "",
) -> dict:
    """Linhas completas + totais do inventário valorizado.

    Retorna:
      linhas, totais, categorias (faceta), marcas (faceta), truncado_export
    """
    from django.db.models import Q

    from produtos.catalogo_agro import produto_agro_para_row
    from produtos.estoque_saldo_agro_util import mapa_saldos_operacionais_agro
    from produtos.models import Produto
    from produtos.views import obter_conexao_mongo

    deposito = deposito if deposito in ("centro", "vila", "ambos") else "ambos"
    qs = Produto.objects.all().order_by("nome")
    if ativos == "ativos":
        qs = qs.filter(ativo=True, cadastro_inativo=False)
    elif ativos == "inativos":
        qs = qs.filter(Q(ativo=False) | Q(cadastro_inativo=True)).distinct().order_by("nome")

    cat_f = _norm(categoria)
    marca_f = _norm(marca)
    q_f = _norm(q).lower()

    produtos = list(qs[: EXPORT_MAX + 1])
    truncado_export = len(produtos) > EXPORT_MAX
    produtos = produtos[:EXPORT_MAX]

    pids = []
    for p in produtos:
        pid = _norm(p.produto_externo_id or p.erp_produto_id or str(p.pk))
        if pid:
            pids.append(pid)

    ov_map = _overlay_mapa(pids)

    client, db = None, None
    try:
        from produtos.agro_fonte_config import agro_estoque_operacional_sem_mongo_erp

        if not agro_estoque_operacional_sem_mongo_erp():
            client, db = obter_conexao_mongo()
    except Exception:
        try:
            client, db = obter_conexao_mongo()
        except Exception:
            client, db = None, None

    saldos_all: dict[str, dict] = {}
    for i in range(0, len(pids), CHUNK_SALDO):
        chunk = pids[i : i + CHUNK_SALDO]
        try:
            saldos_all.update(mapa_saldos_operacionais_agro(chunk, db=db, client=client))
        except Exception as exc:
            logger.warning("inventario saldos chunk: %s", exc)

    cats: set[str] = set()
    marcas: set[str] = set()
    linhas: list[dict] = []

    for p in produtos:
        pid = _norm(p.produto_externo_id or p.erp_produto_id or str(p.pk))
        if not pid:
            continue
        ov = ov_map.get(pid)
        row = produto_agro_para_row(p, ov, resolver_overlay_faltante=False)
        nome = _norm(row.get("nome")) or f"Produto {pid}"
        cat = _norm(row.get("categoria")) or "Sem categoria"
        mar = _norm(row.get("marca")) or "Sem marca"
        cats.add(cat)
        marcas.add(mar)

        if cat_f and cat != cat_f:
            continue
        if marca_f and mar != marca_f:
            continue
        if q_f:
            blob = " ".join(
                [
                    nome,
                    _norm(row.get("codigo")),
                    _norm(row.get("codigo_nfe")),
                    _norm(row.get("codigo_barras")),
                    cat,
                    mar,
                ]
            ).lower()
            if q_f not in blob:
                continue

        s = saldos_all.get(pid) or {}
        sc = round(_f(s.get("saldo_centro")), 3)
        sv = round(_f(s.get("saldo_vila")), 3)
        st = round(sc + sv, 3)
        line = {
            "produto_id": pid,
            "codigo_gm": _norm(row.get("codigo_nfe")) or _norm(row.get("codigo")),
            "codigo_sistema": _norm(row.get("codigo")),
            "nome": nome,
            "codigo_barras": _norm(row.get("codigo_barras")),
            "categoria": cat,
            "marca": mar,
            "unidade": _norm(row.get("unidade")) or "UN",
            "saldo_centro": sc,
            "saldo_vila": sv,
            "saldo_total": st,
            "estoque_min_centro": _ov_float(ov, "estoque_min_centro"),
            "estoque_max_centro": _ov_float(ov, "estoque_max_centro"),
            "estoque_min_vila": _ov_float(ov, "estoque_min_vila"),
            "estoque_max_vila": _ov_float(ov, "estoque_max_vila"),
            "custo": round(_f(row.get("preco_custo")), 4),
            "preco_venda": round(_f(row.get("preco_venda")), 4),
            "ativo": not bool(row.get("inativo")),
        }
        sal_rel = _saldo_relevante(line, deposito)
        line["saldo_relevante"] = round(sal_rel, 3)
        line["valor_custo"] = round(sal_rel * line["custo"], 2)
        line["valor_venda"] = round(sal_rel * line["preco_venda"], 2)

        if so_saldo and abs(sal_rel) < 0.0001:
            continue
        linhas.append(line)

    linhas.sort(key=lambda r: (-r["valor_custo"], r["nome"].lower()))

    total_custo = round(sum(r["valor_custo"] for r in linhas), 2)
    total_venda = round(sum(r["valor_venda"] for r in linhas), 2)
    com_saldo = sum(1 for r in linhas if abs(r["saldo_relevante"]) >= 0.0001)
    margem_rs = round(total_venda - total_custo, 2)
    margem_pct = round((margem_rs / total_venda) * 100, 1) if total_venda > 0.009 else 0.0

    for i, r in enumerate(linhas, start=1):
        r["pos"] = i

    return {
        "linhas": linhas,
        "totais": {
            "skus": len(linhas),
            "com_saldo": com_saldo,
            "valor_custo": total_custo,
            "valor_venda": total_venda,
            "margem_rs": margem_rs,
            "margem_pct": margem_pct,
            "deposito": deposito,
        },
        "categorias": sorted(cats),
        "marcas": sorted(marcas),
        "truncado_export": truncado_export,
    }


def inventario_min_max(
    pacote: dict,
    *,
    modo: str = "abaixo",
) -> list[dict]:
    """Abaixo do mínimo ou acima do máximo (Centro e/ou Vila)."""
    modo = modo if modo in ("abaixo", "acima", "ambos") else "abaixo"
    out: list[dict] = []
    for r in pacote.get("linhas") or []:
        hits: list[str] = []
        sc, sv = r["saldo_centro"], r["saldo_vila"]
        mn_c, mx_c = r.get("estoque_min_centro"), r.get("estoque_max_centro")
        mn_v, mx_v = r.get("estoque_min_vila"), r.get("estoque_max_vila")

        if modo in ("abaixo", "ambos"):
            if mn_c is not None and sc < mn_c - 0.0001:
                hits.append(f"Centro abaixo (saldo {sc} < mín {mn_c})")
            if mn_v is not None and sv < mn_v - 0.0001:
                hits.append(f"Vila abaixo (saldo {sv} < mín {mn_v})")
        if modo in ("acima", "ambos"):
            if mx_c is not None and sc > mx_c + 0.0001:
                hits.append(f"Centro acima (saldo {sc} > máx {mx_c})")
            if mx_v is not None and sv > mx_v + 0.0001:
                hits.append(f"Vila acima (saldo {sv} > máx {mx_v})")

        if not hits:
            # Sem meta cadastrada: não entra
            continue
        # Se só pediu abaixo e só bateu acima (ou vice-versa) — já filtrado por modo
        out.append(
            {
                **r,
                "alerta": " · ".join(hits),
                "min_centro": mn_c,
                "max_centro": mx_c,
                "min_vila": mn_v,
                "max_vila": mx_v,
            }
        )
    for i, r in enumerate(out, start=1):
        r["pos"] = i
    return out


def inventario_resumo(pacote: dict, *, agrupar: str = "categoria") -> list[dict]:
    agrupar = agrupar if agrupar in ("categoria", "marca", "unidade") else "categoria"
    key_name = {"categoria": "categoria", "marca": "marca", "unidade": "unidade"}[agrupar]
    buckets: dict[str, dict] = {}
    for r in pacote.get("linhas") or []:
        k = _norm(r.get(key_name)) or f"Sem {agrupar}"
        b = buckets.setdefault(
            k,
            {
                "grupo": k,
                "skus": 0,
                "saldo": 0.0,
                "valor_custo": 0.0,
                "valor_venda": 0.0,
            },
        )
        b["skus"] += 1
        b["saldo"] = round(b["saldo"] + _f(r.get("saldo_relevante")), 3)
        b["valor_custo"] = round(b["valor_custo"] + _f(r.get("valor_custo")), 2)
        b["valor_venda"] = round(b["valor_venda"] + _f(r.get("valor_venda")), 2)

    total_venda = sum(b["valor_venda"] for b in buckets.values()) or 0.0
    out = sorted(buckets.values(), key=lambda x: (-x["valor_custo"], x["grupo"].lower()))
    for i, r in enumerate(out, start=1):
        r["pos"] = i
        r["pct"] = (
            round((r["valor_venda"] / total_venda) * 100, 1) if total_venda > 0.009 else 0.0
        )
    return out


def inventario_sem_custo(pacote: dict) -> list[dict]:
    out = [
        r
        for r in (pacote.get("linhas") or [])
        if abs(_f(r.get("saldo_relevante"))) >= 0.0001 and _f(r.get("custo")) < 0.0001
    ]
    for i, r in enumerate(out, start=1):
        r["pos"] = i
    return out


def inventario_zerados(pacote: dict, *, modo: str = "zerados") -> list[dict]:
    """modo: zerados | negativos | ambos — usa saldo do depósito filtrado no pacote."""
    modo = modo if modo in ("zerados", "negativos", "ambos") else "zerados"
    out: list[dict] = []
    for r in pacote.get("linhas") or []:
        # Pacote com so_saldo=True já excluiu zeros — recolher sem so_saldo
        sal = _f(r.get("saldo_relevante"))
        if modo == "zerados" and abs(sal) < 0.0001:
            out.append(r)
        elif modo == "negativos" and sal < -0.0001:
            out.append(r)
        elif modo == "ambos" and sal <= 0.0001:
            out.append(r)
    for i, r in enumerate(out, start=1):
        r["pos"] = i
    return out


def fmt_num(v: float | None, casas: int = 2) -> str:
    if v is None:
        return "—"
    try:
        n = float(v)
    except (TypeError, ValueError):
        return "—"
    s = f"{n:,.{casas}f}"
    return s.replace(",", "X").replace(".", ",").replace("X", ".")
