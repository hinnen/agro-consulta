"""Colunas estruturadas — relatório Vendas por grupo (tela + Excel)."""
from __future__ import annotations

from typing import Any, Callable

from produtos import relatorios_vendas_util as ru


def _period_col_defs(col_grupo: str) -> list[dict[str, Any]]:
    return [
        {"id": "pos", "label": "#", "sort_key": None, "locked": True, "group": None},
        {
            "id": "grupo",
            "label": col_grupo or "Grupo",
            "sort_key": "grupo",
            "locked": True,
            "group": None,
        },
        {"id": "skus", "label": "SKUs", "sort_key": "skus", "group": "detalhe"},
        {"id": "qtd", "label": "Qtd", "sort_key": "qtd", "group": "detalhe"},
        {"id": "ticket", "label": "Ticket méd.", "sort_key": "ticket", "group": "detalhe"},
        {"id": "valor", "label": "Venda R$", "sort_key": "valor", "group": "venda", "picker_hint": "vendas"},
        {"id": "pct", "label": "% venda", "sort_key": "pct", "group": "venda"},
        {"id": "custo", "label": "Custo R$", "sort_key": "custo", "group": "custo"},
        {"id": "margem_rs", "label": "Margem R$", "sort_key": "margem_rs", "group": "margem", "picker_hint": "margem em R$"},
        {
            "id": "margem_pct",
            "label": "Lucro bruto %",
            "sort_key": "margem_pct",
            "group": "margem",
            "picker_hint": "margem em %",
        },
        {
            "id": "pct_margem",
            "label": "% do lucro total",
            "sort_key": "pct_margem",
            "group": "margem",
        },
    ]


def period_table_from_rows(
    rows: list[dict[str, Any]], col_grupo: str
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Retorna (colunas, linhas com cells ordenadas por coluna)."""
    cols = _period_col_defs(col_grupo)
    out_rows: list[dict[str, Any]] = []
    for r in rows:
        qtd_txt = f'{r["qtd"]:.3f}'.rstrip("0").rstrip(".")
        cells = {
            "pos": {"text": str(r["pos"]), "sort": r["pos"]},
            "grupo": {"text": r["grupo"], "sort": (r["grupo"] or "").casefold()},
            "skus": {"text": str(r["skus"]), "sort": r["skus"]},
            "qtd": {"text": qtd_txt, "sort": r["qtd"]},
            "ticket": {"text": ru.fmt_brl(r["ticket"]), "sort": r["ticket"]},
            "valor": {"text": ru.fmt_brl(r["valor"]), "sort": r["valor"]},
            "pct": {"text": f'{r["pct"]}%', "sort": r["pct"]},
            "custo": {"text": ru.fmt_brl(r["custo"]), "sort": r["custo"]},
            "margem_rs": {"text": ru.fmt_brl(r["margem_rs"]), "sort": r["margem_rs"]},
            "margem_pct": {
                "text": f'{r["margem_pct"]}%',
                "sort": r["margem_pct"],
            },
            "pct_margem": {
                "text": f'{r["pct_margem"]}%',
                "sort": r["pct_margem"],
            },
        }
        out_rows.append(
            {
                "alerta": bool(r.get("alerta")),
                "cells": cells,
            }
        )
    return cols, out_rows


def period_flat_headers(cols: list[dict[str, Any]]) -> list[str]:
    return [c["label"] for c in cols]


def period_flat_row(cells: dict[str, dict], cols: list[dict[str, Any]]) -> list[str]:
    return [cells[c["id"]]["text"] for c in cols]


def comparativo_col_defs(mes_labels: list[str]) -> list[dict[str, Any]]:
    cols: list[dict[str, Any]] = [
        {"id": "pos", "label": "#", "sort_key": None, "locked": True, "group": None},
        {
            "id": "grupo",
            "label": "Grupo",
            "sort_key": "grupo",
            "locked": True,
            "group": None,
        },
    ]
    for i, lab in enumerate(mes_labels):
        cols.extend(
            [
                {
                    "id": f"m{i}_valor",
                    "label": f"{lab} Venda",
                    "sort_key": f"m{i}_valor",
                    "group": "venda",
                },
                {
                    "id": f"m{i}_margem_rs",
                    "label": f"{lab} Margem R$",
                    "sort_key": f"m{i}_margem_rs",
                    "group": "margem_rs",
                },
                {
                    "id": f"m{i}_margem_pct",
                    "label": f"{lab} Lucro bruto %",
                    "sort_key": f"m{i}_margem_pct",
                    "group": "margem_pct",
                },
                {
                    "id": f"m{i}_var_venda",
                    "label": f"{lab} Δ% venda",
                    "sort_key": f"m{i}_var_venda",
                    "group": "var_venda",
                },
            ]
        )
    cols.extend(
        [
            {
                "id": "var_total_venda",
                "label": "Δ% venda 1º→últ",
                "sort_key": "var_total_venda",
                "group": "var_total",
            },
            {
                "id": "var_total_margem",
                "label": "Δ% margem 1º→últ",
                "sort_key": "var_total_margem",
                "group": "var_total",
            },
        ]
    )
    return cols


def comparativo_table_from_rows(
    rows: list[dict[str, Any]],
    mes_labels: list[str],
    col_grupo: str,
    fmt_var: Callable[[float | None, bool], str],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    cols = comparativo_col_defs(mes_labels)
    if cols[1]["id"] == "grupo":
        cols[1]["label"] = col_grupo or "Grupo"
    out_rows: list[dict[str, Any]] = []
    for idx, r in enumerate(rows, start=1):
        pos = int(r.get("pos") or idx)
        cells: dict[str, dict[str, Any]] = {
            "pos": {"text": str(pos), "sort": pos},
            "grupo": {
                "text": r["grupo"],
                "sort": (r["grupo"] or "").casefold(),
            },
        }
        for i, c in enumerate(r.get("meses") or []):
            cells[f"m{i}_valor"] = {
                "text": ru.fmt_brl(c["valor"]),
                "sort": c["valor"],
            }
            cells[f"m{i}_margem_rs"] = {
                "text": ru.fmt_brl(c["margem_rs"]),
                "sort": c["margem_rs"],
            }
            mp = c.get("margem_pct")
            cells[f"m{i}_margem_pct"] = {
                "text": f"{mp}%" if mp is not None else "—",
                "sort": mp if mp is not None else -1e9,
            }
            var_v = c.get("var_venda_pct")
            novo_v = bool(c.get("novo_venda"))
            cells[f"m{i}_var_venda"] = {
                "text": fmt_var(var_v, novo_v),
                "sort": None if novo_v or var_v is None else var_v,
            }
        vt = r.get("var_total_venda_pct")
        mt = r.get("var_total_margem_pct")
        cells["var_total_venda"] = {
            "text": fmt_var(vt, False),
            "sort": vt,
        }
        cells["var_total_margem"] = {
            "text": fmt_var(mt, False),
            "sort": mt,
        }
        out_rows.append({"alerta": False, "cells": cells})
    return cols, out_rows


def comparativo_flat_row(cells: dict[str, dict], cols: list[dict[str, Any]]) -> list[str]:
    return [cells[c["id"]]["text"] for c in cols]


def picker_groups_comparativo() -> list[dict[str, str]]:
    return [
        {"id": "venda", "label": "Venda R$ (cada mês)"},
        {"id": "margem_rs", "label": "Margem R$ (cada mês)"},
        {"id": "margem_pct", "label": "Lucro bruto % (cada mês)"},
        {"id": "var_venda", "label": "Δ% venda mês a mês"},
        {"id": "var_total", "label": "Δ% 1º→último"},
    ]
