"""Pacote leve de despesas por categoria para a prévia visual do DRE."""
from __future__ import annotations

from typing import Any


def montar_dre_visual(*, empresa_id: int, por: str = "competencia") -> dict[str, Any]:
    from financeiro.services.gastos_variacao_pg import gastos_variacao_pg

    var = gastos_variacao_pg(
        empresa_id=empresa_id,
        modo="mes",
        por=por or "competencia",
        top_chart=8,
    )
    if not var.get("ok"):
        return {
            "ok": False,
            "erro": var.get("erro") or "variação indisponível",
            "variacao": {"ok": False},
        }
    top: list[dict[str, Any]] = []
    for row in (var.get("linhas") or [])[:12]:
        vals = row.get("valores") or []
        ultimo = float(vals[-1] if vals else 0)
        top.append(
            {
                "plano": row.get("plano") or row.get("categoria") or "",
                "ultimo": round(ultimo, 2),
                "delta_abs": round(float(row.get("delta_abs") or 0), 2),
                "tendencia": row.get("tendencia") or "flat",
            }
        )
    return {
        "ok": True,
        "variacao": {
            "ok": True,
            "buckets": var.get("buckets") or [],
            "resumo_grupos": var.get("resumo_grupos") or [],
            "total_ultimo_periodo": var.get("total_ultimo_periodo") or 0,
            "top": top,
        },
    }
