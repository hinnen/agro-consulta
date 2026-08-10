"""Pacote leve de despesas por categoria para a prévia visual do DRE."""
from __future__ import annotations

from typing import Any


def montar_dre_visual(
    *,
    empresa_id: int,
    por: str = "competencia",
    data_inicio=None,
    data_fim=None,
    empresa_nome: str | None = None,
    valor: str = "bruto",
) -> dict[str, Any]:
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
    emprestimos: dict[str, Any] = {"ok": False}
    receita_categorias: dict[str, Any] = {"ok": False}
    if data_inicio and data_fim:
        from financeiro.services.receita_pdv_util import (
            deposito_pdv_por_empresa_id,
            deposito_pdv_por_empresa_nome,
        )
        from produtos.relatorios_vendas_util import receita_categorias_pdv

        dep = deposito_pdv_por_empresa_nome(empresa_nome) or deposito_pdv_por_empresa_id(
            empresa_id
        )
        try:
            receita_categorias = receita_categorias_pdv(
                data_inicio, data_fim, deposito=dep, top=6
            )
        except Exception:
            receita_categorias = {"ok": False}
        if (empresa_nome or "").strip():
            from financeiro.services.dre_emprestimos_util import resumo_emprestimos_pg

            try:
                emprestimos = resumo_emprestimos_pg(
                    empresa_nome=empresa_nome,
                    data_inicio=data_inicio,
                    data_fim=data_fim,
                    por=por or "competencia",
                    valor=valor or "bruto",
                )
            except Exception:
                emprestimos = {"ok": False}
    return {
        "ok": True,
        "variacao": {
            "ok": True,
            "buckets": var.get("buckets") or [],
            "resumo_grupos": var.get("resumo_grupos") or [],
            "total_ultimo_periodo": var.get("total_ultimo_periodo") or 0,
            "top": top,
        },
        "emprestimos": emprestimos,
        "receita_categorias": receita_categorias,
    }
