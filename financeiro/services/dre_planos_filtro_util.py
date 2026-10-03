"""Filtro de planos de conta (gastos) — DRE gerencial."""
from __future__ import annotations

from typing import Any

from financeiro.services.plano_conta_dre_util import nome_oficial_plano


def parse_planos_gasto_param(raw: str | None) -> list[str] | None:
    """
    ``None`` = todos os planos de despesa.
    Lista explícita = só esses planos entram como gasto no DRE / gráfico.
    """
    if raw is None:
        return None
    s = str(raw).strip()
    if not s or s == "*":
        return None
    out: list[str] = []
    seen: set[str] = set()
    for part in s.split(","):
        nome = nome_oficial_plano(part.strip()) or part.strip()
        if not nome:
            continue
        key = nome.casefold()
        if key in seen:
            continue
        seen.add(key)
        out.append(nome)
    return out or None


def plano_chave(nome: str) -> str:
    return (nome_oficial_plano(nome) or nome or "").strip().casefold()


def plano_despesa_incluido(nome_plano: str, planos_incluir: list[str] | None) -> bool:
    if not planos_incluir:
        return True
    ch = plano_chave(nome_plano)
    incl = {plano_chave(p) for p in planos_incluir}
    return ch in incl


def filtrar_linhas_dre_planos(
    linhas: list[dict[str, Any]] | None,
    planos_incluir: list[str] | None,
) -> list[dict[str, Any]]:
    """Zera despesa das linhas cujo plano não está na seleção (receita intacta)."""
    if not planos_incluir:
        return list(linhas or [])
    out: list[dict[str, Any]] = []
    for row in linhas or []:
        plano = str(row.get("plano") or "")
        des = float(row.get("despesa") or 0)
        rec = float(row.get("receita") or 0)
        if des > 0 and not plano_despesa_incluido(plano, planos_incluir):
            row = {**row, "despesa": 0.0, "saldo": round(rec, 2)}
        out.append(row)
    return out
