"""Lucro líquido dia a dia (DRE gerencial) — mesma conta do card Lucro Líquido do BI."""
from __future__ import annotations

import calendar
from collections import defaultdict
from datetime import date, timedelta
from typing import Any

from django.utils import timezone

from financeiro.services.receita_pdv_util import (
    deposito_de_loja,
    label_loja_filtro,
    normalizar_loja_filtro,
)

_POR_LUCRO_BI = "vencimento"


def _deposito_bi(loja: str) -> str | None:
    dep = deposito_de_loja(normalizar_loja_filtro(loja))
    return dep if dep in ("centro", "vila") else None


def _lucro_dia_bi(d: date, *, loja: str, valor: str) -> float:
    from financeiro.services.indicadores_gerencial_pg import (
        lucro_liquido_vencimento_bruto_pago,
    )

    pack = lucro_liquido_vencimento_bruto_pago(d, d, deposito=_deposito_bi(loja))
    if not pack.get("ok"):
        return 0.0
    key = "pago" if (valor or "").strip().lower() == "realizado" else "bruto"
    return round(float(pack.get(key) or 0), 2)


def _lucro_por_dia_range(
    data_de: date,
    data_ate: date,
    *,
    loja: str,
    valor: str,
) -> dict[str, float]:
    out: dict[str, float] = {}
    d = data_de
    while d <= data_ate:
        out[d.isoformat()] = _lucro_dia_bi(d, loja=loja, valor=valor)
        d += timedelta(days=1)
    return out


def _medias_dow_lucro(lucros: dict[str, float], dias: int) -> list[float]:
    hoje = timezone.localdate()
    ini = hoje - timedelta(days=max(dias - 1, 0))
    lucros_dow: dict[int, list[float]] = defaultdict(list)
    d = ini
    while d <= hoje:
        lucros_dow[d.weekday()].append(float(lucros.get(d.isoformat()) or 0))
        d += timedelta(days=1)
    avg = [0.0] * 7
    for wd in range(7):
        xs = lucros_dow.get(wd) or [0.0]
        avg[wd] = sum(xs) / len(xs) if xs else 0.0
    return avg


def dre_saldo_diario_mes_pg(
    *,
    loja: str = "todas",
    por: str = "vencimento",
    valor: str = "bruto",
    ref: date | None = None,
    dias_previsao: int = 90,
    filtro_contas: str = "resultado",
    planos_incluir: list[str] | None = None,
    cmv_modo: str = "vendida",
) -> dict[str, Any]:
    """
    Série dia a dia do mês calendário de ``ref`` (default: hoje).

    Cada dia chama ``lucro_liquido_vencimento_bruto_pago`` (mesma função do BI).
    A soma dos dias do mês = card Lucro Líquido do BI no mesmo recorte.
    """
    del planos_incluir, cmv_modo, por, filtro_contas

    hoje = ref or timezone.localdate()
    ano, mes = hoje.year, hoje.month
    ultimo = calendar.monthrange(ano, mes)[1]
    grid_ini = date(ano, mes, 1)
    grid_fim = date(ano, mes, ultimo)

    loja_n = normalizar_loja_filtro(loja)
    look_ini = hoje - timedelta(days=max(int(dias_previsao), 1) - 1)

    lucro_mes = _lucro_por_dia_range(grid_ini, min(hoje, grid_fim), loja=loja_n, valor=valor)
    lucro90 = _lucro_por_dia_range(look_ini, hoje, loja=loja_n, valor=valor)
    avg_lucro_dow = _medias_dow_lucro(lucro90, dias_previsao)

    nomes_dow = ("Seg", "Ter", "Qua", "Qui", "Sex", "Sáb", "Dom")
    dias_out: list[dict[str, Any]] = []
    total_real = 0.0
    total_prev = 0.0
    d = grid_ini
    while d <= grid_fim:
        k = d.isoformat()
        passado = d <= hoje
        lucro = round(lucro_mes.get(k, 0.0), 2) if passado else None
        previsto = round(avg_lucro_dow[d.weekday()], 2)
        if passado and lucro is not None:
            total_real += lucro
        total_prev += previsto
        dias_out.append(
            {
                "data": k,
                "dia": d.day,
                "label": f"{d.day:02d}/{mes:02d} {nomes_dow[d.weekday()]}",
                "lucro": lucro,
                "saldo": lucro,
                "previsto": previsto,
                "futuro": d > hoje,
                "hoje": d == hoje,
            }
        )
        d += timedelta(days=1)

    valor_label = "pago" if (valor or "").strip().lower() == "realizado" else "bruto"
    return {
        "ok": True,
        "fonte": "postgres",
        "metrica": "lucro_liquido",
        "loja": loja_n,
        "loja_label": label_loja_filtro(loja_n),
        "por": _POR_LUCRO_BI,
        "valor": valor,
        "cmv_modo": "vendida",
        "mes": f"{ano}-{mes:02d}",
        "grid_ini": grid_ini.isoformat(),
        "grid_fim": grid_fim.isoformat(),
        "hoje": hoje.isoformat(),
        "dias_previsao": dias_previsao,
        "planos_filtrados": False,
        "dias": dias_out,
        "totais": {
            "lucro_ate_hoje": round(total_real, 2),
            "previsto_lucro_mes": round(total_prev, 2),
            "saldo_real_ate_hoje": round(total_real, 2),
            "previsto_mes": round(total_prev, 2),
        },
        "aviso": (
            "Lucro do dia = mesma conta do card Lucro Líquido do BI "
            f"(vencimento · CMV vendida · {valor_label}). "
            "Previsto = média de lucro por dia da semana nos últimos 90 dias."
        ),
    }
