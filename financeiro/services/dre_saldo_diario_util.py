"""Lucro líquido dia a dia (DRE gerencial) — mesma conta do card Lucro Líquido do BI."""
from __future__ import annotations

import calendar
from collections import defaultdict
from datetime import date, timedelta
from decimal import Decimal
from typing import Any

from django.conf import settings
from django.utils import timezone

from financeiro.models import LancamentoFinanceiro as NF
from financeiro.services.receita_pdv_util import (
    deposito_de_loja,
    empresas_ids_para_deposito,
    faturamento_pdv_periodo,
    label_loja_filtro,
    normalizar_loja_filtro,
)

_POR_LUCRO_BI = "vencimento"
_NATS_DESPESA_LIQUIDO = {
    NF.NATUREZA_DESPESA_FIXA,
    NF.NATUREZA_DESPESA_VARIAVEL,
    NF.NATUREZA_DESPESA_FINANCEIRA,
}


def _dec(x) -> Decimal:
    try:
        return Decimal(str(x or 0))
    except Exception:
        return Decimal("0")


def _empresas_nomes_loja(loja: str) -> list[str]:
    from base.models import Empresa

    loja_n = normalizar_loja_filtro(loja)
    dep = deposito_de_loja(loja_n)
    eids = empresas_ids_para_deposito(dep)
    nomes: list[str] = []
    for e in Empresa.objects.filter(pk__in=eids).only("nome_fantasia"):
        n = (e.nome_fantasia or "").strip()
        if n:
            nomes.append(n)
    return nomes


def _deposito_pdv(loja: str) -> str | None:
    dep = deposito_de_loja(normalizar_loja_filtro(loja))
    return dep if dep in ("centro", "vila") else None


def _titulos_dre_por_dia(
    *,
    data_de: date,
    data_ate: date,
    valor: str,
    empresas_nomes: list[str],
    filtro_contas: str = "resultado",
) -> tuple[dict[str, float], dict[str, float]]:
    """Despesas do líquido (fixas+var+fin) e receita não operacional, por dia (vencimento)."""
    from financeiro.services.resumo_operacional_mongo import (
        classificar_despesa_plano,
        classificar_receita_plano,
    )
    from produtos.lancamentos_financeiro_pg_analytics_util import (
        _campo_data_titulo,
        _plano_excluido_dre,
        _titulos_no_periodo_pg,
        _valor_titulo_dre,
    )

    por = _POR_LUCRO_BI
    fc = (filtro_contas or "resultado").strip().lower()
    extra = getattr(settings, "DRE_RESULTADO_EXCLUIR_REGEX_EXTRA", "") or ""
    emp_set = {n.strip().casefold() for n in empresas_nomes if n.strip()}
    titulos = _titulos_no_periodo_pg(
        data_de=data_de, data_ate=data_ate, por=por, status="todos"
    )
    desp_dia: dict[str, Decimal] = defaultdict(lambda: Decimal("0"))
    rec_no_dia: dict[str, Decimal] = defaultdict(lambda: Decimal("0"))
    for t in titulos:
        if fc in ("resultado", "resultado_erp") and _plano_excluido_dre(t.plano_conta, extra):
            continue
        if emp_set:
            emp = str(t.empresa or "").strip().casefold()
            if emp not in emp_set:
                continue
        dt = _campo_data_titulo(t, por)
        if dt is None or dt < data_de or dt > data_ate:
            continue
        val = _valor_titulo_dre(t, valor)
        if val <= Decimal("0.02"):
            continue
        k = dt.isoformat()
        plano = str(t.plano_conta or "")
        if getattr(t, "despesa", False):
            nat = classificar_despesa_plano(plano)
            if nat in _NATS_DESPESA_LIQUIDO:
                desp_dia[k] += val
        else:
            if classificar_receita_plano(plano) == NF.NATUREZA_RECEITA_NAO_OPERACIONAL:
                rec_no_dia[k] += val
    return (
        {k: round(float(v), 2) for k, v in desp_dia.items()},
        {k: round(float(v), 2) for k, v in rec_no_dia.items()},
    )


def _lucro_maps(
    *,
    fetch_ini: date,
    fetch_fim: date,
    loja: str,
    valor: str,
    filtro_contas: str,
) -> dict[str, float]:
    """Lucro/dia = PDV + receita não op − CMV vendida − (fixas+var+fin) — conta do BI."""
    dep = _deposito_pdv(loja)
    empresas = _empresas_nomes_loja(loja)
    fat = faturamento_pdv_periodo(fetch_ini, fetch_fim, deposito=dep)
    pdv = {k: float(v) for k, v in (fat.get("por_dia") or {}).items()}
    from produtos.relatorios_vendas_util import cmv_vendida_por_dia

    cmv = cmv_vendida_por_dia(fetch_ini, fetch_fim, deposito=dep)
    desp, rec_no = _titulos_dre_por_dia(
        data_de=fetch_ini,
        data_ate=fetch_fim,
        valor=valor,
        empresas_nomes=empresas,
        filtro_contas=filtro_contas,
    )
    out: dict[str, float] = {}
    d = fetch_ini
    while d <= fetch_fim:
        k = d.isoformat()
        out[k] = round(
            pdv.get(k, 0.0)
            + rec_no.get(k, 0.0)
            - cmv.get(k, 0.0)
            - desp.get(k, 0.0),
            2,
        )
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

    Mesma conta do card **Lucro Líquido** do BI (vencimento · CMV vendida),
    em **uma** leitura do período — não um DRE por dia.
    """
    del planos_incluir, cmv_modo, por

    hoje = ref or timezone.localdate()
    ano, mes = hoje.year, hoje.month
    ultimo = calendar.monthrange(ano, mes)[1]
    grid_ini = date(ano, mes, 1)
    grid_fim = date(ano, mes, ultimo)

    loja_n = normalizar_loja_filtro(loja)
    look_ini = hoje - timedelta(days=max(int(dias_previsao), 1) - 1)
    fetch_ini = min(grid_ini, look_ini)
    fetch_fim = max(grid_fim, hoje)

    lucro_all = _lucro_maps(
        fetch_ini=fetch_ini,
        fetch_fim=fetch_fim,
        loja=loja_n,
        valor=valor,
        filtro_contas=filtro_contas,
    )
    avg_lucro_dow = _medias_dow_lucro(lucro_all, dias_previsao)

    nomes_dow = ("Seg", "Ter", "Qua", "Qui", "Sex", "Sáb", "Dom")
    dias_out: list[dict[str, Any]] = []
    total_real = 0.0
    total_prev = 0.0
    d = grid_ini
    while d <= grid_fim:
        k = d.isoformat()
        passado = d <= hoje
        lucro = round(lucro_all.get(k, 0.0), 2) if passado else None
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
