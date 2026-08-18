"""Saldo diário do mês (DRE gerencial): vendas PDV − CMV − despesas + previsão 90d."""
from __future__ import annotations

import calendar
from collections import defaultdict
from datetime import date, timedelta
from decimal import Decimal
from typing import Any

from django.conf import settings
from django.utils import timezone

from financeiro.services.dre_planos_filtro_util import plano_despesa_incluido
from financeiro.services.receita_pdv_util import (
    deposito_de_loja,
    empresas_ids_para_deposito,
    faturamento_pdv_periodo,
    label_loja_filtro,
    normalizar_loja_filtro,
)


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


def _despesas_por_dia_pg(
    *,
    data_de: date,
    data_ate: date,
    por: str,
    valor: str,
    empresas_nomes: list[str],
    filtro_contas: str = "resultado",
    planos_incluir: list[str] | None = None,
) -> dict[str, float]:
    from financeiro.models import LancamentoFinanceiro as NF
    from financeiro.services.resumo_operacional_mongo import classificar_despesa_plano
    from produtos.lancamentos_financeiro_pg_analytics_util import (
        _campo_data_titulo,
        _plano_excluido_dre,
        _titulos_no_periodo_pg,
        _valor_titulo_dre,
    )

    fc = (filtro_contas or "resultado").strip().lower()
    extra = getattr(settings, "DRE_RESULTADO_EXCLUIR_REGEX_EXTRA", "") or ""
    emp_set = {n.strip().casefold() for n in empresas_nomes if n.strip()}
    st = "todos" if (por or "").strip().lower() == "pagamento" else "todos"
    titulos = _titulos_no_periodo_pg(
        data_de=data_de, data_ate=data_ate, por=por, despesa=True, status=st
    )
    por_dia: dict[str, Decimal] = defaultdict(lambda: Decimal("0"))
    for t in titulos:
        if fc in ("resultado", "resultado_erp") and _plano_excluido_dre(t.plano_conta, extra):
            continue
        plano = str(t.plano_conta or "")
        nat = classificar_despesa_plano(plano)
        if nat == NF.NATUREZA_CMV:
            continue
        if not plano_despesa_incluido(plano, planos_incluir):
            continue
        if emp_set:
            emp = str(t.empresa or "").strip().casefold()
            if emp not in emp_set:
                continue
        dt = _campo_data_titulo(t, por)
        if dt is None or dt < data_de or dt > data_ate:
            continue
        if (por or "").strip().lower() == "pagamento" and _dec(t.valor_pago) <= 0:
            continue
        val = _valor_titulo_dre(t, valor)
        if val <= Decimal("0.02"):
            continue
        por_dia[dt.isoformat()] += val
    return {k: round(float(v), 2) for k, v in por_dia.items()}


def _cmv_paga_por_dia_pg(
    *,
    data_de: date,
    data_ate: date,
    por: str,
    valor: str,
    empresas_nomes: list[str],
    filtro_contas: str = "resultado",
) -> dict[str, float]:
    from financeiro.models import LancamentoFinanceiro as NF
    from financeiro.services.resumo_operacional_mongo import classificar_despesa_plano
    from produtos.lancamentos_financeiro_pg_analytics_util import (
        _campo_data_titulo,
        _plano_excluido_dre,
        _titulos_no_periodo_pg,
        _valor_titulo_dre,
    )

    fc = (filtro_contas or "resultado").strip().lower()
    extra = getattr(settings, "DRE_RESULTADO_EXCLUIR_REGEX_EXTRA", "") or ""
    emp_set = {n.strip().casefold() for n in empresas_nomes if n.strip()}
    st = "todos" if (por or "").strip().lower() == "pagamento" else "todos"
    titulos = _titulos_no_periodo_pg(
        data_de=data_de, data_ate=data_ate, por=por, despesa=True, status=st
    )
    por_dia: dict[str, Decimal] = defaultdict(lambda: Decimal("0"))
    for t in titulos:
        if fc in ("resultado", "resultado_erp") and _plano_excluido_dre(t.plano_conta, extra):
            continue
        if classificar_despesa_plano(str(t.plano_conta or "")) != NF.NATUREZA_CMV:
            continue
        if emp_set:
            emp = str(t.empresa or "").strip().casefold()
            if emp not in emp_set:
                continue
        dt = _campo_data_titulo(t, por)
        if dt is None or dt < data_de or dt > data_ate:
            continue
        if (por or "").strip().lower() == "pagamento" and _dec(t.valor_pago) <= 0:
            continue
        val = _valor_titulo_dre(t, valor)
        if val <= Decimal("0.02"):
            continue
        por_dia[dt.isoformat()] += val
    return {k: round(float(v), 2) for k, v in por_dia.items()}


def _cmv_por_dia(
    *,
    data_de: date,
    data_ate: date,
    deposito: str | None,
    cmv_modo: str,
    por: str,
    valor: str,
    empresas_nomes: list[str],
    filtro_contas: str,
) -> dict[str, float]:
    modo = (cmv_modo or "vendida").strip().lower()
    if modo == "paga":
        return _cmv_paga_por_dia_pg(
            data_de=data_de,
            data_ate=data_ate,
            por=por,
            valor=valor,
            empresas_nomes=empresas_nomes,
            filtro_contas=filtro_contas,
        )
    from produtos.relatorios_vendas_util import cmv_vendida_por_dia

    return cmv_vendida_por_dia(data_de, data_ate, deposito=deposito)


def _medias_dow_trio(
    vendas: dict[str, float],
    despesas: dict[str, float],
    cmv: dict[str, float],
    dias: int,
) -> tuple[list[float], list[float], list[float]]:
    hoje = timezone.localdate()
    ini = hoje - timedelta(days=max(dias - 1, 0))
    vendas_dow: dict[int, list[float]] = defaultdict(list)
    despesas_dow: dict[int, list[float]] = defaultdict(list)
    cmv_dow: dict[int, list[float]] = defaultdict(list)
    d = ini
    while d <= hoje:
        k = d.isoformat()
        vendas_dow[d.weekday()].append(float(vendas.get(k) or 0))
        despesas_dow[d.weekday()].append(float(despesas.get(k) or 0))
        cmv_dow[d.weekday()].append(float(cmv.get(k) or 0))
        d += timedelta(days=1)
    avg_v = [0.0] * 7
    avg_d = [0.0] * 7
    avg_c = [0.0] * 7
    for wd in range(7):
        vs = vendas_dow.get(wd) or [0.0]
        ds = despesas_dow.get(wd) or [0.0]
        cs = cmv_dow.get(wd) or [0.0]
        avg_v[wd] = sum(vs) / len(vs) if vs else 0.0
        avg_d[wd] = sum(ds) / len(ds) if ds else 0.0
        avg_c[wd] = sum(cs) / len(cs) if cs else 0.0
    return avg_v, avg_d, avg_c


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

    * saldo_real = vendas PDV − CMV − despesas (planos selecionados na tela)
    * previsto = média por dia da semana nos últimos ``dias_previsao`` dias
    """
    hoje = ref or timezone.localdate()
    ano, mes = hoje.year, hoje.month
    ultimo = calendar.monthrange(ano, mes)[1]
    grid_ini = date(ano, mes, 1)
    grid_fim = date(ano, mes, ultimo)

    loja_n = normalizar_loja_filtro(loja)
    dep = deposito_de_loja(loja_n)
    empresas = _empresas_nomes_loja(loja_n)

    fat = faturamento_pdv_periodo(grid_ini, grid_fim, deposito=dep)
    vendas_map = {k: float(v) for k, v in (fat.get("por_dia") or {}).items()}
    despesas_map = _despesas_por_dia_pg(
        data_de=grid_ini,
        data_ate=grid_fim,
        por=por,
        valor=valor,
        empresas_nomes=empresas,
        filtro_contas=filtro_contas,
        planos_incluir=planos_incluir,
    )
    cmv_map = _cmv_por_dia(
        data_de=grid_ini,
        data_ate=grid_fim,
        deposito=dep,
        cmv_modo=cmv_modo,
        por=por,
        valor=valor,
        empresas_nomes=empresas,
        filtro_contas=filtro_contas,
    )

    look_ini = hoje - timedelta(days=max(int(dias_previsao), 1) - 1)
    fat90 = faturamento_pdv_periodo(look_ini, hoje, deposito=dep)
    vendas90 = {k: float(v) for k, v in (fat90.get("por_dia") or {}).items()}
    despesas90 = _despesas_por_dia_pg(
        data_de=look_ini,
        data_ate=hoje,
        por=por,
        valor=valor,
        empresas_nomes=empresas,
        filtro_contas=filtro_contas,
        planos_incluir=planos_incluir,
    )
    cmv90 = _cmv_por_dia(
        data_de=look_ini,
        data_ate=hoje,
        deposito=dep,
        cmv_modo=cmv_modo,
        por=por,
        valor=valor,
        empresas_nomes=empresas,
        filtro_contas=filtro_contas,
    )
    avg_v_dow, avg_d_dow, avg_c_dow = _medias_dow_trio(
        vendas90, despesas90, cmv90, dias_previsao
    )

    nomes_dow = ("Seg", "Ter", "Qua", "Qui", "Sex", "Sáb", "Dom")
    dias_out: list[dict[str, Any]] = []
    total_real = 0.0
    total_prev = 0.0
    d = grid_ini
    while d <= grid_fim:
        k = d.isoformat()
        vendas = round(vendas_map.get(k, 0.0), 2)
        desp = round(despesas_map.get(k, 0.0), 2)
        cmv = round(cmv_map.get(k, 0.0), 2)
        saldo = round(vendas - cmv - desp, 2)
        prev_v = round(avg_v_dow[d.weekday()], 2)
        prev_d = round(avg_d_dow[d.weekday()], 2)
        prev_c = round(avg_c_dow[d.weekday()], 2)
        previsto = round(prev_v - prev_c - prev_d, 2)
        futuro = d > hoje
        passado = d <= hoje
        if passado:
            total_real += saldo
        total_prev += previsto
        dias_out.append(
            {
                "data": k,
                "dia": d.day,
                "label": f"{d.day:02d}/{mes:02d} {nomes_dow[d.weekday()]}",
                "vendas": vendas if passado else None,
                "cmv": cmv if passado else None,
                "despesas": desp if passado else None,
                "saldo": saldo if passado else None,
                "previsto": previsto,
                "futuro": futuro,
                "hoje": d == hoje,
            }
        )
        d += timedelta(days=1)

    cmv_label = "mercadoria paga" if (cmv_modo or "").strip().lower() == "paga" else "mercadoria vendida"
    return {
        "ok": True,
        "fonte": "postgres",
        "loja": loja_n,
        "loja_label": label_loja_filtro(loja_n),
        "por": por,
        "valor": valor,
        "cmv_modo": (cmv_modo or "vendida").strip().lower(),
        "mes": f"{ano}-{mes:02d}",
        "grid_ini": grid_ini.isoformat(),
        "grid_fim": grid_fim.isoformat(),
        "hoje": hoje.isoformat(),
        "dias_previsao": dias_previsao,
        "planos_filtrados": bool(planos_incluir),
        "dias": dias_out,
        "totais": {
            "saldo_real_ate_hoje": round(total_real, 2),
            "previsto_mes": round(total_prev, 2),
        },
        "aviso": (
            "Saldo do dia = vendas PDV − CMV (" + cmv_label + ") − despesas dos planos selecionados. "
            "Previsto = média por dia da semana nos últimos 90 dias."
        ),
    }
