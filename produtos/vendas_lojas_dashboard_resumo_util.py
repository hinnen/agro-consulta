"""Resumo read-only do dashboard mobile SistApp (Etapa 1) — só PostgreSQL / VendaAgro.

Reutiliza ``vendas_lojas_totais`` / ``vendas_lojas_total_deposito`` (mesma regra do painel
``/vendas/lojas/painel/``). Não grava nada no banco.
"""
from __future__ import annotations

from datetime import date, datetime, time, timedelta
from decimal import Decimal
from typing import Any

from django.db.models import Count, Sum
from django.db.models.functions import TruncDate
from django.utils import timezone

from produtos.vendas_lojas_util import vendas_lojas_total_deposito, vendas_lojas_totais

# Intervalo máximo aceito pela API (presets 7/30/90/365 + personalizado).
MAX_DIAS_PERIODO = 366

LOJA_ALIASES = {
    "todas": None,
    "all": None,
    "consolidado": None,
    "centro": "centro",
    "vila": "vila",
    "vila_elias": "vila",
}


def _q2(val) -> Decimal:
    if val is None:
        return Decimal("0.00")
    if isinstance(val, Decimal):
        return val.quantize(Decimal("0.01"))
    return Decimal(str(val)).quantize(Decimal("0.01"))


def _money_str(val: Decimal | None) -> str:
    return format(_q2(val), "f")


def normalizar_loja(loja: str | None) -> tuple[str, str | None]:
    """
    Retorna (loja_contrato, deposito_filtro).
    deposito_filtro: None = as duas; 'centro' | 'vila'.
    """
    raw = (loja or "todas").strip().lower()
    if raw not in LOJA_ALIASES:
        raise ValueError("loja inválida; use todas, centro ou vila")
    dep = LOJA_ALIASES[raw]
    if dep is None:
        return "todas", None
    return dep, dep


def parse_periodo(
    inicio: str | None,
    fim: str | None,
    *,
    hoje: date | None = None,
) -> tuple[date, date, str]:
    """Valida YYYY-MM-DD, início ≤ fim, sem futuro, máx. MAX_DIAS_PERIODO dias."""
    hoje = hoje or timezone.localdate()
    if not inicio or not fim:
        raise ValueError("informe inicio e fim no formato YYYY-MM-DD")
    try:
        di = date.fromisoformat(str(inicio).strip()[:10])
        df = date.fromisoformat(str(fim).strip()[:10])
    except ValueError as exc:
        raise ValueError("datas inválidas; use YYYY-MM-DD") from exc
    if di > df:
        raise ValueError("inicio deve ser menor ou igual a fim")
    if di > hoje or df > hoje:
        raise ValueError("não é permitido período com data futura")
    dias = (df - di).days + 1
    if dias > MAX_DIAS_PERIODO:
        raise ValueError(f"intervalo máximo de {MAX_DIAS_PERIODO} dias")
    if di.year < 2000:
        raise ValueError("data inicial fora do intervalo aceito")
    label = f"{di.strftime('%d/%m/%Y')} — {df.strftime('%d/%m/%Y')}"
    return di, df, label


def periodo_equivalente_anterior(data_ini: date, data_fim: date) -> tuple[date, date]:
    dias = (data_fim - data_ini).days + 1
    fim_ant = data_ini - timedelta(days=1)
    ini_ant = fim_ant - timedelta(days=dias - 1)
    return ini_ant, fim_ant


def _faturamento_loja(
    data_ini: date, data_fim: date, deposito: str | None
) -> Decimal:
    return _q2(vendas_lojas_total_deposito(data_ini, data_fim, deposito))


def _contar_vendas(data_ini: date, data_fim: date, deposito: str | None) -> int:
    """Número de cupons no intervalo local (criado_em); não abate devoluções."""
    from produtos.models import VendaAgro

    tz = timezone.get_current_timezone()
    ini = timezone.make_aware(datetime.combine(data_ini, time.min), tz)
    fim = timezone.make_aware(datetime.combine(data_fim, time.max), tz)
    qs = VendaAgro.objects.filter(criado_em__gte=ini, criado_em__lte=fim)
    if deposito == "vila":
        qs = qs.filter(deposito__iexact="vila")
    elif deposito == "centro":
        qs = qs.exclude(deposito__iexact="vila")
    return int(qs.count())


def _serie_diaria(
    data_ini: date, data_fim: date, deposito: str | None
) -> list[dict[str, Any]]:
    """
    Série por dia civil local: vendas no dia − abatimento de devoluções no dia do evento.
    Critério alinhado a ``vendas_lojas_totais`` (soma da série deve conciliar com o cartão).
    """
    from produtos.dashboard_pdv_devolucao_util import (
        abatimento_devolucoes_por_dia,
        aplicar_abatimento_por_dia,
    )
    from produtos.models import VendaAgro

    tz = timezone.get_current_timezone()
    ini = timezone.make_aware(datetime.combine(data_ini, time.min), tz)
    fim = timezone.make_aware(datetime.combine(data_fim, time.max), tz)
    qs = VendaAgro.objects.filter(criado_em__gte=ini, criado_em__lte=fim)
    if deposito == "vila":
        qs = qs.filter(deposito__iexact="vila")
    elif deposito == "centro":
        qs = qs.exclude(deposito__iexact="vila")

    por_dia: dict[str, float] = {}
    qtd_por_dia: dict[str, int] = {}
    for row in (
        qs.annotate(dia=TruncDate("criado_em", tzinfo=tz))
        .values("dia")
        .annotate(soma=Sum("total"), n=Count("id"))
        .order_by("dia")
    ):
        d = row.get("dia")
        if not d:
            continue
        chave = d.isoformat() if hasattr(d, "isoformat") else str(d)[:10]
        por_dia[chave] = float(_q2(row.get("soma")))
        qtd_por_dia[chave] = int(row.get("n") or 0)

    abat = abatimento_devolucoes_por_dia(data_ini, data_fim, deposito=deposito)
    por_dia = aplicar_abatimento_por_dia(por_dia, abat)

    out: list[dict[str, Any]] = []
    cur = data_ini
    while cur <= data_fim:
        k = cur.isoformat()
        fat = _q2(por_dia.get(k, 0))
        out.append(
            {
                "data": k,
                "faturamento": _money_str(fat),
                "numero_vendas": int(qtd_por_dia.get(k, 0)),
            }
        )
        cur += timedelta(days=1)
    return out


def montar_resumo_dashboard(
    *,
    loja: str | None,
    inicio: str | None,
    fim: str | None,
    hoje: date | None = None,
) -> dict[str, Any]:
    """Contrato JSON da Etapa 1 (somente leitura)."""
    hoje = hoje or timezone.localdate()
    loja_id, deposito = normalizar_loja(loja)
    data_ini, data_fim, descricao = parse_periodo(inicio, fim, hoje=hoje)

    centro, vila, total = vendas_lojas_totais(data_ini, data_fim)
    centro, vila, total = _q2(centro), _q2(vila), _q2(total)
    faturamento = total if deposito is None else (vila if deposito == "vila" else centro)

    n_vendas = _contar_vendas(data_ini, data_fim, deposito)
    ticket: Decimal | None = None
    if n_vendas > 0:
        ticket = _q2(faturamento / Decimal(n_vendas))

    ini_ant, fim_ant = periodo_equivalente_anterior(data_ini, data_fim)
    fat_ant = _faturamento_loja(ini_ant, fim_ant, deposito)
    comparativo_pct: float | None = None
    if fat_ant != 0:
        comparativo_pct = float(
            ((faturamento - fat_ant) / fat_ant * Decimal("100")).quantize(Decimal("0.1"))
        )

    serie = _serie_diaria(data_ini, data_fim, deposito)
    soma_serie = _q2(sum(Decimal(p["faturamento"]) for p in serie))
    avisos: list[str] = []
    indisponiveis: list[str] = []
    if soma_serie != faturamento:
        avisos.append(
            "Série diária diverge do cartão por arredondamento ou TruncDate; "
            f"cartão={_money_str(faturamento)} série={_money_str(soma_serie)}. "
            "O cartão usa vendas_lojas_totais (fonte do painel)."
        )

    # Produtos, pagamentos, CMV, metas gravadas etc. ficam para Etapa 2.
    indisponiveis.extend(
        [
            "produtos",
            "categorias",
            "formas_pagamento",
            "movimentacoes_individuais",
            "cmv",
            "despesas",
            "margem",
            "lucro",
            "metas_servidor",
        ]
    )

    atualizado = timezone.localtime()
    return {
        "ok": True,
        "fonte": "postgres",
        "metrica": "faturamento_liquido_pdv",
        "metrica_descricao": (
            "Soma de VendaAgro.total no intervalo local (America/Sao_Paulo), "
            "menos devoluções pelo dia do evento — mesma regra de /vendas/lojas/painel/."
        ),
        "loja": loja_id,
        "periodo": {
            "inicio": data_ini.isoformat(),
            "fim": data_fim.isoformat(),
            "descricao": descricao,
            "dias": (data_fim - data_ini).days + 1,
        },
        "periodo_anterior": {
            "inicio": ini_ant.isoformat(),
            "fim": fim_ant.isoformat(),
        },
        "atualizado_em": atualizado.isoformat(),
        "atualizado_hora": atualizado.strftime("%H:%M"),
        "indicadores": {
            "faturamento": _money_str(faturamento),
            "numero_vendas": n_vendas,
            "ticket_medio": _money_str(ticket) if ticket is not None else None,
            "comparativo_pct": comparativo_pct,
            "faturamento_anterior": _money_str(fat_ant),
        },
        "serie": serie,
        "por_loja": {
            "centro": _money_str(centro),
            "vila": _money_str(vila),
            "total": _money_str(total),
        },
        "avisos": avisos,
        "indisponiveis": indisponiveis,
        "sem_vendas": faturamento == 0 and n_vendas == 0 and soma_serie == 0,
    }
