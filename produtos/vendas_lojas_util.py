"""Totais simples Centro × Vila — só PDV (VendaAgro), sem cache e sem Mongo."""
from __future__ import annotations

from datetime import date, datetime, time, timedelta
from decimal import Decimal


def _parse_iso(s: str | None) -> date | None:
    raw = (s or "").strip()[:10]
    if not raw:
        return None
    try:
        return date.fromisoformat(raw)
    except ValueError:
        return None


def vendas_lojas_periodo_bounds(
    hoje: date,
    periodo: str | None,
    dia_iso: str | None = None,
) -> tuple[date, date, str, str]:
    """
    Dia (padrão) · ontem · semana (segunda até hoje) · mês até hoje ·
    mês anterior (mês civil completo) · ano até hoje · um dia do calendário.
    Período inválido cai em hoje.
    """
    raw = (periodo or "hoje").strip().lower()
    if raw == "ontem":
        d = hoje - timedelta(days=1)
        return d, d, f"Ontem — {d.strftime('%d/%m/%Y')}", "ontem"
    if raw in ("mes_ant", "mes_anterior"):
        primeiro_este = hoje.replace(day=1)
        ultimo = primeiro_este - timedelta(days=1)
        primeiro = ultimo.replace(day=1)
        return primeiro, ultimo, f"Mês anterior — {primeiro.strftime('%m/%Y')}", "mes_ant"
    if raw in ("dia", "data", "calendario"):
        d = _parse_iso(dia_iso) or hoje
        if d > hoje:
            d = hoje
        return d, d, d.strftime("%d/%m/%Y"), "dia"
    if raw == "semana":
        ini = hoje - timedelta(days=hoje.weekday())
        fim = hoje
        label = f"Semana {ini.strftime('%d/%m')} — {fim.strftime('%d/%m/%Y')}"
        return ini, fim, label, "semana"
    if raw == "mes":
        ini = hoje.replace(day=1)
        fim = hoje
        label = f"Mês até {fim.strftime('%d/%m/%Y')}"
        return ini, fim, label, "mes"
    if raw == "ano":
        ini = hoje.replace(month=1, day=1)
        fim = hoje
        label = f"Ano {hoje.year} até {fim.strftime('%d/%m/%Y')}"
        return ini, fim, label, "ano"
    ini = fim = hoje
    label = f"Hoje — {hoje.strftime('%d/%m/%Y')}"
    return ini, fim, label, "hoje"


def _q2(val) -> Decimal:
    if val is None:
        return Decimal("0.00")
    if isinstance(val, Decimal):
        return val.quantize(Decimal("0.01"))
    return Decimal(str(val)).quantize(Decimal("0.01"))


def vendas_lojas_totais(data_ini: date, data_fim: date) -> tuple[Decimal, Decimal, Decimal]:
    """
    Soma PDV no intervalo local (início do dia ini → fim do dia fim).
    Exclui devolvidas. Vila = deposito vila; demais (centro / vazio / outro) = Centro.
    """
    from django.db.models import Sum
    from django.utils import timezone

    from produtos.models import VendaAgro

    tz = timezone.get_current_timezone()
    ini = timezone.make_aware(datetime.combine(data_ini, time.min), tz)
    fim = timezone.make_aware(datetime.combine(data_fim, time.max), tz)
    qs = VendaAgro.objects.filter(
        criado_em__gte=ini,
        criado_em__lte=fim,
        devolvida_em__isnull=True,
    )
    centro = _q2(
        qs.exclude(deposito__iexact="vila").aggregate(soma=Sum("total")).get("soma")
    )
    vila = _q2(
        qs.filter(deposito__iexact="vila").aggregate(soma=Sum("total")).get("soma")
    )
    return centro, vila, (centro + vila).quantize(Decimal("0.01"))


def vendas_lojas_meta_c_soma(
    data_ini: date, data_fim: date, deposito: str | None = None
) -> Decimal:
    """
    Soma da Meta C do BI no intervalo (mesma regra do gráfico).
    ``deposito=centro|vila`` filtra a loja; ``None`` = as duas (como o BI sem filtro).
    """
    from produtos.views import _dashboard_serie_meta_c_vendas

    dep = deposito if deposito in ("centro", "vila") else None
    serie = _dashboard_serie_meta_c_vendas(data_ini, data_fim, deposito=dep)
    return _q2(sum(float(x or 0) for x in serie))


def vendas_lojas_cmp_meta(vendido, esperado) -> dict:
    """Vendido vs média esperada: diferença em R$ e % acima/abaixo."""
    vendido_q = _q2(vendido)
    esperado_q = _q2(esperado)
    if esperado_q <= 0:
        return {
            "esperado": esperado_q,
            "diff": None,
            "pct": None,
            "pct_signed": None,
            "sentido": "sem",
        }
    diff = (vendido_q - esperado_q).quantize(Decimal("0.01"))
    pct_signed = (diff / esperado_q * Decimal("100")).quantize(Decimal("0.1"))
    if abs(diff) < Decimal("0.005"):
        sentido = "igual"
    elif diff > 0:
        sentido = "acima"
    else:
        sentido = "abaixo"
    return {
        "esperado": esperado_q,
        "diff": diff,
        "pct": abs(pct_signed),
        "pct_signed": pct_signed,
        "sentido": sentido,
    }
