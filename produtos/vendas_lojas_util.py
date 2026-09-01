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
    Devolução desconta no dia do evento. Vila = deposito vila; demais = Centro.
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
    )
    centro = _q2(
        qs.exclude(deposito__iexact="vila").aggregate(soma=Sum("total")).get("soma")
    )
    vila = _q2(
        qs.filter(deposito__iexact="vila").aggregate(soma=Sum("total")).get("soma")
    )
    from produtos.dashboard_pdv_devolucao_util import abatimento_devolucoes_totais_loja

    ab_c, ab_v = abatimento_devolucoes_totais_loja(data_ini, data_fim)
    centro = _q2(centro - ab_c)
    vila = _q2(vila - ab_v)
    return centro, vila, (centro + vila).quantize(Decimal("0.01"))


# Expediente da loja — média «até agora» cresce da abertura até o fechamento.
VL_EXPEDIENTE_INI = time(7, 30)
VL_EXPEDIENTE_FIM = time(18, 30)


def vendas_lojas_fracao_expediente(agora: datetime) -> Decimal:
    """
    0 antes de abrir · 1 depois de fechar · linear no meio (7h30–18h30).
    Usado só na média em tempo real; o dia todo continua 100 %.
    """
    t = agora.time().replace(tzinfo=None)
    if t <= VL_EXPEDIENTE_INI:
        return Decimal("0")
    if t >= VL_EXPEDIENTE_FIM:
        return Decimal("1")
    elapsed = datetime.combine(date.min, t) - datetime.combine(date.min, VL_EXPEDIENTE_INI)
    total = datetime.combine(date.min, VL_EXPEDIENTE_FIM) - datetime.combine(
        date.min, VL_EXPEDIENTE_INI
    )
    frac = Decimal(str(elapsed.total_seconds())) / Decimal(str(total.total_seconds()))
    if frac < 0:
        return Decimal("0")
    if frac > 1:
        return Decimal("1")
    return frac.quantize(Decimal("0.0001"))


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


def vendas_lojas_meta_c_modos(
    data_ini: date,
    data_fim: date,
    deposito: str | None,
    *,
    hoje: date,
    agora: datetime,
) -> tuple[Decimal, Decimal, bool]:
    """
    (dia_todo, ate_agora, mostra_toggle).
    Dias fechados entram 100 %. Só o dia de hoje é cortado pelo expediente.
    """
    from produtos.views import _dashboard_serie_meta_c_vendas

    dep = deposito if deposito in ("centro", "vila") else None
    serie = _dashboard_serie_meta_c_vendas(data_ini, data_fim, deposito=dep)
    dia_todo = _q2(sum(float(x or 0) for x in serie))
    inclui_hoje = data_ini <= hoje <= data_fim
    if not inclui_hoje:
        return dia_todo, dia_todo, False
    idx = (hoje - data_ini).days
    if idx < 0 or idx >= len(serie):
        return dia_todo, dia_todo, False
    frac = vendas_lojas_fracao_expediente(agora)
    hoje_val = _q2(serie[idx])
    resto = (dia_todo - hoje_val).quantize(Decimal("0.01"))
    ate_agora = (resto + (hoje_val * frac)).quantize(Decimal("0.01"))
    mostra_toggle = frac < Decimal("1")
    return dia_todo, ate_agora, mostra_toggle


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


def vendas_lojas_cmp_meta_agora(vendido, esperado_agora, esperado_dia) -> dict:
    """Comparação «até agora»: se ainda não abriu, não trata como «sem média»."""
    if _q2(esperado_dia) <= 0:
        return vendas_lojas_cmp_meta(vendido, 0)
    if _q2(esperado_agora) > 0:
        return vendas_lojas_cmp_meta(vendido, esperado_agora)
    vendido_q = _q2(vendido)
    if vendido_q <= 0:
        return {
            "esperado": Decimal("0.00"),
            "diff": Decimal("0.00"),
            "pct": Decimal("0.0"),
            "pct_signed": Decimal("0.0"),
            "sentido": "igual",
        }
    return {
        "esperado": Decimal("0.00"),
        "diff": vendido_q,
        "pct": None,
        "pct_signed": None,
        "sentido": "acima",
    }
