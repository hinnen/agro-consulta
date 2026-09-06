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
    dia_fim_iso: str | None = None,
) -> tuple[date, date, str, str]:
    """
    Dia (padrão) · ontem · semana (segunda até hoje) · mês até hoje ·
    mês anterior (mês civil completo) · ano até hoje · um dia do calendário ·
    intervalo entre dois dias (calendário).
    Período inválido cai em hoje.
    """
    raw = (periodo or "hoje").strip().lower()
    if raw in ("intervalo", "entre"):
        ini = _parse_iso(dia_iso) or hoje
        fim = _parse_iso(dia_fim_iso) or ini
        if ini > hoje:
            ini = hoje
        if fim > hoje:
            fim = hoje
        if ini > fim:
            ini, fim = fim, ini
        if ini == fim:
            return ini, fim, ini.strftime("%d/%m/%Y"), "dia"
        label = f"{ini.strftime('%d/%m/%Y')} — {fim.strftime('%d/%m/%Y')}"
        return ini, fim, label, "intervalo"
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


def vendas_lojas_soma_fiado_vendas_periodo(
    data_ini: date, data_fim: date
) -> tuple[Decimal, Decimal]:
    """Parte fiado das vendas PDV no intervalo (criado_em), por loja."""
    from django.utils import timezone

    from produtos.fiado_credito_util import valor_fiado_venda_local
    from produtos.models import VendaAgro

    tz = timezone.get_current_timezone()
    ini = timezone.make_aware(datetime.combine(data_ini, time.min), tz)
    fim = timezone.make_aware(datetime.combine(data_fim, time.max), tz)
    qs = VendaAgro.objects.filter(criado_em__gte=ini, criado_em__lte=fim).only(
        "pk", "deposito", "pagamentos_json", "forma_pagamento", "total"
    )
    centro = vila = Decimal("0.00")
    for v in qs:
        f = valor_fiado_venda_local(v)
        if (v.deposito or "").strip().lower() == "vila":
            vila += f
        else:
            centro += f
    return _q2(centro), _q2(vila)


def _deposito_fiado_baixa(baixa) -> str:
    titulo = getattr(baixa, "titulo", None)
    if titulo is not None and titulo.venda_agro_id:
        if (titulo.venda_agro.deposito or "").strip().lower() == "vila":
            return "vila"
    sessao = baixa.sessao_caixa
    if sessao is None and getattr(baixa, "movimento_caixa", None):
        sessao = baixa.movimento_caixa.sessao_caixa
    if sessao is not None and getattr(sessao, "ponto_caixa", "") == "vila":
        return "vila"
    return "centro"


def vendas_lojas_fiado_baixas_periodo(
    data_ini: date, data_fim: date
) -> tuple[Decimal, Decimal, Decimal]:
    """Quitaciones de fiado recebidas no intervalo (criado_em da baixa), por loja."""
    from django.utils import timezone

    from produtos.models import FiadoBaixaAgro

    tz = timezone.get_current_timezone()
    ini = timezone.make_aware(datetime.combine(data_ini, time.min), tz)
    fim = timezone.make_aware(datetime.combine(data_fim, time.max), tz)
    qs = FiadoBaixaAgro.objects.filter(criado_em__gte=ini, criado_em__lte=fim).select_related(
        "titulo",
        "titulo__venda_agro",
        "sessao_caixa",
        "movimento_caixa",
        "movimento_caixa__sessao_caixa",
    )
    centro = vila = Decimal("0.00")
    for b in qs:
        val = _q2(b.valor)
        if _deposito_fiado_baixa(b) == "vila":
            vila += val
        else:
            centro += val
    total = (centro + vila).quantize(Decimal("0.01"))
    return _q2(centro), _q2(vila), total


def vendas_lojas_sem_fiado_totais(data_ini: date, data_fim: date) -> tuple[Decimal, Decimal, Decimal]:
    """Vendas do período sem a parte fiado; devolução abate só o não-fiado."""
    from django.db.models import Sum
    from django.utils import timezone

    from produtos.dashboard_pdv_devolucao_util import abatimento_devolucoes_sem_fiado_totais_loja
    from produtos.models import VendaAgro

    tz = timezone.get_current_timezone()
    ini = timezone.make_aware(datetime.combine(data_ini, time.min), tz)
    fim = timezone.make_aware(datetime.combine(data_fim, time.max), tz)
    qs = VendaAgro.objects.filter(criado_em__gte=ini, criado_em__lte=fim)
    centro = _q2(
        qs.exclude(deposito__iexact="vila").aggregate(soma=Sum("total")).get("soma")
    )
    vila = _q2(qs.filter(deposito__iexact="vila").aggregate(soma=Sum("total")).get("soma"))
    cf, vf = vendas_lojas_soma_fiado_vendas_periodo(data_ini, data_fim)
    ab_c, ab_v = abatimento_devolucoes_sem_fiado_totais_loja(data_ini, data_fim)
    centro = _q2(centro - cf - ab_c)
    vila = _q2(vila - vf - ab_v)
    return centro, vila, (centro + vila).quantize(Decimal("0.01"))


def vendas_lojas_sem_fiado_mais_quitacoes_totais(
    data_ini: date, data_fim: date
) -> tuple[Decimal, Decimal, Decimal]:
    """Sem fiado + quitaciones de fiado antigo recebidas no período."""
    c, v, t = vendas_lojas_sem_fiado_totais(data_ini, data_fim)
    bc, bv, bt = vendas_lojas_fiado_baixas_periodo(data_ini, data_fim)
    centro = _q2(c + bc)
    vila = _q2(v + bv)
    return centro, vila, _q2(t + bt)


def vendas_lojas_total_deposito(
    data_ini: date, data_fim: date, deposito: str | None = None
) -> Decimal:
    """Mesmo número do /vendas-lojas, filtrado pela loja do aparelho (ou as duas)."""
    centro, vila, total = vendas_lojas_totais(data_ini, data_fim)
    if deposito == "vila":
        return vila
    if deposito == "centro":
        return centro
    return total


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


def vendas_lojas_meta_c_modos_de_serie(
    serie,
    data_ini: date,
    data_fim: date,
    *,
    hoje: date,
    agora: datetime,
) -> tuple[Decimal, Decimal, bool]:
    """(dia_todo, ate_agora, mostra_toggle) a partir de uma série Meta C já calculada."""
    serie = list(serie or [])
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
    return vendas_lojas_meta_c_modos_de_serie(
        serie, data_ini, data_fim, hoje=hoje, agora=agora
    )


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


def vendas_lojas_ultimo_dia_mes(d: date) -> date:
    """Último dia civil do mês de ``d``."""
    if d.month == 12:
        return date(d.year, 12, 31)
    return date(d.year, d.month + 1, 1) - timedelta(days=1)


def vendas_lojas_previsao_mes(
    *,
    hoje: date,
    agora: datetime,
    deposito: str | None = None,
    serie_mes=None,
) -> dict:
    """
    Previsão do mês civil em tempo real (Centro, Vila ou as duas).

    ritmo = vendido_mês ÷ média esperada até agora (Meta C + expediente).
    previsão = vendido × meta_mês ÷ meta_até_agora.

    Cedo demais (< 2 % da meta do mês) → usa a meta do mês (sem ritmo).
    Mês já fechado (último dia após expediente) → previsão = vendido.
    ``serie_mes`` opcional evita recalcular a Meta C do mês inteiro.
    """
    from produtos.views import _dashboard_serie_meta_c_vendas

    mes_ini = hoje.replace(day=1)
    mes_fim = vendas_lojas_ultimo_dia_mes(hoje)
    dep = deposito if deposito in ("centro", "vila") else None
    vendido = _q2(vendas_lojas_total_deposito(mes_ini, hoje, deposito))
    if serie_mes is None:
        serie_mes = _dashboard_serie_meta_c_vendas(mes_ini, mes_fim, deposito=dep)
    else:
        serie_mes = list(serie_mes)
    meta_mes = _q2(sum(float(x or 0) for x in serie_mes))
    # Até agora = só dias 1…hoje da mesma série (não puxa outra Meta C).
    n_ate = (hoje - mes_ini).days + 1
    serie_ate = serie_mes[: max(0, n_ate)]
    _dia_hoje, meta_ate_agora, _ = vendas_lojas_meta_c_modos_de_serie(
        serie_ate, mes_ini, hoje, hoje=hoje, agora=agora
    )
    meta_ate_agora = _q2(meta_ate_agora)

    mes_fechado = hoje >= mes_fim and vendas_lojas_fracao_expediente(agora) >= Decimal("1")
    if mes_fechado or meta_mes <= 0:
        return {
            "vendido": vendido,
            "meta_mes": meta_mes,
            "meta_ate_agora": meta_ate_agora,
            "previsao": vendido,
            "ritmo": None,
            "ritmo_pct": None,
            "fonte": "fechado" if mes_fechado else "sem_meta",
            "mes_ini": mes_ini,
            "mes_fim": mes_fim,
        }

    limiar = (meta_mes * Decimal("0.02")).quantize(Decimal("0.01"))
    if meta_ate_agora <= 0 or meta_ate_agora < limiar:
        return {
            "vendido": vendido,
            "meta_mes": meta_mes,
            "meta_ate_agora": meta_ate_agora,
            "previsao": meta_mes,
            "ritmo": None,
            "ritmo_pct": None,
            "fonte": "media",
            "mes_ini": mes_ini,
            "mes_fim": mes_fim,
        }

    ritmo = (vendido / meta_ate_agora).quantize(Decimal("0.0001"))
    previsao = (vendido * meta_mes / meta_ate_agora).quantize(Decimal("0.01"))
    ritmo_pct = (ritmo * Decimal("100")).quantize(Decimal("0.1"))
    return {
        "vendido": vendido,
        "meta_mes": meta_mes,
        "meta_ate_agora": meta_ate_agora,
        "previsao": previsao,
        "ritmo": ritmo,
        "ritmo_pct": ritmo_pct,
        "fonte": "ritmo",
        "mes_ini": mes_ini,
        "mes_fim": mes_fim,
    }


def vendas_lojas_previsao_mes_lojas(*, hoje: date, agora: datetime) -> dict:
    """Previsão do mês: Centro, Vila e total (soma das duas previsões)."""
    from produtos.views import _dashboard_serie_meta_c_vendas

    mes_ini = hoje.replace(day=1)
    mes_fim = vendas_lojas_ultimo_dia_mes(hoje)
    serie_c = _dashboard_serie_meta_c_vendas(mes_ini, mes_fim, deposito="centro")
    serie_v = _dashboard_serie_meta_c_vendas(mes_ini, mes_fim, deposito="vila")
    c = vendas_lojas_previsao_mes(
        hoje=hoje, agora=agora, deposito="centro", serie_mes=serie_c
    )
    v = vendas_lojas_previsao_mes(
        hoje=hoje, agora=agora, deposito="vila", serie_mes=serie_v
    )
    total_prev = _q2(c["previsao"] + v["previsao"])
    total_vend = _q2(c["vendido"] + v["vendido"])
    total_meta = _q2(c["meta_mes"] + v["meta_mes"])
    total_agora = _q2(c["meta_ate_agora"] + v["meta_ate_agora"])
    if total_agora > 0 and c["fonte"] == "ritmo" and v["fonte"] == "ritmo":
        ritmo = (total_vend / total_agora).quantize(Decimal("0.0001"))
        ritmo_pct = (ritmo * Decimal("100")).quantize(Decimal("0.1"))
        fonte = "ritmo"
    elif c["fonte"] == "fechado" and v["fonte"] == "fechado":
        ritmo = ritmo_pct = None
        fonte = "fechado"
    elif c["fonte"] == "sem_meta" and v["fonte"] == "sem_meta":
        ritmo = ritmo_pct = None
        fonte = "sem_meta"
    else:
        ritmo = ritmo_pct = None
        fonte = "misto"
    return {
        "centro": c,
        "vila": v,
        "total": {
            "vendido": total_vend,
            "meta_mes": total_meta,
            "meta_ate_agora": total_agora,
            "previsao": total_prev,
            "ritmo": ritmo,
            "ritmo_pct": ritmo_pct,
            "fonte": fonte,
            "mes_ini": c["mes_ini"],
            "mes_fim": c["mes_fim"],
        },
        "aviso_cedo": vendas_lojas_previsao_aviso_cedo(
            agora=agora, fonte_total=fonte
        ),
    }


def vendas_lojas_previsao_aviso_cedo(*, agora: datetime, fonte_total: str) -> bool:
    """
    True se ainda é cedo demais pra confiar no número (manhã ou início do mês).
    Só aviso na tela — não muda a conta.
    """
    if fonte_total in ("media", "sem_meta"):
        return True
    if fonte_total == "fechado":
        return False
    # Antes de ~11h20 (35 % do expediente 7h30–18h30) o ritmo ainda oscila muito.
    return vendas_lojas_fracao_expediente(agora) < Decimal("0.35")

