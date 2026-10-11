"""Ritmo «até agora» da média Meta C — hora fechada + histórico PDV (VendaAgro)."""
from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from decimal import Decimal

from django.core.cache import cache
from django.utils import timezone

from produtos.vendas_lojas_util import (
    VL_EXPEDIENTE_FIM,
    _q2,
    vendas_lojas_fracao_expediente,
)

logger = logging.getLogger(__name__)

# Até 12 ocorrências do mesmo weekday (~12 semanas) no PDV (VendaAgro).
VL_RITMO_MAX_DIAS_HIST = 12
VL_RITMO_MIN_TOTAL_DIA = Decimal("80.00")
VL_RITMO_MIN_AMOSTRAS = 2
VL_RITMO_CACHE_SEG = 3600


def vendas_lojas_hora_referencia(agora: datetime) -> int:
    """Hora civil local (0–23) usada no corte «até Nh» (última hora em curso)."""
    if timezone.is_aware(agora):
        agora = timezone.localtime(agora)
    return int(agora.hour)


def vendas_lojas_hora_referencia_label(agora: datetime) -> str:
    h = vendas_lojas_hora_referencia(agora)
    return f"Até {h}h"


def vendas_lojas_nota_ate_agora(agora: datetime) -> str:
    return (
        f"O que já deveria ter saído {vendas_lojas_hora_referencia_label(agora).lower()} "
        f"(padrão do PDV neste dia da semana)"
    )


def _dias_mesmo_weekday_historico(anchor: date, max_n: int = VL_RITMO_MAX_DIAS_HIST) -> list[date]:
    wd = anchor.weekday()
    out: list[date] = []
    d = anchor - timedelta(days=1)
    limite = anchor - timedelta(days=100)
    while len(out) < max_n and d >= limite:
        if d.weekday() == wd:
            out.append(d)
        d -= timedelta(days=1)
    return out


def _passa_loja(deposito: str | None, dep_venda: str | None) -> bool:
    loja = "vila" if str(dep_venda or "").strip().lower() == "vila" else "centro"
    if deposito == "vila":
        return loja == "vila"
    if deposito == "centro":
        return loja == "centro"
    return True


def _como_local(dt: datetime | None) -> datetime:
    if dt is None:
        return datetime.min
    if timezone.is_aware(dt):
        dt = timezone.localtime(dt)
    return dt.replace(tzinfo=None)


def _fracoes_por_dia_historico(
    dias: list[date], hora_ref: int, deposito: str | None
) -> list[float]:
    if not dias:
        return []
    from produtos.relatorios_hora_util import carregar_vendas_intervalo

    d0, d1 = min(dias), max(dias)
    try:
        vendas = carregar_vendas_intervalo(d0, d1)
    except Exception:
        logger.exception("vendas_lojas_ritmo: carregar_vendas_intervalo")
        return []

    dia_set = set(dias)
    totais: dict[date, Decimal] = {}
    acum: dict[date, Decimal] = {}

    for v in vendas:
        if not _passa_loja(deposito, v.get("deposito")):
            continue
        quando = _como_local(v.get("quando"))
        d = quando.date()
        if d not in dia_set:
            continue
        val = _q2(v.get("total") or 0)
        totais[d] = totais.get(d, Decimal("0")) + val
        if quando.hour <= hora_ref:
            acum[d] = acum.get(d, Decimal("0")) + val

    fracs: list[float] = []
    for d in dias:
        t = totais.get(d, Decimal("0"))
        if t < VL_RITMO_MIN_TOTAL_DIA:
            continue
        a = acum.get(d, Decimal("0"))
        fracs.append(float((a / t).quantize(Decimal("0.0001"))))
    return fracs


def vendas_lojas_fracao_meta_ate_agora(
    *,
    hoje: date,
    agora: datetime,
    deposito: str | None = None,
) -> Decimal:
    """
    Fração 0–1 da meta do **dia** ``hoje`` já esperada no relógio local.
    Histórico: mesmos dias da semana no PDV; fallback = expediente linear legado.
    """
    if timezone.is_aware(agora):
        agora_local = timezone.localtime(agora)
    else:
        agora_local = agora

    if agora_local.date() > hoje:
        return Decimal("1")

    if agora_local.date() < hoje:
        return Decimal("1")

    t = agora_local.time().replace(tzinfo=None)
    if t >= VL_EXPEDIENTE_FIM:
        return Decimal("1")

    hora_ref = vendas_lojas_hora_referencia(agora_local)
    dep_key = deposito if deposito in ("centro", "vila") else "todas"
    ck = f"vl:ritmo:v1:{hoje.isoformat()}:{dep_key}:{hora_ref}"
    cached = cache.get(ck)
    if cached is not None:
        try:
            return Decimal(str(cached)).quantize(Decimal("0.0001"))
        except Exception:
            pass

    dias = _dias_mesmo_weekday_historico(hoje)
    fracs = _fracoes_por_dia_historico(dias, hora_ref, deposito)
    if len(fracs) >= VL_RITMO_MIN_AMOSTRAS:
        media = sum(fracs) / len(fracs)
        frac = Decimal(str(media)).quantize(Decimal("0.0001"))
    else:
        frac = vendas_lojas_fracao_expediente(agora_local)

    if frac < Decimal("0"):
        frac = Decimal("0")
    if frac > Decimal("1"):
        frac = Decimal("1")
    cache.set(ck, str(frac), VL_RITMO_CACHE_SEG)
    return frac
