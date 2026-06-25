"""Métricas da tela Compras (média/sugestão) via VendaAgro Postgres — desvinculação D4."""
from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any

from django.utils import timezone

logger = logging.getLogger(__name__)


def indice_semana_4(dt: datetime | None, now: datetime) -> int | None:
    """0 = semana mais antiga (dias 28–22), 3 = últimos 7 dias."""
    if dt is None or not isinstance(dt, datetime):
        return None
    if dt < now - timedelta(days=28):
        return None
    if dt >= now - timedelta(days=7):
        return 3
    if dt >= now - timedelta(days=14):
        return 2
    if dt >= now - timedelta(days=21):
        return 1
    return 0


def _naive_local(dt: datetime | None) -> datetime | None:
    if dt is None:
        return None
    if timezone.is_aware(dt):
        return timezone.localtime(dt).replace(tzinfo=None)
    return dt


def metricas_vendas_agregadas_por_produto_postgres(dias_media: int) -> tuple[
    dict[str, float],
    dict[str, float],
    dict[str, float],
    dict[str, list[float]],
]:
    """
    Mesmo contrato que ``_metricas_vendas_agregadas_por_produto`` (Mongo), fonte ``ItemVendaAgro``.
    Ignora vendas devolvidas.
    """
    from produtos.models import ItemVendaAgro

    now = _naive_local(timezone.now()) or datetime.now()
    dias_media = max(7, min(365, int(dias_media or 30)))
    t_m = now - timedelta(days=dias_media)
    t_w0 = now - timedelta(days=7)
    t_w1 = now - timedelta(days=14)
    limite = min(t_m, t_w1, now - timedelta(days=28))

    limite_aware = timezone.make_aware(limite) if timezone.is_naive(limite) else limite

    media_tot: dict[str, float] = {}
    w0: dict[str, float] = {}
    w1: dict[str, float] = {}
    spark: dict[str, list[float]] = {}

    qs = (
        ItemVendaAgro.objects.filter(
            venda__devolvida_em__isnull=True,
            venda__criado_em__gte=limite_aware,
        )
        .exclude(produto_id_externo="")
        .select_related("venda")
        .only("produto_id_externo", "quantidade", "venda__criado_em")
    )

    for item in qs.iterator(chunk_size=1500):
        pid = str(item.produto_id_externo or "").strip()
        if not pid:
            continue
        dt = _naive_local(item.venda.criado_em)
        if dt is None:
            continue
        try:
            qtd = float(item.quantidade or 0)
        except (TypeError, ValueError):
            qtd = 0.0
        if qtd == 0:
            continue
        if dt >= t_m:
            media_tot[pid] = media_tot.get(pid, 0.0) + qtd
        if dt >= t_w0:
            w0[pid] = w0.get(pid, 0.0) + qtd
        if t_w1 <= dt < t_w0:
            w1[pid] = w1.get(pid, 0.0) + qtd
        bi = indice_semana_4(dt, now)
        if bi is not None:
            if pid not in spark:
                spark[pid] = [0.0, 0.0, 0.0, 0.0]
            spark[pid][bi] += qtd

    return media_tot, w0, w1, spark


def metricas_compras_rows_postgres(dias: int) -> dict[str, Any]:
    """Payload JSON alinhado a ``api_pdv_metricas_produtos`` (v2, 12 colunas por linha)."""
    dias = max(7, min(365, int(dias or 30)))
    media_tot, w0, w1, spark_map = metricas_vendas_agregadas_por_produto_postgres(dias)

    pids = set(media_tot.keys()) | set(w0.keys()) | set(w1.keys()) | set(spark_map.keys())
    div = float(dias) if dias else 30.0
    rows: list[list[Any]] = []
    for pid in sorted(pids):
        tot_p = float(media_tot.get(pid, 0.0))
        media_d = round(tot_p / div, 6) if div else 0.0
        s0 = float(w0.get(pid, 0.0))
        s1 = float(w1.get(pid, 0.0))
        if s1 > 0:
            var_pct = round((s0 - s1) / s1 * 100.0, 2)
        elif s0 > 0:
            var_pct = 100.0
        else:
            var_pct = None
        sp = spark_map.get(pid) or [0.0, 0.0, 0.0, 0.0]
        rows.append(
            [
                pid,
                media_d,
                round(tot_p, 4),
                round(s0, 4),
                round(s1, 4),
                var_pct,
                "",
                0.0,
                round(float(sp[0]), 4),
                round(float(sp[1]), 4),
                round(float(sp[2]), 4),
                round(float(sp[3]), 4),
            ]
        )
    return {"v": 2, "dias": dias, "rows": rows, "fonte": "venda_agro_pg"}
