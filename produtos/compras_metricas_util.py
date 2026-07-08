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


def medias_diarias_por_pids_postgres(pids: list[str], dias: int = 30) -> dict[str, float]:
    """Média diária de vendas (ItemVendaAgro) só para os ids pedidos — busca Compras."""
    from produtos.models import ItemVendaAgro

    variants = [str(x).strip() for x in (pids or []) if str(x).strip()]
    if not variants:
        return {}
    dias = max(7, min(365, int(dias or 30)))
    now = _naive_local(timezone.now()) or datetime.now()
    t_m = now - timedelta(days=dias)
    limite_aware = timezone.make_aware(t_m) if timezone.is_naive(t_m) else t_m
    tot: dict[str, float] = {}
    qs = (
        ItemVendaAgro.objects.filter(
            venda__devolvida_em__isnull=True,
            venda__criado_em__gte=limite_aware,
            produto_id_externo__in=variants[:800],
        )
        .only("produto_id_externo", "quantidade")
    )
    for item in qs.iterator(chunk_size=1500):
        pid = str(item.produto_id_externo or "").strip()
        if not pid:
            continue
        try:
            qtd = float(item.quantidade or 0)
        except (TypeError, ValueError):
            qtd = 0.0
        if qtd == 0:
            continue
        tot[pid] = tot.get(pid, 0.0) + qtd
    div = float(dias) if dias else 30.0
    return {pid: round(tot.get(pid, 0.0) / div, 6) for pid in variants}


def metricas_compras_rows_postgres(dias: int) -> dict[str, Any]:
    """Payload JSON alinhado a ``api_pdv_metricas_produtos`` (v2, 12 colunas por linha)."""
    dias = max(7, min(365, int(dias or 30)))
    media_tot, w0, w1, spark_map = metricas_vendas_agregadas_por_produto_postgres(dias)

    pids = set(media_tot.keys()) | set(w0.keys()) | set(w1.keys()) | set(spark_map.keys())
    ent_map: dict[str, dict[str, Any]] = {}
    if pids:
        try:
            from produtos.views import obter_conexao_mongo

            _, db = obter_conexao_mongo()
            if db is not None:
                from produtos.compras_ultimas_compras_util import ultima_entrada_nf_agro_por_produto_ids

                ent_map = ultima_entrada_nf_agro_por_produto_ids(
                    db, sorted(pids), None, mongo_max_time_ms=25_000
                )
        except Exception as exc:
            logger.warning("metricas_compras_rows_postgres entrada_nf_agro: %s", exc)

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
        ent = ent_map.get(pid) or {}
        rows.append(
            [
                pid,
                media_d,
                round(tot_p, 4),
                round(s0, 4),
                round(s1, 4),
                var_pct,
                ent.get("data") or "",
                float(ent.get("qtd") or 0),
                round(float(sp[0]), 4),
                round(float(sp[1]), 4),
                round(float(sp[2]), 4),
                round(float(sp[3]), 4),
            ]
        )
    return {"v": 2, "dias": dias, "rows": rows, "fonte": "venda_agro_pg"}


def _aware_bounds(desde: datetime, ate: datetime) -> tuple[datetime, datetime]:
    d0 = timezone.make_aware(desde) if timezone.is_naive(desde) else desde
    d1 = timezone.make_aware(ate) if timezone.is_naive(ate) else ate
    return d0, d1


def vendas_qtd_por_produto_intervalo_postgres(
    pid_variants: list[str],
    desde: datetime,
    ate: datetime,
) -> tuple[dict[str, float], dict[str, datetime]]:
    """
    Quantidades vendidas no intervalo [desde, ate] via ``ItemVendaAgro``.
    Chaves = ``produto_id_externo`` (mesmo contrato que Mongo ``DtoVendaProduto``).
    """
    from produtos.models import ItemVendaAgro

    tot: dict[str, float] = {}
    first_dt: dict[str, datetime] = {}
    variants = {str(x).strip() for x in (pid_variants or []) if str(x).strip()}
    if not variants or desde > ate:
        return tot, first_dt

    d0, d1 = _aware_bounds(desde, ate)
    qs = (
        ItemVendaAgro.objects.filter(
            venda__devolvida_em__isnull=True,
            venda__criado_em__gte=d0,
            venda__criado_em__lte=d1,
            produto_id_externo__in=list(variants),
        )
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
        tot[pid] = tot.get(pid, 0.0) + qtd
        prev = first_dt.get(pid)
        if prev is None or dt < prev:
            first_dt[pid] = dt
    return tot, first_dt


def vendas_qtd_apos_ref_compra_postgres(
    ref_por_canon: dict[str, datetime],
    variant_to_canon: dict[str, str],
) -> dict[str, float]:
    """
    Soma vendas **após** a data de última compra por produto (id canônico).
    Mesmo contrato que ``_vendas_qtd_apos_ultima_compra_por_canon`` (Mongo).
    """
    from produtos.models import ItemVendaAgro

    tot: dict[str, float] = {}
    if not ref_por_canon or not variant_to_canon:
        return tot

    ref_n: dict[str, datetime] = {}
    dts: list[datetime] = []
    for canon, raw_d in ref_por_canon.items():
        if not isinstance(raw_d, datetime):
            continue
        d0 = _naive_local(raw_d) or raw_d
        ref_n[str(canon)] = d0
        dts.append(d0)
    if not dts:
        return tot

    desde = min(dts)
    ate = _naive_local(timezone.now()) or datetime.now()
    if desde > ate:
        return tot

    canon_set = set(ref_n.keys())
    inv: dict[str, str] = {}
    for var, canon in variant_to_canon.items():
        if str(canon) in canon_set:
            inv[str(var)] = str(canon)

    d0, d1 = _aware_bounds(desde, ate)
    qs = (
        ItemVendaAgro.objects.filter(
            venda__devolvida_em__isnull=True,
            venda__criado_em__gt=d0,
            venda__criado_em__lte=d1,
            produto_id_externo__in=list(inv.keys())[:8000],
        )
        .select_related("venda")
        .only("produto_id_externo", "quantidade", "venda__criado_em")
    )
    for item in qs.iterator(chunk_size=1500):
        pid = str(item.produto_id_externo or "").strip()
        canon = inv.get(pid)
        if not canon:
            continue
        ref = ref_n.get(canon)
        if ref is None:
            continue
        dt = _naive_local(item.venda.criado_em)
        if dt is None or dt <= ref:
            continue
        try:
            qtd = float(item.quantidade or 0)
        except (TypeError, ValueError):
            qtd = 0.0
        if qtd == 0:
            continue
        tot[canon] = tot.get(canon, 0.0) + qtd
    return tot
