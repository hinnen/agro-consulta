"""Estoque parado e giro — vendas ``ItemVendaAgro`` + saldos operacionais Agro."""
from __future__ import annotations

import logging
from datetime import timedelta

from django.db.models import Sum
from django.utils import timezone

logger = logging.getLogger(__name__)


def _limite_dias(dias: int):
    return timezone.now() - timedelta(days=max(1, int(dias)))


def _mapa_info_produtos(pids: list[str]) -> dict[str, dict]:
    from produtos.catalogo_agro import produto_agro_para_row
    from produtos.models import Produto

    out: dict[str, dict] = {}
    chunk = 500
    for i in range(0, len(pids), chunk):
        slice_ids = [str(x).strip() for x in pids[i : i + chunk] if str(x).strip()]
        if not slice_ids:
            continue
        for p in Produto.objects.filter(produto_externo_id__in=slice_ids):
            pid = str(p.produto_externo_id or "").strip()
            if not pid:
                continue
            row = produto_agro_para_row(p)
            out[pid] = {
                "nome": (row.get("nome") or f"Produto {pid}").strip(),
                "custo": float(row.get("preco_custo") or 0),
            }
    missing = [x for x in pids if str(x).strip() and str(x).strip() not in out]
    if missing:
        out.update(_mapa_info_produtos_mongo(missing[:800]))
    return out


def _mapa_info_produtos_mongo(pids: list[str]) -> dict[str, dict]:
    from produtos.views import obter_conexao_mongo

    client, db = obter_conexao_mongo()
    if db is None or client is None:
        return {}
    out: dict[str, dict] = {}
    col = getattr(client, "col_p", "DtoProduto")
    chunk = 400
    for i in range(0, len(pids), chunk):
        slice_ids = pids[i : i + chunk]
        try:
            cur = db[col].find(
                {"_id": {"$in": slice_ids}},
                {"Nome": 1, "PrecoCusto": 1, "ValorCusto": 1, "_id": 1},
            )
        except Exception:
            try:
                cur = db[col].find(
                    {"Id": {"$in": slice_ids}},
                    {"Nome": 1, "PrecoCusto": 1, "ValorCusto": 1, "Id": 1},
                )
            except Exception as exc:
                logger.warning("dashboard restrito mongo produtos: %s", exc)
                continue
        for doc in cur:
            pid = str(doc.get("_id") or doc.get("Id") or "").strip()
            if not pid:
                continue
            custo = doc.get("PrecoCusto") or doc.get("ValorCusto") or 0
            try:
                custo_f = float(custo or 0)
            except (TypeError, ValueError):
                custo_f = 0.0
            out[pid] = {
                "nome": (doc.get("Nome") or f"Produto {pid}").strip(),
                "custo": custo_f,
            }
    return out


def _pids_vendidos_desde(dias: int) -> set[str]:
    from produtos.models import ItemVendaAgro

    limite = _limite_dias(dias)
    qs = (
        ItemVendaAgro.objects.filter(
            venda__devolvida_em__isnull=True,
            venda__criado_em__gte=limite,
        )
        .exclude(produto_id_externo="")
        .values_list("produto_id_externo", flat=True)
        .distinct()
    )
    return {str(x).strip() for x in qs if str(x).strip()}


def _candidatos_estoque_positivo() -> list[str]:
    """IDs com saldo operacional C+V > 0 (Mongo estoque + ajustes Agro)."""
    from produtos.estoque_saldo_agro_util import mapa_saldos_operacionais_agro
    from produtos.views import obter_conexao_mongo

    client, db = obter_conexao_mongo()
    pids: set[str] = set()
    if db is not None and client is not None:
        col_e = getattr(client, "col_e", None)
        if col_e:
            try:
                pipeline = [
                    {"$match": {"Saldo": {"$gt": 0}}},
                    {"$group": {"_id": "$ProdutoID"}},
                ]
                for doc in db[col_e].aggregate(pipeline, allowDiskUse=True):
                    pid = str(doc.get("_id") or "").strip()
                    if pid:
                        pids.add(pid)
            except Exception as exc:
                logger.warning("dashboard restrito estoque mongo: %s", exc)

    from estoque.models import AjusteRapidoEstoque

    for pid in (
        AjusteRapidoEstoque.objects.values_list("produto_externo_id", flat=True)
        .distinct()
        .iterator(chunk_size=2000)
    ):
        s = str(pid or "").strip()
        if s:
            pids.add(s)

    if not pids:
        return []

    out: list[str] = []
    chunk = 600
    plist = sorted(pids)
    for i in range(0, len(plist), chunk):
        slice_ids = plist[i : i + chunk]
        saldos = mapa_saldos_operacionais_agro(slice_ids, db=db, client=client)
        for pid in slice_ids:
            info = saldos.get(pid) or {}
            total = float(info.get("saldo_centro") or 0) + float(info.get("saldo_vila") or 0)
            if total > 0.0001:
                out.append(pid)
    return out


def obter_top_giro_30d(limit: int = 5) -> list[dict]:
    from produtos.models import ItemVendaAgro

    limite = _limite_dias(30)
    qs = (
        ItemVendaAgro.objects.filter(
            venda__devolvida_em__isnull=True,
            venda__criado_em__gte=limite,
        )
        .exclude(produto_id_externo="")
        .values("produto_id_externo")
        .annotate(
            total_vendido=Sum("quantidade"),
            receita_gerada=Sum("valor_total"),
        )
        .order_by("-total_vendido")[: max(1, int(limit))]
    )
    rows = list(qs)
    pids = [str(r["produto_id_externo"]) for r in rows]
    info = _mapa_info_produtos(pids)
    out: list[dict] = []
    for r in rows:
        pid = str(r["produto_id_externo"])
        meta = info.get(pid) or {}
        out.append(
            {
                "produto_id": pid,
                "nome": meta.get("nome") or pid,
                "total_vendido": float(r["total_vendido"] or 0),
                "receita_gerada": float(r["receita_gerada"] or 0),
            }
        )
    return out


def obter_estoque_parado_90d(limit: int = 150) -> tuple[list[dict], float]:
    """
    Produtos com estoque C+V > 0 e sem venda PDV nos últimos 90 dias.
    Retorna (linhas ordenadas por valor parado desc, total valor parado).
    """
    vendidos = _pids_vendidos_desde(90)
    candidatos = [p for p in _candidatos_estoque_positivo() if p not in vendidos]
    if not candidatos:
        return [], 0.0

    from produtos.estoque_saldo_agro_util import mapa_saldos_operacionais_agro
    from produtos.views import obter_conexao_mongo

    client, db = obter_conexao_mongo()
    info = _mapa_info_produtos(candidatos)
    linhas: list[dict] = []
    chunk = 600
    for i in range(0, len(candidatos), chunk):
        slice_ids = candidatos[i : i + chunk]
        saldos = mapa_saldos_operacionais_agro(slice_ids, db=db, client=client)
        for pid in slice_ids:
            sinfo = saldos.get(pid) or {}
            estoque = float(sinfo.get("saldo_centro") or 0) + float(
                sinfo.get("saldo_vila") or 0
            )
            if estoque <= 0.0001:
                continue
            meta = info.get(pid) or {}
            custo = float(meta.get("custo") or 0)
            valor_parado = round(estoque * custo, 2)
            linhas.append(
                {
                    "produto_id": pid,
                    "nome": meta.get("nome") or pid,
                    "estoque_atual": round(estoque, 2),
                    "custo": round(custo, 2),
                    "valor_parado": valor_parado,
                }
            )

    linhas.sort(key=lambda x: x["valor_parado"], reverse=True)
    cap = max(1, int(limit))
    linhas = linhas[:cap]
    total = round(sum(x["valor_parado"] for x in linhas), 2)
    return linhas, total


def pacote_dashboard_financeiro_restrito() -> dict:
    parado, total_parado = obter_estoque_parado_90d()
    giro = obter_top_giro_30d(5)
    return {
        "estoque_parado": parado,
        "estoque_parado_total": total_parado,
        "top_giro": giro,
        "gerado_em": timezone.localtime(timezone.now()),
    }
