"""Importação Mongo ``DtoLancamento`` → Postgres ``TituloFinanceiroAgro`` (preparação desvinculação)."""
from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any

from django.db import transaction
from django.utils import timezone

from produtos.models import TituloFinanceiroAgro
from produtos.mongo_financeiro_util import (
    AGRO_FONTE_VERDADE,
    COL_DTO_LANCAMENTO,
    _dto_mongo_val_para_date,
    _dt_efetiva,
    lancamento_para_api,
)

_BATCH = 400


def _dec2(v: object) -> Decimal:
    try:
        return Decimal(str(v or 0)).quantize(Decimal("0.01"))
    except Exception:
        return Decimal("0.00")


def _mongo_dt_para_datetime(v: Any) -> datetime | None:
    if v is None or not _dt_efetiva(v):
        return None
    if timezone.is_naive(v):
        return timezone.make_aware(v, timezone.get_current_timezone())
    return v


def titulo_financeiro_agro_from_mongo_doc(doc: dict) -> TituloFinanceiroAgro | None:
    """Monta instância (não salva) a partir de um documento Mongo."""
    mongo_id = str(doc.get("_id") or "").strip()
    if not mongo_id:
        return None
    despesa = bool(doc.get("Despesa"))
    api = lancamento_para_api(doc, despesa)
    dp = doc.get("DataPagamento")
    dp_date = _dto_mongo_val_para_date(dp) if _dt_efetiva(dp) else None
    last_up = doc.get("LastUpdate") or doc.get("DataModificacao")
    return TituloFinanceiroAgro(
        mongo_id=mongo_id,
        despesa=despesa,
        descricao=str(api.get("descricao") or "")[:500],
        cliente=str(api.get("cliente") or "")[:300],
        cliente_id=str(api.get("cliente_id") or "")[:32],
        numero_documento=str(api.get("numero_documento") or "")[:80],
        parcela=int(api.get("parcela") or 0),
        plano_conta=str(api.get("plano_conta") or "")[:200],
        plano_conta_id=str(api.get("plano_conta_id") or "")[:32],
        grupo=str(api.get("grupo") or "")[:200],
        forma_pagamento=str(api.get("forma_pagamento") or "")[:120],
        forma_pagamento_id=str(api.get("forma_pagamento_id") or "")[:32],
        banco=str(api.get("banco") or "")[:120],
        banco_id=str(api.get("banco_id") or "")[:32],
        centro_custo=str(api.get("centro_custo") or "")[:120],
        empresa=str(api.get("empresa") or "")[:200],
        observacoes=str(api.get("observacoes") or ""),
        valor_bruto=_dec2(api.get("valor_bruto")),
        valor_pago=_dec2(api.get("valor_movimentado")),
        valor_restante=_dec2(api.get("restante")),
        quitado=bool(api.get("pago")),
        data_vencimento=_dto_mongo_val_para_date(doc.get("DataVencimento")),
        data_competencia=_dto_mongo_val_para_date(doc.get("DataCompetencia")),
        data_fluxo=_dto_mongo_val_para_date(doc.get("DataFluxo")),
        data_pagamento=dp_date,
        agro_recorrente=bool(api.get("agro_recorrente")),
        recorrencia_intervalo_meses=max(1, min(int(api.get("recorrencia_intervalo_meses") or 1), 36)),
        agro_recorrente_sempre=bool(api.get("agro_recorrente_sempre")),
        boleto_codigo_barras=str(api.get("boleto_codigo_barras") or "")[:54],
        usuario_lancou=str(api.get("usuario_lancou") or "")[:150],
        usuario_quitou=str(api.get("usuario_quitou") or "")[:150],
        modificado_por=str(api.get("modificado_por") or "")[:200],
        criado_por=str(api.get("criado_por") or "")[:200],
        mongo_congelado=bool(doc.get(AGRO_FONTE_VERDADE)),
        mongo_ultima_atualizacao=_mongo_dt_para_datetime(last_up),
        dados_snapshot_json={
            "mongo_id": mongo_id,
            "last_update": api.get("last_update"),
            "data_modificacao": api.get("data_modificacao"),
        },
    )


def _campos_update() -> list[str]:
    return [
        f.name
        for f in TituloFinanceiroAgro._meta.fields
        if f.name not in ("id", "importado_em", "mongo_id")
    ]


def importar_titulos_financeiro_mongo_para_postgres(
    db,
    *,
    dry_run: bool = True,
    limite: int | None = None,
    despesa: bool | None = None,
) -> dict[str, Any]:
    """Lê ``DtoLancamento`` e (opcionalmente) grava em ``TituloFinanceiroAgro``."""
    if db is None:
        return {"ok": False, "erro": "Mongo indisponível"}

    query: dict[str, Any] = {}
    if despesa is True:
        query["Despesa"] = True
    elif despesa is False:
        query["Despesa"] = False

    col = db[COL_DTO_LANCAMENTO]
    total_mongo = col.count_documents(query)
    cursor = col.find(query)
    if limite and limite > 0:
        cursor = cursor.limit(int(limite))

    stats: dict[str, Any] = {
        "ok": True,
        "dry_run": dry_run,
        "total_mongo": total_mongo,
        "lidos": 0,
        "ignorados_sem_id": 0,
        "criar": 0,
        "atualizar": 0,
        "cp": 0,
        "cr": 0,
        "quitados": 0,
        "abertos": 0,
        "congelados_mongo": 0,
        "bruto_total": Decimal("0.00"),
        "restante_total": Decimal("0.00"),
        "pg_antes": TituloFinanceiroAgro.objects.count(),
        "pg_depois": TituloFinanceiroAgro.objects.count(),
        "erros_amostra": [],
    }

    existentes: set[str] = set(
        TituloFinanceiroAgro.objects.values_list("mongo_id", flat=True)
    )
    pk_por_mongo: dict[str, int] = dict(
        TituloFinanceiroAgro.objects.values_list("mongo_id", "pk")
    )
    batch_novos: list[TituloFinanceiroAgro] = []
    batch_upd: list[TituloFinanceiroAgro] = []
    update_fields = _campos_update()

    def _flush() -> None:
        nonlocal batch_novos, batch_upd
        if dry_run:
            batch_novos = []
            batch_upd = []
            return
        if batch_novos:
            TituloFinanceiroAgro.objects.bulk_create(batch_novos, batch_size=_BATCH)
            batch_novos = []
        if batch_upd:
            TituloFinanceiroAgro.objects.bulk_update(
                batch_upd, update_fields, batch_size=_BATCH
            )
            batch_upd = []

    for doc in cursor:
        stats["lidos"] += 1
        titulo = titulo_financeiro_agro_from_mongo_doc(doc)
        if titulo is None:
            stats["ignorados_sem_id"] += 1
            if len(stats["erros_amostra"]) < 5:
                stats["erros_amostra"].append("documento sem _id")
            continue

        if titulo.despesa:
            stats["cp"] += 1
        else:
            stats["cr"] += 1
        if titulo.quitado:
            stats["quitados"] += 1
        else:
            stats["abertos"] += 1
        if titulo.mongo_congelado:
            stats["congelados_mongo"] += 1
        stats["bruto_total"] += titulo.valor_bruto
        stats["restante_total"] += titulo.valor_restante

        if titulo.mongo_id in existentes:
            stats["atualizar"] += 1
            if not dry_run:
                titulo.pk = pk_por_mongo.get(titulo.mongo_id)
                if titulo.pk:
                    batch_upd.append(titulo)
                    if len(batch_upd) >= _BATCH:
                        _flush()
        else:
            stats["criar"] += 1
            if not dry_run:
                batch_novos.append(titulo)
                existentes.add(titulo.mongo_id)
                if len(batch_novos) >= _BATCH:
                    _flush()

    if not dry_run:
        with transaction.atomic():
            _flush()
        stats["pg_depois"] = TituloFinanceiroAgro.objects.count()

    stats["bruto_total"] = float(stats["bruto_total"])
    stats["restante_total"] = float(stats["restante_total"])
    return stats
