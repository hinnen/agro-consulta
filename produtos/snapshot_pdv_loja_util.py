"""Cópia pontual Postgres loja → staging (catálogo PDV + overlay + ajustes estoque)."""
from __future__ import annotations

import logging
from typing import Any

import dj_database_url
from django.conf import settings
from django.db import connections, transaction

logger = logging.getLogger(__name__)

_FONTE_ALIAS = "snapshot_fonte_pdv"


def _url_fonte() -> str:
    url = (getattr(settings, "AGRO_SNAPSHOT_FONTE_DATABASE_URL", "") or "").strip()
    if not url:
        raise ValueError(
            "Configure AGRO_SNAPSHOT_FONTE_DATABASE_URL no Render teste "
            "(Internal Database URL do Postgres da loja — SistVale)."
        )
    return url


def _exigir_ambiente_seguro() -> None:
    if not getattr(settings, "AGRO_STAGING_READONLY", False):
        raise ValueError(
            "Abortado: só roda com AGRO_STAGING_READONLY=true (serviço agro-consulta-staging)."
        )
    if not getattr(settings, "AGRO_ERP_PEDIDOS_DRY_RUN", False):
        raise ValueError(
            "Abortado: só roda com AGRO_ERP_PEDIDOS_DRY_RUN=true (nunca na loja)."
        )
    fonte = _url_fonte()
    destino = (getattr(settings, "DATABASE_URL", "") or "").strip()
    if destino and fonte == destino:
        raise ValueError("Fonte e destino iguais — abortado.")


def _registrar_conexao_fonte() -> None:
    url = _url_fonte()
    cfg = dj_database_url.parse(url, conn_max_age=0)
    settings.DATABASES[_FONTE_ALIAS] = cfg
    if _FONTE_ALIAS in connections:
        connections[_FONTE_ALIAS].close()


def _copiar_queryset(model, *, batch: int = 500) -> int:
    """Substitui tabela destino pelos registros da fonte (preserva campos escalares)."""
    n = 0
    model.objects.using("default").all().delete()
    qs = model.objects.using(_FONTE_ALIAS).all().order_by("pk")
    buf: list[Any] = []
    fk_nulos = ("usuario_id", "empresa_id", "loja_id")
    for obj in qs.iterator(chunk_size=batch):
        obj.pk = None
        for fk in fk_nulos:
            if hasattr(obj, fk):
                setattr(obj, fk, None)
        buf.append(obj)
        if len(buf) >= batch:
            model.objects.using("default").bulk_create(buf, batch_size=batch)
            n += len(buf)
            buf = []
    if buf:
        model.objects.using("default").bulk_create(buf, batch_size=batch)
        n += len(buf)
    return n


def executar_snapshot_pdv_loja(*, incluir_ajustes_estoque: bool = True) -> dict[str, Any]:
    """
    Copia catálogo SisVale da loja para o Postgres do staging.

    Tabelas: ``Produto``, ``ProdutoGestaoOverlayAgro`` e, opcional, ``AjusteRapidoEstoque``.
    """
    from estoque.models import AjusteRapidoEstoque
    from produtos.models import Produto, ProdutoGestaoOverlayAgro

    _exigir_ambiente_seguro()
    _registrar_conexao_fonte()

    try:
        connections[_FONTE_ALIAS].ensure_connection()
    except Exception as exc:
        return {"ok": False, "erro": f"Conexão fonte falhou: {exc}"}

    try:
        with transaction.atomic(using="default"):
            n_prod = _copiar_queryset(Produto)
            n_ov = _copiar_queryset(ProdutoGestaoOverlayAgro)
            n_aj = 0
            if incluir_ajustes_estoque:
                n_aj = _copiar_queryset(AjusteRapidoEstoque)
    except Exception as exc:
        logger.exception("snapshot_pdv_loja falhou")
        return {"ok": False, "erro": str(exc)}
    finally:
        if _FONTE_ALIAS in connections:
            connections[_FONTE_ALIAS].close()

    from django.core.cache import cache
    from produtos.views import CATALOGO_PDV_CACHE_ENTRY_KEY, CATALOGO_PDV_CACHE_PREV_ENTRY_KEY

    cache.delete(CATALOGO_PDV_CACHE_ENTRY_KEY)
    cache.delete(CATALOGO_PDV_CACHE_PREV_ENTRY_KEY)

    return {
        "ok": True,
        "produtos": n_prod,
        "overlays": n_ov,
        "ajustes_estoque": n_aj,
        "incluiu_ajustes": bool(incluir_ajustes_estoque),
    }
