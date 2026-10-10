"""Promove EAN-13 válido (bip) como principal e guarda legado 230… em opcionais."""
from __future__ import annotations

import logging
from typing import Any

from django.db import transaction

from produtos.agro_codigo_barras_loja_util import (
    ean13_checksum_ok,
    ean13_para_bip_codigo_barras_loja,
    eh_codigo_barras_loja,
)
from produtos.mongo_index_codigos import mesclar_codigos_barras_opcionais_adicionar

logger = logging.getLogger(__name__)


def _digits(cb: str) -> str:
    return "".join(ch for ch in str(cb or "") if ch.isdigit())


def migrar_cb_loja_legado_em_produto(
    produto,
    *,
    overlay=None,
    dry_run: bool = False,
) -> dict[str, Any] | None:
    """
    Se ``codigo_barras`` for 230… legado sem DV: principal = EAN bipável; legado → opcional.
    Retorna resumo da alteração ou None se nada a fazer.
    """
    from produtos.models import ProdutoGestaoOverlayAgro

    pid = str(getattr(produto, "produto_externo_id", "") or "").strip()
    if not pid:
        return None
    if overlay is None:
        overlay = ProdutoGestaoOverlayAgro.objects.filter(produto_externo_id=pid).first()

    atual = _digits(getattr(produto, "codigo_barras", "") or "")
    if not atual and overlay:
        atual = _digits(getattr(overlay, "codigo_barras", "") or "")
    if not eh_codigo_barras_loja(atual) or ean13_checksum_ok(atual):
        return None

    novo = ean13_para_bip_codigo_barras_loja(atual)
    if not novo or novo == atual or not ean13_checksum_ok(novo):
        return None

    res = {
        "produto_externo_id": pid,
        "legado": atual,
        "principal_novo": novo,
        "dry_run": dry_run,
    }
    if dry_run:
        return res

    legado = atual
    with transaction.atomic():
        produto.codigo_barras = novo
        produto.save(update_fields=["codigo_barras"])

        if overlay is None:
            overlay = ProdutoGestaoOverlayAgro.objects.filter(produto_externo_id=pid).first()
        if overlay is not None:
            overlay.codigo_barras = novo
            ce = overlay.cadastro_extras if isinstance(overlay.cadastro_extras, dict) else {}
            ce = dict(ce)
            merged = mesclar_codigos_barras_opcionais_adicionar(
                ce,
                [legado],
                principal=novo,
            )
            if merged:
                ce["codigos_barras_opcionais"] = merged
            overlay.cadastro_extras = ce
            overlay.save(update_fields=["codigo_barras", "cadastro_extras"])

    logger.info("cb loja migrado %s: %s -> %s (legado opcional)", pid, legado, novo)
    return res


def iter_produtos_cb_loja_legado(*, limit: int = 5000):
    from produtos.models import Produto

    n = 0
    qs = Produto.objects.exclude(codigo_barras="").only(
        "id", "produto_externo_id", "codigo_barras"
    )
    for p in qs.iterator(chunk_size=200):
        d = _digits(p.codigo_barras)
        if eh_codigo_barras_loja(d) and not ean13_checksum_ok(d):
            yield p
            n += 1
            if n >= limit:
                break
