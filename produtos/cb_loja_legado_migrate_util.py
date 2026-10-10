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


def preparar_codigo_barras_loja_legado(
    cb: str,
    *,
    produto_externo_id: str = "",
    db=None,
    col: str | None = None,
) -> tuple[str, str | None, str | None]:
    """
    Legado 230… (DV inválido) → principal = EAN bipável; legado fica para opcionais.

    Retorna (principal, legado_opcional, erro). Sem alteração: (cb, None, None).
    """
    from produtos.agro_codigo_barras_loja_util import validar_codigo_barras_loja_para_salvar

    atual = _digits(cb)
    if not eh_codigo_barras_loja(atual) or ean13_checksum_ok(atual):
        return atual, None, None

    novo = ean13_para_bip_codigo_barras_loja(atual)
    if not novo or novo == atual or not ean13_checksum_ok(novo):
        return atual, None, None

    pid = str(produto_externo_id or "").strip()[:64]
    erro = validar_codigo_barras_loja_para_salvar(
        novo,
        produto_externo_id=pid,
        db=db,
        col=col,
    )
    if erro:
        return (
            atual,
            None,
            f"{erro} (cadastro legado {atual} → bip {novo}). "
            "Reatribua o código 230 no produto que já usa o EAN bipável e tente de novo.",
        )
    return novo, atual, None


def mesclar_legado_cb_em_cadastro_extras(
    cadastro_extras: dict | None,
    *,
    legado: str,
    principal: str,
) -> dict:
    ce = dict(cadastro_extras) if isinstance(cadastro_extras, dict) else {}
    merged = mesclar_codigos_barras_opcionais_adicionar(
        ce,
        [legado],
        principal=principal,
    )
    if merged:
        ce["codigos_barras_opcionais"] = merged
    return ce


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

    novo, legado, erro = preparar_codigo_barras_loja_legado(
        atual,
        produto_externo_id=pid,
    )
    if erro:
        return {
            "produto_externo_id": pid,
            "legado": atual,
            "principal_novo": novo,
            "dry_run": dry_run,
            "erro": erro,
        }
    if not legado or novo == atual:
        return None

    res = {
        "produto_externo_id": pid,
        "legado": legado,
        "principal_novo": novo,
        "dry_run": dry_run,
    }
    if dry_run:
        return res

    with transaction.atomic():
        produto.codigo_barras = novo
        produto.save(update_fields=["codigo_barras"])

        if overlay is None:
            overlay = ProdutoGestaoOverlayAgro.objects.filter(produto_externo_id=pid).first()
        if overlay is not None:
            overlay.codigo_barras = novo
            ce = mesclar_legado_cb_em_cadastro_extras(
                overlay.cadastro_extras,
                legado=legado,
                principal=novo,
            )
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
