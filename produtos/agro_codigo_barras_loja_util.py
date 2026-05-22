"""
Código de barras interno da loja (embalagem no balcão): prefixo 230 + 10 dígitos sequenciais.
Ex.: 2300000000001, 2300000000002 …
"""

from __future__ import annotations

import logging
import re
from typing import TYPE_CHECKING

from django.http import JsonResponse

if TYPE_CHECKING:
    from pymongo.database import Database

logger = logging.getLogger(__name__)

CB_LOJA_PREFIX = "230"
CB_LOJA_SEQ_LEN = 10
_CB_LOJA_REGEX = re.compile(rf"^{CB_LOJA_PREFIX}\d{{{CB_LOJA_SEQ_LEN}}}$")


def formatar_codigo_barras_loja(seq: int) -> str:
    n = max(1, int(seq))
    return f"{CB_LOJA_PREFIX}{n:0{CB_LOJA_SEQ_LEN}d}"


def parsear_seq_codigo_barras_loja(cb: str) -> int | None:
    d = re.sub(r"\D", "", str(cb or ""))
    if not _CB_LOJA_REGEX.match(d):
        return None
    try:
        return int(d[len(CB_LOJA_PREFIX) :])
    except ValueError:
        return None


def _cb_loja_ocupado(db: Database, col: str, cb: str) -> bool:
    from .models import ProdutoGestaoOverlayAgro, ProdutoMarcaVariacaoAgro

    or_dup = [{fld: cb} for fld in ("CodigoBarras", "CodigoBarrasProduto", "Codigo", "CodigoNFe", "EAN_NFe")]
    try:
        if db[col].find_one({"$or": or_dup}):
            return True
    except Exception:
        logger.warning("cb loja: colisão Mongo", exc_info=True)
        return True
    if ProdutoGestaoOverlayAgro.objects.filter(codigo_barras=cb).exists():
        return True
    if ProdutoMarcaVariacaoAgro.objects.filter(codigo_barras=cb).exists():
        return True
    return False


def _max_seq_cb_loja_catalogo(db: Database, col: str) -> int:
    from .models import ProdutoGestaoOverlayAgro, ProdutoMarcaVariacaoAgro

    max_seq = 0

    def bump(cb_raw: object) -> None:
        nonlocal max_seq
        s = parsear_seq_codigo_barras_loja(str(cb_raw or ""))
        if s is not None and s > max_seq:
            max_seq = s

    for fld in ("CodigoBarras", "CodigoBarrasProduto"):
        try:
            cur = db[col].find(
                {fld: {"$regex": rf"^{CB_LOJA_PREFIX}[0-9]{{{CB_LOJA_SEQ_LEN}}}$"}},
                {fld: 1, "_id": 0},
            )
            for doc in cur:
                bump(doc.get(fld))
        except Exception:
            logger.warning("cb loja: scan Mongo %s", fld, exc_info=True)

    for cb in ProdutoGestaoOverlayAgro.objects.exclude(codigo_barras="").values_list(
        "codigo_barras", flat=True
    ):
        bump(cb)
    for cb in ProdutoMarcaVariacaoAgro.objects.exclude(codigo_barras="").values_list(
        "codigo_barras", flat=True
    ):
        bump(cb)

    return max_seq


def mongo_alocar_proximo_codigo_barras_loja(
    db: Database, col: str
) -> tuple[JsonResponse | None, str | None]:
    """Próximo EAN 230… livre no catálogo (Mongo + overlays Agro)."""
    n = max(1, _max_seq_cb_loja_catalogo(db, col) + 1)
    max_steps = 100_000
    steps = 0
    while steps < max_steps:
        cb = formatar_codigo_barras_loja(n)
        if not _cb_loja_ocupado(db, col, cb):
            return None, cb
        n += 1
        steps += 1
    return (
        JsonResponse(
            {
                "ok": False,
                "erro": (
                    "Não foi possível gerar código de barras da loja (230…): "
                    "faixa esgotada ou muitas tentativas."
                ),
            },
            status=400,
        ),
        None,
    )
