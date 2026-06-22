"""Próximo código sistema + GM para produto novo (Postgres e espelho Mongo)."""
from __future__ import annotations

import logging
import os

from django.db.models import Q

from produtos.models import Produto

logger = logging.getLogger(__name__)


def extrair_numero_sequencial(val) -> int | None:
    s = str(val or "").strip()
    if not s or s.lower() == "__novo__":
        return None
    if s.upper().startswith("GM"):
        s = s[2:].strip()
    if s.isdigit():
        n = int(s)
        if 1 <= n <= 9_999_999:
            return n
    return None


def codigo_sequencial_variantes_colisao(ds: str, gm: str, n: int) -> list:
    raw: list = [ds, gm, n]
    try:
        raw.append(float(int(n)))
    except (TypeError, ValueError):
        pass
    out: list = []
    seen: set = set()
    for v in raw:
        if v is None or v == "":
            continue
        key = (type(v).__name__, repr(v))
        if key in seen:
            continue
        seen.add(key)
        out.append(v)
    return out


def max_codigo_numerico_postgres() -> int:
    mx = 0
    qs = Produto.objects.values_list("codigo_interno", "codigo_nfe").iterator(chunk_size=500)
    for ci, cn in qs:
        for val in (ci, cn):
            n = extrair_numero_sequencial(val)
            if n is not None:
                mx = max(mx, n)
    return mx


def max_codigo_numerico_mongo(db, col: str) -> int:
    mx = 0
    if db is None or not col:
        return mx
    try:
        for doc in db[col].find({}, {"Codigo": 1, "CodigoNFe": 1}).batch_size(500):
            for fld in ("Codigo", "CodigoNFe"):
                n = extrair_numero_sequencial(doc.get(fld))
                if n is not None:
                    mx = max(mx, n)
    except Exception:
        logger.warning("cod seq: max mongo", exc_info=True)
    return mx


def _codigo_sequencial_inicio(db=None, col: str | None = None) -> int:
    try:
        n0 = int(os.environ.get("AGRO_NOVO_PRODUTO_COD_MIN", "6000"))
    except ValueError:
        n0 = 6000
    n0 = max(0, min(n0, 9_999_999))
    mx = max(max_codigo_numerico_postgres(), max_codigo_numerico_mongo(db, col or ""))
    if mx > 0:
        return mx + 1
    return n0


def pg_codigo_seq_ocupado(ds: str, gm: str, n: int) -> bool:
    or_q = Q()
    for v in codigo_sequencial_variantes_colisao(ds, gm, n):
        sv = str(v).strip()
        if not sv:
            continue
        or_q |= Q(codigo_interno=sv) | Q(codigo_nfe=sv) | Q(codigo_barras=sv)
    if not or_q:
        return False
    return Produto.objects.filter(or_q).exists()


def mongo_codigo_seq_ocupado(db, col: str, ds: str, gm: str, n: int) -> bool:
    or_dup: list[dict] = []
    for v in codigo_sequencial_variantes_colisao(ds, gm, n):
        for fld in ("Codigo", "CodigoNFe", "CodigoBarras", "EAN_NFe"):
            or_dup.append({fld: v})
    if not or_dup:
        return False
    try:
        return db[col].find_one({"$or": or_dup}) is not None
    except Exception:
        logger.warning("cod seq: mongo colisão", exc_info=True)
        return True


def codigo_seq_ocupado(db, col: str | None, ds: str, gm: str, n: int) -> bool:
    if pg_codigo_seq_ocupado(ds, gm, n):
        return True
    if db is not None and col:
        return mongo_codigo_seq_ocupado(db, col, ds, gm, n)
    return False


def alocar_codigo_sequencial_novo_cadastro(
    db=None,
    col: str | None = None,
) -> tuple[dict | None, str | None, str | None]:
    """
    Próximo par livre: código sistema (numérico) + GM + mesmo número.
    Continua após o maior código já usado; se catálogo vazio, usa ``AGRO_NOVO_PRODUTO_COD_MIN``.
    """
    n = _codigo_sequencial_inicio(db, col)
    max_steps = 65_000
    steps = 0
    while steps < max_steps:
        ds = str(int(n))
        gm = f"GM{ds}"
        if not codigo_seq_ocupado(db, col, ds, gm, int(n)):
            return None, ds, gm
        n += 1
        steps += 1
    return (
        {
            "ok": False,
            "erro": (
                "Não foi possível gerar código sequencial automático: faixa esgotada ou muitas tentativas. "
                "Informe manualmente «Código sistema» e «Código interno (GM)»."
            ),
            "status": 400,
        },
        None,
        None,
    )
