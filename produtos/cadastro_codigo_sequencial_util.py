"""Próximo código sistema + GM para produto novo (Postgres e espelho Mongo)."""
from __future__ import annotations

import logging
import os
import re

from django.db.models import Q

from produtos.models import Produto

logger = logging.getLogger(__name__)

_CODIGO_SISTEMA_4D = re.compile(r"^\d{4}$")
_CODIGO_SISTEMA_MAX = 9999
_CODIGO_SEQ_PISO = 4010
# Sequência automática da loja (~4–5 mil). Códigos 6xxx–9xxx (ERP/legado) não avançam o contador.
_CODIGO_SEQ_TETO_AUTO = 5999


def _piso_sequencia_codigo() -> int:
    try:
        n0 = int(os.environ.get("AGRO_NOVO_PRODUTO_COD_MIN", str(_CODIGO_SEQ_PISO)))
    except ValueError:
        n0 = _CODIGO_SEQ_PISO
    return max(_CODIGO_SEQ_PISO, min(n0, _CODIGO_SISTEMA_MAX))


def _teto_sequencia_auto() -> int:
    try:
        n0 = int(os.environ.get("AGRO_NOVO_PRODUTO_COD_MAX_AUTO", str(_CODIGO_SEQ_TETO_AUTO)))
    except ValueError:
        n0 = _CODIGO_SEQ_TETO_AUTO
    piso = _piso_sequencia_codigo()
    return max(piso, min(n0, _CODIGO_SISTEMA_MAX))


def codigo_sistema_4_digitos_valido(val) -> bool:
    s = str(val or "").strip()
    return bool(_CODIGO_SISTEMA_4D.match(s))


def gm_sugerido_de_codigo_sistema(codigo_sistema: str) -> str:
    s = str(codigo_sistema or "").strip()
    if not codigo_sistema_4_digitos_valido(s):
        return ""
    return f"GM{s}"


def erro_codigo_sistema_4_digitos(val, *, obrigatorio: bool = True) -> str | None:
    s = str(val or "").strip()
    if not s:
        return "Informe o código sistema (4 números)." if obrigatorio else None
    if not codigo_sistema_4_digitos_valido(s):
        return "Código sistema deve ter exatamente 4 números (ex.: 4252)."
    return None


def extrair_numero_sequencial_4d(val) -> int | None:
    """Só considera códigos sistema de exatamente 4 dígitos (ou GM + 4 dígitos)."""
    s = str(val or "").strip()
    if not s or s.lower() == "__novo__":
        return None
    if s.upper().startswith("GM"):
        s = s[2:].strip()
    if codigo_sistema_4_digitos_valido(s):
        return int(s)
    return None


def extrair_numero_sequencial_faixa(val) -> int | None:
    """Código sistema (4 dígitos) na faixa automática da loja (4010–5999). GM/9xxx não entram."""
    n = extrair_numero_sequencial_4d(val)
    if n is None:
        return None
    piso = _piso_sequencia_codigo()
    teto = _teto_sequencia_auto()
    if n < piso or n > teto:
        return None
    return n


def formatar_codigo_sistema(n: int) -> str:
    piso = _piso_sequencia_codigo()
    n = max(piso, min(int(n), _CODIGO_SISTEMA_MAX))
    return f"{n:04d}"


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
    qs = Produto.objects.values_list("codigo_interno", flat=True).iterator(chunk_size=500)
    for ci in qs:
        n = extrair_numero_sequencial_faixa(ci)
        if n is not None:
            mx = max(mx, n)
    return mx


def max_codigo_numerico_mongo(db, col: str) -> int:
    mx = 0
    if db is None or not col:
        return mx
    try:
        for doc in db[col].find({}, {"Codigo": 1}).batch_size(500):
            n = extrair_numero_sequencial_faixa(doc.get("Codigo"))
            if n is not None:
                mx = max(mx, n)
    except Exception:
        logger.warning("cod seq: max mongo", exc_info=True)
    return mx


def _codigo_sequencial_inicio(db=None, col: str | None = None) -> int:
    piso = _piso_sequencia_codigo()
    teto = _teto_sequencia_auto()
    mx = max(max_codigo_numerico_postgres(), max_codigo_numerico_mongo(db, col or ""))
    if mx >= piso:
        return min(mx + 1, teto)
    return piso


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
    Próximo par livre: código sistema (4 dígitos) + ``GM`` + mesmo número.
    Continua após o maior código da faixa automática (4010–5999); 9xxx não puxa a sequência.
    """
    n = _codigo_sequencial_inicio(db, col)
    piso = _piso_sequencia_codigo()
    teto = _teto_sequencia_auto()
    max_steps = teto - piso + 2
    steps = 0
    while steps < max_steps and n <= teto:
        ds = formatar_codigo_sistema(n)
        gm = gm_sugerido_de_codigo_sistema(ds)
        if not codigo_seq_ocupado(db, col, ds, gm, int(n)):
            return None, ds, gm
        n += 1
        steps += 1
    return (
        {
            "ok": False,
            "erro": (
                "Não foi possível gerar código sequencial automático (faixa 4010–5999 esgotada ou ocupada). "
                "Informe manualmente «Código sistema» (4 números) e «Código interno (GM)»."
            ),
            "status": 400,
        },
        None,
        None,
    )
