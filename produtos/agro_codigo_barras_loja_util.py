"""
Código de barras interno da loja (embalagem no balcão): prefixo 230.

Formato novo (EAN-13 válido): 230 + 9 dígitos de sequência + dígito verificador.
Ex.: seq 1572 → 2300000015728 (DV correto).

Formato legado: 230 + 10 dígitos sequenciais sem DV EAN.
Na etiqueta o DV era corrigido, mas a busca não trata mais legado e EAN corrigido como
equivalentes: ambos podem pertencer a produtos distintos. Novas atribuições usam somente
EAN-13 válido e reservam todo o grupo que produziria o mesmo valor físico bipado.
"""

from __future__ import annotations

import logging
import re
from typing import TYPE_CHECKING

from django.db import connection
from django.http import JsonResponse

if TYPE_CHECKING:
    from pymongo.database import Database

logger = logging.getLogger(__name__)

CB_LOJA_PREFIX = "230"
CB_LOJA_SEQ_LEN = 10  # corpo legado (10) / regex 13 dígitos totais
CB_LOJA_SEQ_LEN_NOVO = 9  # payload EAN-13 novo
CB_LOJA_SEQ_MAX = 999_999_999
_CB_LOJA_REGEX = re.compile(rf"^{CB_LOJA_PREFIX}\d{{{CB_LOJA_SEQ_LEN}}}$")
_ADVISORY_LOCK_CB_LOJA = 230_4045_147


def _cap_seq_loja(n: int) -> int:
    return max(1, min(int(n), CB_LOJA_SEQ_MAX))


def ean13_digito_verificador(d12: str) -> int | None:
    d = re.sub(r"\D", "", str(d12 or ""))
    if len(d) != 12 or not d.isdigit():
        return None
    soma = 0
    for i, ch in enumerate(d):
        n = int(ch)
        soma += n if i % 2 == 0 else n * 3
    return (10 - (soma % 10)) % 10


def ean13_checksum_ok(d13: str) -> bool:
    d = re.sub(r"\D", "", str(d13 or ""))
    if len(d) != 13 or not d.isdigit():
        return False
    dv = ean13_digito_verificador(d[:12])
    return dv is not None and str(dv) == d[12]


def formatar_codigo_barras_loja(seq: int) -> str:
    """Próximo código novo: EAN-13 válido (230 + 9 dígitos + DV)."""
    n = max(1, int(seq))
    if n > 999_999_999:
        n = 999_999_999
    d12 = f"{CB_LOJA_PREFIX}{n:0{CB_LOJA_SEQ_LEN_NOVO}d}"
    dv = ean13_digito_verificador(d12)
    assert dv is not None
    return f"{d12}{dv}"


def parsear_seq_codigo_barras_loja(cb: str) -> int | None:
    """
    Sequência lógica do código 230….
    Novo (DV EAN ok) → 9 dígitos centrais; legado → 10 dígitos após 230.
    """
    d = re.sub(r"\D", "", str(cb or ""))
    if not _CB_LOJA_REGEX.match(d):
        return None
    try:
        if ean13_checksum_ok(d):
            return _cap_seq_loja(int(d[len(CB_LOJA_PREFIX) : 12]))
        return _cap_seq_loja(int(d[len(CB_LOJA_PREFIX) :]))
    except ValueError:
        return None


def _seq_legado_10d_parece_cb_loja(d: str) -> bool:
    """
    Evita NCM/outros 13 dígitos que começam com 230 (ex. 23099020…000) inflarem max_seq.
    Legado interno costuma ter zeros à esquerda no corpo (2300000001480).
    """
    if not _CB_LOJA_REGEX.match(d):
        return False
    if ean13_checksum_ok(d):
        return True
    corpo = d[len(CB_LOJA_PREFIX) :]
    if not corpo.isdigit():
        return False
    # NCM comum no cadastro (23099020…) preenchido em campo errado → 23099020xxxxxx
    if corpo.startswith("99020"):
        return False
    if int(corpo[:3]) >= 990:
        return False
    return True


def _seqs_para_max_alocacao(cb: str) -> list[int]:
    """Candidatos de seq para achar o próximo livre (legado 10 + novo 9)."""
    d = re.sub(r"\D", "", str(cb or ""))
    if not _CB_LOJA_REGEX.match(d):
        return []
    out: list[int] = []
    try:
        if ean13_checksum_ok(d):
            out.append(_cap_seq_loja(int(d[len(CB_LOJA_PREFIX) : 12])))
        elif _seq_legado_10d_parece_cb_loja(d):
            out.append(_cap_seq_loja(int(d[len(CB_LOJA_PREFIX) :])))
    except ValueError:
        return []
    return out


def eh_codigo_barras_loja(cb: str) -> bool:
    """True se for faixa interna 230… (13 dígitos da loja — legado ou EAN novo)."""
    d = re.sub(r"\D", "", str(cb or ""))
    return bool(_CB_LOJA_REGEX.match(d))


def ean13_para_bip_codigo_barras_loja(cb: str) -> str | None:
    """
    EAN-13 que o leitor lê na etiqueta.
    Legado: recalcula só o DV (corpo = 12 primeiros do cadastro).
    """
    d = re.sub(r"\D", "", str(cb or ""))
    if not _CB_LOJA_REGEX.match(d):
        return None
    if ean13_checksum_ok(d):
        return d
    dv = ean13_digito_verificador(d[:12])
    if dv is None:
        return None
    canonical = f"{d[:12]}{dv}"
    return canonical


def codigos_grupo_bip_canonico(cb: str) -> list[str]:
    """Códigos armazenáveis que produzem o mesmo EAN efetivamente bipado."""
    d = re.sub(r"\D", "", str(cb or ""))
    if not _CB_LOJA_REGEX.match(d):
        return [d] if d else []
    canonical = ean13_para_bip_codigo_barras_loja(d)
    if not canonical:
        return [d]
    body = canonical[:12]
    return [f"{body}{tail}" for tail in "0123456789"]


def variantes_busca_codigo_barras_loja(cb: str) -> list[str]:
    """Busca literal segura; equivalência legado↔canônico causava colisões entre produtos."""
    d = re.sub(r"\D", "", str(cb or ""))
    if not _CB_LOJA_REGEX.match(d):
        return []
    return [d]


def bloquear_alocacao_codigo_barras_loja() -> None:
    """Serializa validação + gravação de códigos 230 dentro da transação atual."""
    if connection.vendor != "postgresql":
        return
    with connection.cursor() as cursor:
        cursor.execute("SELECT pg_advisory_xact_lock(%s)", [_ADVISORY_LOCK_CB_LOJA])


def _cb_loja_pertence_ao_produto_postgres(cb: str, produto_externo_id: str) -> bool:
    from django.db.models import Q

    from .models import Produto, ProdutoGestaoOverlayAgro, ProdutoMarcaVariacaoAgro

    pid = str(produto_externo_id or "").strip()[:64]
    if not pid:
        return False
    if Produto.objects.filter(
        Q(codigo_barras=cb) | Q(codigo_interno=cb) | Q(codigo_nfe=cb),
        produto_externo_id=pid,
    ).exists():
        return True
    if ProdutoGestaoOverlayAgro.objects.filter(
        produto_externo_id=pid,
        codigo_barras=cb,
    ).exists():
        return True
    return ProdutoMarcaVariacaoAgro.objects.filter(
        produto_externo_id=pid,
        codigo_barras=cb,
    ).exists()


def _cb_loja_ocupado_postgres_por_outro(cb: str, produto_externo_id: str = "") -> bool:
    from django.db.models import Q

    from .models import Produto, ProdutoGestaoOverlayAgro, ProdutoMarcaVariacaoAgro
    from .mongo_index_codigos import codigos_barras_opcionais_de_cadastro_extras

    pid = str(produto_externo_id or "").strip()[:64]
    produtos = Produto.objects.filter(
        Q(codigo_barras=cb) | Q(codigo_interno=cb) | Q(codigo_nfe=cb)
    )
    overlays = ProdutoGestaoOverlayAgro.objects.filter(codigo_barras=cb)
    variacoes = ProdutoMarcaVariacaoAgro.objects.filter(codigo_barras=cb)
    if pid:
        produtos = produtos.exclude(produto_externo_id=pid)
        overlays = overlays.exclude(produto_externo_id=pid)
        variacoes = variacoes.exclude(produto_externo_id=pid)
    if produtos.exists() or overlays.exists() or variacoes.exists():
        return True
    opcionais = ProdutoGestaoOverlayAgro.objects.exclude(cadastro_extras={})
    if pid:
        opcionais = opcionais.exclude(produto_externo_id=pid)
    for ov in opcionais.only("cadastro_extras"):
        if cb in codigos_barras_opcionais_de_cadastro_extras(ov.cadastro_extras):
            return True
    return False


def validar_codigo_barras_loja_para_salvar(
    cb: str,
    *,
    produto_externo_id: str = "",
    db: Database | None = None,
    col: str | None = None,
) -> str | None:
    """Valida um 230 novo e impede colisão literal/canônica com outro produto."""
    d = re.sub(r"\D", "", str(cb or ""))
    if not _CB_LOJA_REGEX.match(d):
        return None
    pid = str(produto_externo_id or "").strip()[:64]
    legado_do_mesmo_produto = bool(
        pid and _cb_loja_pertence_ao_produto_postgres(d, pid)
    )
    if not ean13_checksum_ok(d) and not legado_do_mesmo_produto:
        return (
            "Código 230 inválido: gere um novo EAN-13 pelo botão 230 "
            "antes de salvar."
        )
    for alt in codigos_grupo_bip_canonico(d):
        if _cb_loja_ocupado_postgres_por_outro(alt, pid):
            return (
                "Este código 230 já está ocupado por outro produto, inclusive pelo "
                "valor físico bipado. Clique em 230 novamente e salve de novo."
            )
        if db is not None and col and _cb_loja_ocupado_mongo_por_outro(
            db, col, alt, pid
        ):
            return (
                "Este código 230 já está ocupado no catálogo ERP. "
                "Clique em 230 novamente e salve de novo."
            )
    return None


def _cb_loja_ocupado_overlays(cb: str) -> bool:
    from .models import ProdutoGestaoOverlayAgro, ProdutoMarcaVariacaoAgro

    if ProdutoGestaoOverlayAgro.objects.filter(codigo_barras=cb).exists():
        return True
    if ProdutoMarcaVariacaoAgro.objects.filter(codigo_barras=cb).exists():
        return True
    from .mongo_index_codigos import codigos_barras_opcionais_de_cadastro_extras

    for ov in ProdutoGestaoOverlayAgro.objects.exclude(cadastro_extras={}).only(
        "cadastro_extras"
    ):
        if cb in codigos_barras_opcionais_de_cadastro_extras(ov.cadastro_extras):
            return True
    return False


def _cb_loja_ocupado_postgres(cb: str) -> bool:
    from django.db.models import Q

    from .models import Produto

    if Produto.objects.filter(
        Q(codigo_barras=cb) | Q(codigo_interno=cb) | Q(codigo_nfe=cb)
    ).exists():
        return True
    return _cb_loja_ocupado_overlays(cb)


def _cb_loja_ocupado_mongo(db: Database, col: str, cb: str) -> bool:
    or_dup = [{fld: cb} for fld in ("CodigoBarras", "CodigoBarrasProduto", "Codigo", "CodigoNFe", "EAN_NFe")]
    or_dup.append({"index_codigos": cb})
    try:
        return bool(db[col].find_one({"$or": or_dup}, {"_id": 1}))
    except Exception:
        logger.warning("cb loja: colisão Mongo", exc_info=True)
        return False


def _cb_loja_ocupado_mongo_por_outro(
    db: Database,
    col: str,
    cb: str,
    produto_externo_id: str = "",
) -> bool:
    or_dup = [
        {fld: cb}
        for fld in (
            "CodigoBarras",
            "CodigoBarrasProduto",
            "Codigo",
            "CodigoNFe",
            "EAN_NFe",
            "index_codigos",
        )
    ]
    pid = str(produto_externo_id or "").strip()
    try:
        for doc in db[col].find({"$or": or_dup}, {"_id": 1, "Id": 1}).limit(8):
            ids = {str(doc.get("Id") or "").strip(), str(doc.get("_id") or "").strip()}
            if not pid or pid not in ids:
                return True
        return False
    except Exception:
        logger.warning("cb loja: colisão Mongo por outro produto", exc_info=True)
        return True


def _cb_loja_ocupado(db: Database, col: str, cb: str) -> bool:
    if _cb_loja_ocupado_mongo(db, col, cb):
        return True
    return _cb_loja_ocupado_overlays(cb)


def _cb_loja_ocupado_unificado(db: Database | None, col: str | None, cb: str) -> bool:
    exact_pg = _cb_loja_ocupado_postgres(cb)
    diagnostic_variants = codigos_grupo_bip_canonico(cb)
    occupied_pg_variants = [
        alt for alt in diagnostic_variants if alt != cb and _cb_loja_ocupado_postgres(alt)
    ]
    if exact_pg or occupied_pg_variants:
        return True
    if db is not None and col:
        return any(_cb_loja_ocupado_mongo(db, col, alt) for alt in diagnostic_variants)
    return False


def _max_seq_cb_loja_unificado(db: Database | None, col: str | None) -> int:
    max_seq = _max_seq_cb_loja_postgres()
    if db is not None and col:
        try:
            max_seq = max(max_seq, _max_seq_cb_loja_catalogo(db, col))
        except Exception:
            logger.warning("cb loja: max seq Mongo", exc_info=True)
    return max_seq


def _max_seq_cb_loja_catalogo(db: Database, col: str) -> int:
    from .models import ProdutoGestaoOverlayAgro, ProdutoMarcaVariacaoAgro

    max_seq = 0

    def bump(cb_raw: object) -> None:
        nonlocal max_seq
        for s in _seqs_para_max_alocacao(str(cb_raw or "")):
            if s > max_seq:
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


def _max_seq_cb_loja_postgres() -> int:
    from .models import Produto, ProdutoGestaoOverlayAgro, ProdutoMarcaVariacaoAgro

    max_seq = 0

    def bump(cb_raw: object) -> None:
        nonlocal max_seq
        for s in _seqs_para_max_alocacao(str(cb_raw or "")):
            if s > max_seq:
                max_seq = s

    for cb in Produto.objects.exclude(codigo_barras="").values_list("codigo_barras", flat=True):
        bump(cb)
    for cb in Produto.objects.exclude(codigo_interno="").values_list("codigo_interno", flat=True):
        bump(cb)
    for cb in Produto.objects.exclude(codigo_nfe="").values_list("codigo_nfe", flat=True):
        bump(cb)
    for cb in ProdutoGestaoOverlayAgro.objects.exclude(codigo_barras="").values_list(
        "codigo_barras", flat=True
    ):
        bump(cb)
    for cb in ProdutoMarcaVariacaoAgro.objects.exclude(codigo_barras="").values_list(
        "codigo_barras", flat=True
    ):
        bump(cb)
    return max_seq


def _erro_cb_loja_esgotado() -> tuple[JsonResponse, None]:
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


def alocar_proximo_codigo_barras_loja(
    db: Database | None = None,
    col: str | None = None,
) -> tuple[JsonResponse | None, str | None]:
    """
    Próximo EAN-13 230… livre.
    Postgres + overlays sempre; Mongo complementa max/colisião quando disponível.
    """
    n = _cap_seq_loja(_max_seq_cb_loja_unificado(db, col) + 1)
    max_steps = 100_000
    steps = 0
    ultimo_cb = ""
    while steps < max_steps:
        cb = formatar_codigo_barras_loja(n)
        if cb == ultimo_cb and n >= CB_LOJA_SEQ_MAX:
            break
        ultimo_cb = cb
        occupied = _cb_loja_ocupado_unificado(db, col, cb)
        if not occupied:
            return None, cb
        if n >= CB_LOJA_SEQ_MAX:
            break
        n += 1
        steps += 1
    return _erro_cb_loja_esgotado()


def alocar_proximo_codigo_barras_loja_postgres() -> tuple[JsonResponse | None, str | None]:
    """Atalho — só Postgres/overlays (sem Mongo)."""
    return alocar_proximo_codigo_barras_loja(None, None)


def mongo_alocar_proximo_codigo_barras_loja(
    db: Database, col: str
) -> tuple[JsonResponse | None, str | None]:
    """Atalho legado — delega ao alocador unificado."""
    return alocar_proximo_codigo_barras_loja(db, col)
