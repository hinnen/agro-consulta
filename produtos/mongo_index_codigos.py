"""
Denormalização: ``index_codigos`` (array de strings) para busca O(1) com índice multikey.

Evita ``$elemMatch`` em listas de similares no hot path. Preenchimento via
``python manage.py agro_rebuild_index_codigos`` (e opcionalmente cron após sync ERP).
"""

from __future__ import annotations

import logging
import re
from typing import Any

from bson import ObjectId

logger = logging.getLogger(__name__)

INDEX_CODIGOS_CAMPO = "index_codigos"
AGRO_INDEX_AT_CAMPO = "AgroIndexCodigosAt"

_MAX_VALORES = 450
_MAX_LEN_STR = 96

SIMILARES_FIELD_KEYS = (
    "ProdutosSimilares",
    "IdsProdutosSimilares",
    "SimilarProdutoIds",
    "Similares",
    "similares_erp",
    "SimilaresErp",
    "SimilaresERP",
    "ListaSimilaresErp",
    "IdsSimilares",
    "ListaSimilares",
    "ListaProdutosSimilares",
    "ProdutoSimilares",
    "ItensSimilares",
    "VinculosSimilares",
    "ListaVinculosSimilares",
)

CAMPOS_CODIGO_RAIZ_MONGO: tuple[str, ...] = (
    "Codigo",
    "CodigoNFe",
    "CodigoBarras",
    "EAN_NFe",
    "EAN",
    "CodigoDeBarras",
    "CodigoBarrasProduto",
    "GTIN",
    "Sku",
    "SKU",
    "CodigoSku",
    "Referencia",
    "CodigoReferencia",
    "ReferenciaFornecedor",
    "CodigoInterno",
    "CodigoAuxiliar",
    "CodigoFabricante",
    "CodigoFornecedor",
    "CodFornecedor",
    "RefFornecedor",
    "Barras",
    "CodigoAntigo",
    "ProdutoCodigo",
    "CodigoCliente",
    "NumeroSerie",
)

CAMPOS_NCM_FISCAL: tuple[str, ...] = (
    "NCM",
    "NumeroNCM",
    "CodigoNCM",
    "CodigoNcm",
    "CEST",
    "Cest",
    "CodigoCEST",
)

EMB_SIM_NEST_CODIGO = (
    "CodigoBarras",
    "EAN",
    "EAN_NFe",
    "CodigoNFe",
    "Codigo",
    "CodigoDeBarras",
    "GTIN",
    "EAN13",
    "CodigoBarrasEAN",
    "CodigoBarrasProduto",
    "Sku",
    "SKU",
    "CodigoSku",
    "Referencia",
    "CodigoReferencia",
    "ReferenciaFornecedor",
    "CodigoInterno",
    "CodigoAuxiliar",
    "CodigoFabricante",
    "CodigoFornecedor",
    "CodFornecedor",
    "RefFornecedor",
    "Barras",
    "CodigoAntigo",
    "ProdutoCodigo",
    "CodigoCliente",
    "NumeroSerie",
)

EMB_SIM_NEST_ID = (
    "Id",
    "ProdutoID",
    "ProdutoId",
    "IdProduto",
    "Produto_Id",
    "ID",
    "id",
)


def somente_alnum(txt: Any) -> str:
    return re.sub(r"[^a-zA-Z0-9]", "", str(txt or ""))


_CODEISH_KEY_PARTS = (
    "cod",
    "ean",
    "barr",
    "sku",
    "gtin",
    "ref",
    "intern",
    "fabri",
    "fornec",
    "serie",
    "aux",
    "ncm",
    "cest",
    "barras",
)


def _chave_parece_codigo(nome: str) -> bool:
    n = str(nome or "").lower()
    return any(p in n for p in _CODEISH_KEY_PARTS)


def _valor_parece_codigo_longo(v: Any) -> bool:
    """Evita ruído (ex.: quantidade 12) mas pega EAN/GTIN e códigos mistos."""
    s = str(v).strip()
    if not s or len(s) > _MAX_LEN_STR:
        return False
    if s.isdigit():
        return len(s) >= 8
    al = somente_alnum(s)
    if len(al) < 6:
        return False
    tem_d = any(c.isdigit() for c in al)
    tem_a = any(c.isalpha() for c in al)
    return tem_d and (tem_a or len(al) >= 10)


def _extrair_codigos_estrutura_similar(est: Any, out: set[str], depth: int) -> None:
    """Varre dict/list de similares com nomes de campo fora do whitelist (ex.: ERP próprio)."""
    if depth > 6:
        return
    if isinstance(est, dict):
        for k, v in est.items():
            ks = str(k)
            if ks.startswith("$"):
                continue
            if isinstance(v, (dict, list)):
                _extrair_codigos_estrutura_similar(v, out, depth + 1)
            elif _chave_parece_codigo(ks):
                _push_val(out, v)
            elif _valor_parece_codigo_longo(v):
                _push_val(out, v)
    elif isinstance(est, list):
        for it in est:
            _extrair_codigos_estrutura_similar(it, out, depth + 1)


def _push_val(out: set[str], v: Any) -> None:
    if v is None:
        return
    if isinstance(v, ObjectId):
        s = str(v).strip()
        if s:
            out.add(s.lower())
        return
    if isinstance(v, bool):
        return
    if isinstance(v, (int, float)) and not isinstance(v, bool):
        if isinstance(v, float) and v != v:
            return
        try:
            if float(v) == int(v):
                iv = int(v)
                s = str(iv)
                if s:
                    out.add(s.lower())
                return
        except (TypeError, ValueError):
            pass
    s = str(v).strip()
    if not s or len(s) > _MAX_LEN_STR:
        return
    al = somente_alnum(s)
    if al:
        out.add(al.lower())
    if s != al and len(s) <= _MAX_LEN_STR and not any(c.isspace() for c in s):
        out.add(s.lower())


def extrair_index_codigos_de_documento_mongo(doc: dict | None) -> list[str]:
    """Coleta códigos do documento ERP + linhas embutidas em similares (sem variáveis SQLite)."""
    out: set[str] = set()
    if not doc:
        return []

    for fld in CAMPOS_CODIGO_RAIZ_MONGO:
        _push_val(out, doc.get(fld))
    for fld in CAMPOS_NCM_FISCAL:
        _push_val(out, doc.get(fld))

    pid = doc.get("Id") or doc.get("_id")
    _push_val(out, pid)

    for key in SIMILARES_FIELD_KEYS:
        val = doc.get(key)
        if not isinstance(val, list):
            continue
        for item in val:
            if isinstance(item, dict):
                for nk in EMB_SIM_NEST_CODIGO + EMB_SIM_NEST_ID:
                    if nk in item:
                        _push_val(out, item.get(nk))
                _extrair_codigos_estrutura_similar(item, out, 0)
            else:
                _push_val(out, item)

    ordered = sorted(out)
    return ordered[:_MAX_VALORES]


def projection_documento_para_rebuild_index() -> dict:
    """Campos mínimos para ``extrair_index_codigos_de_documento_mongo`` (rebuild em lote)."""
    chaves = set(CAMPOS_CODIGO_RAIZ_MONGO) | set(CAMPOS_NCM_FISCAL) | set(SIMILARES_FIELD_KEYS)
    chaves.add("Id")
    return {k: 1 for k in chaves}


def coletar_extras_agro_para_busca(produto_externo_id: str) -> list[str]:
    """
    Códigos cadastrados no Agro que entram no mesmo ``index_codigos`` do ERP:
    overlay (barras / NFe) + linhas de ProdutoMarcaVariacaoAgro — sem duplicar regra em outro lugar.
    """
    from produtos.models import ProdutoGestaoOverlayAgro, ProdutoMarcaVariacaoAgro

    pid = str(produto_externo_id or "").strip()
    if not pid:
        return []
    out: list[str] = []
    ov = (
        ProdutoGestaoOverlayAgro.objects.filter(produto_externo_id=pid[:64])
        .only("codigo_barras", "codigo_nfe")
        .first()
    )
    if ov:
        for z in (ov.codigo_barras, ov.codigo_nfe):
            s = (z or "").strip()
            if s:
                out.append(s)
    for row in ProdutoMarcaVariacaoAgro.objects.filter(produto_externo_id=pid[:64]).only(
        "codigo_barras",
        "codigo_fornecedor",
        "codigo_interno",
    ):
        for z in (row.codigo_barras, row.codigo_fornecedor, row.codigo_interno):
            s = (z or "").strip()
            if s:
                out.append(s)
    return out


def mapa_extras_agro_por_produto_externo_id() -> dict[str, list[str]]:
    """Pré-carga para rebuild em lote (overlay + variações)."""
    from collections import defaultdict

    from produtos.models import ProdutoGestaoOverlayAgro, ProdutoMarcaVariacaoAgro

    out: dict[str, list[str]] = defaultdict(list)
    for ov in ProdutoGestaoOverlayAgro.objects.all().only(
        "produto_externo_id",
        "codigo_barras",
        "codigo_nfe",
    ).iterator(chunk_size=2000):
        pid = str(ov.produto_externo_id or "").strip()
        if not pid:
            continue
        for z in (ov.codigo_barras, ov.codigo_nfe):
            s = (z or "").strip()
            if s:
                out[pid].append(s)
    for row in ProdutoMarcaVariacaoAgro.objects.all().only(
        "produto_externo_id",
        "codigo_barras",
        "codigo_fornecedor",
        "codigo_interno",
    ).iterator(chunk_size=2000):
        pid = str(row.produto_externo_id or "").strip()
        if not pid:
            continue
        for z in (row.codigo_barras, row.codigo_fornecedor, row.codigo_interno):
            s = (z or "").strip()
            if s:
                out[pid].append(s)
    return dict(out)


def aplicar_index_codigos_no_mongo(
    db,
    col_p: str,
    doc: dict,
    *,
    produto_externo_id: str | None = None,
) -> None:
    """Persiste ``index_codigos`` + timestamp a partir do documento ERP atual + extras Agro (SQLite)."""
    from django.utils import timezone as dj_tz

    if not doc or "_id" not in doc:
        return
    pid = produto_externo_id or str(doc.get("Id") or doc.get("_id") or "")
    extras = coletar_extras_agro_para_busca(pid) if pid else []
    idx = montar_index_codigos_final(doc, extras_sqlite=extras)
    now = dj_tz.now()
    db[col_p].update_one(
        {"_id": doc["_id"]},
        {"$set": {INDEX_CODIGOS_CAMPO: idx, AGRO_INDEX_AT_CAMPO: now}},
    )


def montar_index_codigos_final(
    doc: dict,
    extras_sqlite: list[str] | None = None,
) -> list[str]:
    """União: espelho ERP (incl. similares) + códigos Agro (overlay e variações SQLite)."""
    base = extrair_index_codigos_de_documento_mongo(doc)
    out: set[str] = set(base)
    for x in extras_sqlite or []:
        _push_val(out, x)
    ordered = sorted(out)
    return ordered[:_MAX_VALORES]


def merge_busca_codigo_prioridade_principal(
    exatos: list[dict],
    mestres_sim: list[dict],
    termo_limpo: str,
    limit: int,
) -> list[dict]:
    seen: set[str] = set()
    merged: list[dict] = []
    for lst in (exatos, mestres_sim):
        for item in lst or []:
            pid = str(item.get("Id") or item.get("_id"))
            if pid not in seen:
                seen.add(pid)
                merged.append(item)
    tl = somente_alnum(termo_limpo).lower()
    merged.sort(
        key=lambda p: (
            0 if produto_termo_bate_campos_principais(p, tl) else 1,
            str(p.get("Nome") or "").lower(),
        )
    )
    return merged[: max(1, int(limit))]


def produto_termo_bate_campos_principais(doc: dict, termo_limpo: str) -> bool:
    tl = somente_alnum(termo_limpo).lower()
    if not tl:
        return False
    idx = doc.get(INDEX_CODIGOS_CAMPO)
    if isinstance(idx, list):
        for x in idx:
            if str(x).lower() == tl or somente_alnum(str(x)).lower() == tl:
                return True
    for fld in CAMPOS_CODIGO_RAIZ_MONGO:
        val = doc.get(fld)
        if val is None or val == "":
            continue
        if somente_alnum(str(val)).lower() == tl:
            return True
        if isinstance(val, (int, float)) and tl.isdigit():
            try:
                if str(int(val)) == tl:
                    return True
            except (TypeError, ValueError):
                pass
    cb = (
        doc.get("CodigoBarras")
        or doc.get("EAN_NFe")
        or doc.get("EAN")
        or doc.get("CodigoDeBarras")
        or doc.get("CodigoBarrasProduto")
        or doc.get("GTIN")
        or ""
    )
    if somente_alnum(str(cb)).lower() == tl:
        return True
    return False


def mongo_query_so_index_codigo(termo_limpo: str) -> dict:
    """Uma única chave para find/$or mínimo."""
    tl = somente_alnum(str(termo_limpo or "")).lower()
    return {INDEX_CODIGOS_CAMPO: tl}


def encontrar_produto_casar_entrada_nfe(
    db,
    col_p: str,
    *,
    ean: str,
    c_prod: str,
) -> tuple[dict | None, str | None]:
    base = {"CadastroInativo": {"$ne": True}}
    col = db[col_p]

    def buscar_um(t: str) -> dict | None:
        tl = somente_alnum(t).lower()
        if not tl:
            return None
        try:
            docs = list(
                col.find(
                    {**base, INDEX_CODIGOS_CAMPO: tl},
                    {
                        "Id": 1,
                        "_id": 1,
                        "Nome": 1,
                        **{k: 1 for k in CAMPOS_CODIGO_RAIZ_MONGO},
                        INDEX_CODIGOS_CAMPO: 1,
                    },
                ).limit(24)
            )
        except Exception as exc:
            logger.warning("encontrar_produto_casar_entrada_nfe: %s", exc, exc_info=True)
            return None
        if not docs:
            return None
        if len(docs) == 1:
            return docs[0]
        merged = merge_busca_codigo_prioridade_principal(docs, [], tl, 1)
        return merged[0] if merged else docs[0]

    try:
        e = (ean or "").strip()
        if e and len(e) >= 8:
            doc = buscar_um(e)
            if doc:
                return doc, "ean"
        c = (c_prod or "").strip()
        if c:
            doc = buscar_um(c)
            if doc:
                return doc, "codigo"
        return None, None
    except Exception as exc:
        logger.warning("encontrar_produto_casar_entrada_nfe: %s", exc, exc_info=True)
        return None, None
