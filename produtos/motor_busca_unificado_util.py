"""Motor de busca único SisVale (v2) — Postgres-first, fallback Mongo, ranking unificado."""
from __future__ import annotations

import logging
from typing import Any

from integracoes.texto import normalizar
from produtos.mongo_index_codigos import (
    INDEX_CODIGOS_CAMPO,
    produto_termo_bate_campos_principais,
    somente_alnum,
)

logger = logging.getLogger(__name__)


def _doc_bate_codigo_gm(termo: str, doc: dict) -> bool:
    from produtos.cadastro_busca_codigo_util import termo_bate_codigos_produto

    return termo_bate_codigos_produto(
        termo,
        codigo_interno=doc.get("Codigo") or doc.get("codigo"),
        codigo_nfe=doc.get("CodigoNFe") or doc.get("codigo_nfe"),
        codigo_barras=doc.get("CodigoBarras") or doc.get("EAN_NFe") or doc.get("codigo_barras"),
        extras=(doc.get("Id"), doc.get("_id"), doc.get("id")),
    )


def _extrair_codigo_barras_doc(doc: dict) -> str:
    for k in ("CodigoBarras", "EAN_NFe", "EAN", "codigo_barras", "ean"):
        v = doc.get(k)
        if v not in (None, ""):
            return str(v).strip()
    return ""


def _termo_parece_codigo(termo: str) -> bool:
    from produtos.cadastro_busca_codigo_util import parece_codigo_cadastro

    return parece_codigo_cadastro(termo)


def _score_relevancia_unificado(termo_original: str, doc: dict) -> int:
    termo_original = str(termo_original or "").strip()
    termo_limpo = somente_alnum(termo_original)
    termo_limpo_lower = termo_limpo.lower() if termo_limpo else ""
    palavras = [p for p in termo_original.split() if p]
    termo_norm = normalizar(termo_original)

    nome = str(doc.get("Nome") or doc.get("nome") or "")
    marca = str(doc.get("Marca") or doc.get("marca") or "")
    nome_norm = normalizar(nome)
    marca_norm = normalizar(marca)

    s = 0

    if termo_limpo_lower and _termo_parece_codigo(termo_original):
        if produto_termo_bate_campos_principais(doc, termo_limpo):
            s += 6000

    if termo_limpo_lower:
        exact_ok = False
        pref_ok = False
        idx = doc.get(INDEX_CODIGOS_CAMPO) or doc.get("index_codigos")
        if isinstance(idx, list):
            for x in idx:
                xs = str(x).lower()
                if xs == termo_limpo_lower:
                    exact_ok = True
                    break
                if xs.startswith(termo_limpo_lower):
                    pref_ok = True
        if not exact_ok:
            ext_b = somente_alnum(_extrair_codigo_barras_doc(doc)).lower()
            if ext_b == termo_limpo_lower:
                exact_ok = True
            elif ext_b.startswith(termo_limpo_lower):
                pref_ok = True
        if exact_ok:
            s += 5000
        elif pref_ok:
            s += 1750

    if termo_norm:
        if nome_norm == termo_norm:
            s += 1600
        elif nome_norm.startswith(termo_norm):
            s += 1200
        elif termo_norm in nome_norm:
            s += 700
        if marca_norm.startswith(termo_norm):
            s += 200

    if palavras:
        presentes = sum(1 for p_txt in palavras if normalizar(p_txt) in nome_norm)
        s += presentes * 120
        if presentes == len(palavras):
            s += 300

    s -= len(nome_norm.split())
    return s


def _ordenar_documentos_unificado(docs: list[dict], termo: str) -> list[dict]:
    if not docs:
        return []
    return sorted(
        docs,
        key=lambda d: (
            -_score_relevancia_unificado(termo, d),
            str(d.get("Nome") or d.get("nome") or "").lower(),
        ),
    )


def _filtrar_gm_estrito(termo: str, docs: list[dict]) -> list[dict]:
    from produtos.cadastro_busca_codigo_util import termo_eh_codigo_gm

    if not termo_eh_codigo_gm(termo):
        return docs
    filtrados = [d for d in docs if _doc_bate_codigo_gm(termo, d)]
    return filtrados if filtrados else docs


def _pid_doc(doc: dict) -> str:
    return str(doc.get("Id") or doc.get("_id") or doc.get("id") or "").strip()


def _merge_pg_prioriza_postgres(pg_docs: list[dict], mongo_docs: list[dict]) -> list[dict]:
    from produtos import catalogo_agro as cat_agro

    if not mongo_docs:
        return list(pg_docs)
    if not pg_docs:
        return list(mongo_docs)
    out = list(pg_docs)
    ids = {_pid_doc(p) for p in out if _pid_doc(p)}
    for m in mongo_docs:
        pid = _pid_doc(m)
        if not pid or pid in ids:
            continue
        out.append(m)
        ids.add(pid)
    return cat_agro._dedupe_prods_busca_preferir_com_nome(out)


def buscar_documentos_unificado(
    q: str,
    db,
    client,
    *,
    limit: int = 80,
    include_inactive: bool = False,
    wizard_catalog: bool = False,
    skip_mongo_complemento: bool = False,
) -> list[dict]:
    """
    Busca produtos (documentos estilo Mongo) com pipeline único.

    Postgres quando ``agro_pg`` / merge / somente PG; complemento Mongo só se permitido.
    """
    from produtos.agro_fonte_config import (
        agro_catalogo_usa_postgres,
        agro_pdv_catalogo_somente_postgres,
        agro_pdv_merge_catalogo_postgres,
    )
    from produtos import catalogo_agro as cat_agro

    termo = str(q or "").strip()
    lim = max(1, min(int(limit or 80), 500))
    usa_pg = agro_catalogo_usa_postgres()
    somente_pg = agro_pdv_catalogo_somente_postgres()
    merge_pg = agro_pdv_merge_catalogo_postgres()

    if wizard_catalog:
        if somente_pg or usa_pg:
            rows = cat_agro.listar_todos_rows_ativos()
            return [cat_agro.row_para_doc_busca_pdv(r) for r in rows]
        if db is not None and client is not None:
            from produtos.views import _WIZARD_CATALOG_MONGO_PROJECTION, _wizard_catalog_mongo_limit

            return list(
                db[client.col_p]
                .find({"CadastroInativo": {"$ne": True}}, _WIZARD_CATALOG_MONGO_PROJECTION)
                .sort("Nome", 1)
                .limit(_wizard_catalog_mongo_limit())
            )
        return []

    if not termo:
        return []

    prods: list[dict] = []
    pg_docs: list[dict] = []

    if usa_pg or somente_pg or merge_pg:
        try:
            rows = cat_agro.buscar(termo, limit=min(lim * 2, 160), inativos=include_inactive)
            pg_docs = [cat_agro.row_para_doc_busca_pdv(r) for r in rows]
        except Exception:
            logger.warning("motor_busca_unificado: buscar Postgres falhou", exc_info=True)

    mongo_docs: list[dict] = []
    _pg_suficiente = skip_mongo_complemento and bool(pg_docs) and len(pg_docs) >= min(8, lim)
    if db is not None and client is not None and not somente_pg and not _pg_suficiente:
        try:
            from produtos.views import (
                _CADASTRO_LISTA_MONGO_PROJ,
                motor_busca_consulta_documentos,
            )

            mongo_docs = motor_busca_consulta_documentos(
                termo,
                db,
                client,
                limit=lim,
                include_inactive=include_inactive,
                projection=_CADASTRO_LISTA_MONGO_PROJ if skip_mongo_complemento else None,
                regex_stage2_cap=80 if skip_mongo_complemento else None,
                regex_stage3_cap=80 if skip_mongo_complemento else None,
                regex_stage3b_cap=0 if skip_mongo_complemento else None,
            )
        except Exception:
            logger.warning("motor_busca_unificado: buscar Mongo falhou", exc_info=True)

    if somente_pg or (usa_pg and not mongo_docs):
        prods = pg_docs
    elif pg_docs and mongo_docs:
        if merge_pg:
            prods = cat_agro.mesclar_prods_busca_pdv(mongo_docs, q=termo, limit=lim)
            prods = _merge_pg_prioriza_postgres(pg_docs, prods)
        else:
            prods = mongo_docs
    elif pg_docs:
        prods = pg_docs
    elif mongo_docs:
        if merge_pg:
            prods = cat_agro.mesclar_prods_busca_pdv(mongo_docs, q=termo, limit=lim)
        else:
            prods = mongo_docs
    else:
        prods = []

    prods = _filtrar_gm_estrito(termo, prods)
    from produtos.busca_filtro_pdv_util import filtrar_documentos_estilo_pdv, score_relevancia_doc

    prods = filtrar_documentos_estilo_pdv(prods, termo)
    prods = sorted(
        prods,
        key=lambda d: (
            -score_relevancia_doc(d, termo),
            str(d.get("Nome") or d.get("nome") or "").lower(),
        ),
    )
    prods = cat_agro._dedupe_prods_busca_preferir_com_nome(prods)
    return prods[:lim]
