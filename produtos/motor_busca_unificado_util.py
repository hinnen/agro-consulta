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
    from produtos.cadastro_busca_codigo_util import (
        termo_bate_codigos_produto,
        termo_bate_valor_codigo,
    )

    if termo_bate_codigos_produto(
        termo,
        codigo_interno=doc.get("Codigo") or doc.get("codigo"),
        codigo_nfe=doc.get("CodigoNFe") or doc.get("codigo_nfe") or doc.get("codigo_gm"),
        codigo_barras=doc.get("CodigoBarras") or doc.get("EAN_NFe") or doc.get("codigo_barras"),
        extras=(doc.get("Id"), doc.get("_id"), doc.get("id")),
    ):
        return True
    ix = doc.get(INDEX_CODIGOS_CAMPO) or doc.get("index_codigos")
    if isinstance(ix, list):
        return any(termo_bate_valor_codigo(termo, x) for x in ix if x not in (None, ""))
    return False


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


def _enriquecer_e_injetar_overlay_codigo(
    termo: str,
    docs: list[dict],
    db,
    client,
    *,
    limit: int = 80,
) -> list[dict]:
    """Aplica GM/barras do overlay nos docs e injeta irmãos (família GM) que só existem no overlay."""
    from produtos.cadastro_busca_codigo_util import overlay_pids_por_codigo, parece_codigo_cadastro
    from produtos.mongo_index_codigos import INDEX_CODIGOS_CAMPO
    from produtos.models import ProdutoGestaoOverlayAgro

    termo = str(termo or "").strip()
    if not termo or not parece_codigo_cadastro(termo):
        return list(docs or [])

    out = [dict(d) for d in (docs or [])]
    have = {_pid_doc(d) for d in out if _pid_doc(d)}
    lim = max(1, min(int(limit or 80), 160))

    # 1) Patch CodigoNFe/barras + index a partir do overlay (antes do filtro GM estrito)
    pids_existentes = [pid for pid in have if pid]
    if pids_existentes:
        ov_map: dict[str, ProdutoGestaoOverlayAgro] = {}
        step = 400
        for i in range(0, len(pids_existentes), step):
            chunk = pids_existentes[i : i + step]
            for ov in ProdutoGestaoOverlayAgro.objects.filter(produto_externo_id__in=chunk).only(
                "produto_externo_id", "codigo_nfe", "codigo_barras", "nome", "marca", "cadastro_extras"
            ):
                ov_map[str(ov.produto_externo_id or "").strip()] = ov
        from produtos.cadastro_busca_codigo_util import index_codigos_de_campos

        for d in out:
            pid = _pid_doc(d)
            ov = ov_map.get(pid)
            if not ov:
                continue
            if ov.codigo_nfe.strip():
                d["CodigoNFe"] = ov.codigo_nfe.strip()
            if ov.codigo_barras.strip():
                d["CodigoBarras"] = ov.codigo_barras.strip()
                d["EAN_NFe"] = ov.codigo_barras.strip()
            if ov.nome.strip() and not str(d.get("Nome") or "").strip():
                d["Nome"] = ov.nome.strip()
            if ov.marca.strip() and not str(d.get("Marca") or "").strip():
                d["Marca"] = ov.marca.strip()
            ce = ov.cadastro_extras if isinstance(getattr(ov, "cadastro_extras", None), dict) else None
            d[INDEX_CODIGOS_CAMPO] = index_codigos_de_campos(
                codigo=d.get("Codigo") or d.get("codigo"),
                codigo_nfe=d.get("CodigoNFe") or d.get("codigo_nfe"),
                codigo_barras=d.get("CodigoBarras") or d.get("codigo_barras"),
                cadastro_extras=ce,
            )

    # 2) Injetar pids do overlay ainda ausentes (família GM0024-*)
    try:
        from produtos.views import _mongo_produtos_por_overlay_codigo_busca

        extras = _mongo_produtos_por_overlay_codigo_busca(termo, db, client, have)
    except Exception:
        extras = []
        pids = overlay_pids_por_codigo(termo, limit=lim)
        if pids:
            try:
                from produtos import catalogo_agro as cat_agro

                for pid in pids:
                    if not pid or pid in have:
                        continue
                    p = cat_agro.obter_produto_model(pid)
                    if p is None:
                        continue
                    extras.append(cat_agro.row_para_doc_busca_pdv(cat_agro.produto_agro_para_row(p)))
            except Exception:
                extras = []

    for ex in extras or []:
        pid = _pid_doc(ex)
        if not pid or pid in have:
            continue
        out.append(ex)
        have.add(pid)
        if len(out) >= lim * 2:
            break
    return out


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

    from produtos.cadastro_busca_codigo_util import gm_base_familia, parece_codigo_cadastro

    # Família GM: NÃO pular Mongo só porque o PG trouxe 1 item (ex. GM0024-1 no PG
    # e GM0024-10/15 só no Mongo sem index — loja via agro_pg). Sempre complementar.
    _familia_gm = bool(gm_base_familia(termo))
    mongo_docs: list[dict] = []
    # EMERGÊNCIA: agro_pg + texto comum → NUNCA Mongo (satura loja). Só família GM.
    if usa_pg and not _familia_gm:
        skip_mongo_complemento = True
    # PDV/wizard (skip_mongo_complemento): com agro_pg, 1+ hit no Postgres BASTA.
    _pg_suficiente = skip_mongo_complemento and bool(pg_docs) and (
        len(pg_docs) >= 1 if usa_pg else len(pg_docs) >= min(8, lim)
    )
    if _familia_gm:
        _pg_suficiente = False
    if skip_mongo_complemento and usa_pg and not _familia_gm:
        # Bip 8+ dígitos sem hit no PG: deixa o Mongo complementar (barra extra / legado).
        if pg_docs or not parece_codigo_cadastro(termo):
            _pg_suficiente = True
            mongo_docs = []
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
        # Família GM: sempre mesclar (não descartar PG nem Mongo)
        if merge_pg or _familia_gm:
            prods = cat_agro.mesclar_prods_busca_pdv(mongo_docs, q=termo, limit=lim)
            prods = _merge_pg_prioriza_postgres(pg_docs, prods)
        else:
            prods = mongo_docs
    elif pg_docs:
        prods = pg_docs
    elif mongo_docs:
        if merge_pg or _familia_gm:
            prods = cat_agro.mesclar_prods_busca_pdv(mongo_docs, q=termo, limit=lim)
        else:
            prods = mongo_docs
    else:
        prods = []

    prods = _enriquecer_e_injetar_overlay_codigo(termo, prods, db, client, limit=lim)
    prods = _filtrar_gm_estrito(termo, prods)
    from produtos.busca_filtro_pdv_util import filtrar_documentos_estilo_pdv, score_relevancia_doc

    filtrados = filtrar_documentos_estilo_pdv(prods, termo)
    # Frase tipo "ração estima carne": AND zera (produto = "estimacat…", sem "ração").
    # Fallback: token mais longo ≥4 chars (marca/linha) — loja precisa achar o item.
    if not filtrados and prods and " " in str(termo).strip():
        partes = [p for p in str(termo).split() if len(p.strip()) >= 4]
        if partes:
            best = max(partes, key=len)
            filtrados = filtrar_documentos_estilo_pdv(prods, best)
    prods = filtrados
    prods = sorted(
        prods,
        key=lambda d: (
            -score_relevancia_doc(d, termo),
            str(d.get("Nome") or d.get("nome") or "").lower(),
        ),
    )
    prods = cat_agro._dedupe_prods_busca_preferir_com_nome(prods)
    return prods[:lim]
