"""Filtro e relevância de busca alinhados ao PDV (``filtrarProdutosBuscaInteligente`` / ``relevanciaTextoBuscaPdv``)."""
from __future__ import annotations

import re

from integracoes.texto import normalizar
from produtos.mongo_index_codigos import INDEX_CODIGOS_CAMPO, somente_alnum

_RE_DIGITOS = re.compile(r"\D")


def _norm_termo(termo: str) -> str:
    return normalizar(str(termo or "").strip())


def blob_busca_de_doc(doc: dict) -> str:
    bt = doc.get("BuscaTexto") or doc.get("busca_texto")
    if bt:
        return _norm_termo(str(bt))
    partes: list[str] = []
    for k in (
        "Nome",
        "nome",
        "Marca",
        "marca",
        "CodigoNFe",
        "codigo_nfe",
        "CodigoBarras",
        "codigo_barras",
        "Codigo",
        "codigo",
        "NomeCategoria",
        "categoria",
        "NomeFornecedor",
        "fornecedor",
    ):
        v = doc.get(k)
        if v not in (None, ""):
            partes.append(str(v))
    ix = doc.get(INDEX_CODIGOS_CAMPO) or doc.get("index_codigos")
    if isinstance(ix, list):
        partes.extend(str(x) for x in ix[:120] if x not in (None, ""))
    return _norm_termo(" ".join(partes))


def blob_busca_de_row_api(row: dict) -> str:
    bt = row.get("busca_texto")
    if bt:
        return _norm_termo(str(bt))
    partes: list[str] = []
    for k in ("nome", "marca", "codigo_nfe", "codigo_barras", "codigo", "categoria", "fornecedor"):
        v = row.get(k)
        if v not in (None, ""):
            partes.append(str(v))
    ix = row.get("index_codigos")
    if isinstance(ix, list):
        partes.extend(str(x) for x in ix[:120] if x not in (None, ""))
    return _norm_termo(" ".join(partes))


def _campos_codigo_doc(doc: dict) -> tuple:
    return (
        doc.get("CodigoNFe") or doc.get("codigo_nfe"),
        doc.get("CodigoBarras") or doc.get("EAN_NFe") or doc.get("codigo_barras"),
        doc.get("Codigo") or doc.get("codigo"),
    )


def _palavra_casa_no_doc(palavra: str, doc: dict, blob: str, nome: str) -> bool:
    from produtos.cadastro_busca_codigo_util import (
        termo_bate_codigos_produto,
        termo_bate_valor_codigo,
    )

    pn = _norm_termo(palavra)
    if not pn:
        return True

    if pn.startswith("gm") and len(pn) >= 3:
        cnfe, cbarr, cint = _campos_codigo_doc(doc)
        if termo_bate_codigos_produto(
            pn,
            codigo_interno=cint,
            codigo_nfe=cnfe,
            codigo_barras=cbarr,
            extras=(doc.get("Id"), doc.get("_id"), doc.get("id")),
        ):
            return True
        ix = doc.get(INDEX_CODIGOS_CAMPO) or doc.get("index_codigos")
        if isinstance(ix, list):
            for x in ix:
                if termo_bate_valor_codigo(pn, x):
                    return True

    digits = _RE_DIGITOS.sub("", pn)
    if digits.isdigit() and len(digits) >= 4:
        cnfe, cbarr, cint = _campos_codigo_doc(doc)
        if termo_bate_codigos_produto(
            pn,
            codigo_interno=cint,
            codigo_nfe=cnfe,
            codigo_barras=cbarr,
            extras=(),
        ):
            return True

    if pn in nome or pn in blob:
        return True

    tokens = blob.split()
    for tk in tokens:
        if tk == pn or tk.startswith(pn) or (len(pn) >= 3 and pn in tk):
            return True
    return False


def filtrar_documentos_estilo_pdv(docs: list[dict], termo: str) -> list[dict]:
    """Todas as palavras do termo devem casar (AND) — igual filtro local do PDV."""
    from produtos.cadastro_busca_codigo_util import parece_codigo_cadastro

    termo = str(termo or "").strip()
    if not termo or not docs:
        return list(docs or [])

    if parece_codigo_cadastro(termo) and " " not in termo:
        return list(docs)

    palavras = [p for p in termo.split() if p]
    if not palavras:
        return list(docs)

    out: list[dict] = []
    for doc in docs:
        blob = blob_busca_de_doc(doc)
        nome = _norm_termo(str(doc.get("Nome") or doc.get("nome") or ""))
        if all(_palavra_casa_no_doc(pl, doc, blob, nome) for pl in palavras):
            out.append(doc)
    return out


def score_relevancia_doc(doc: dict, termo: str) -> int:
    """Espelho de ``relevanciaTextoBuscaPdv`` no cliente (+ bônus prefixo GM)."""
    t = _norm_termo(termo)
    if not t:
        return 0

    ix = doc.get(INDEX_CODIGOS_CAMPO) or doc.get("index_codigos")
    if isinstance(ix, list):
        for x in ix:
            if _norm_termo(str(x or "")) == t:
                return 2_000_000

    from produtos.cadastro_busca_codigo_util import termo_eh_codigo_gm, termo_bate_valor_codigo

    if termo_eh_codigo_gm(t):
        cnfe = doc.get("CodigoNFe") or doc.get("codigo_nfe") or doc.get("codigo_gm") or ""
        if termo_bate_valor_codigo(t, cnfe):
            vn = _norm_termo(str(cnfe))
            if vn == t:
                return 1_900_000
            if vn.startswith(t) or somente_alnum(vn).startswith(somente_alnum(t)):
                return 1_750_000
        if isinstance(ix, list):
            for x in ix:
                if termo_bate_valor_codigo(t, x):
                    return 1_700_000

    nome = _norm_termo(str(doc.get("Nome") or doc.get("nome") or ""))
    blob = blob_busca_de_doc(doc)
    if t in nome:
        return 1_000_000
    if t in blob:
        return 500_000

    palavras = [w for w in t.split() if len(w) >= 2]
    s = 0
    for w in palavras:
        if w in nome:
            s += 50_000
        elif w in blob:
            s += 5_000
    return s


def score_relevancia_row_api(row: dict, termo: str) -> int:
    t = _norm_termo(termo)
    if not t:
        return 0

    ix = row.get("index_codigos")
    if isinstance(ix, list):
        for x in ix:
            if _norm_termo(str(x or "")) == t:
                return 2_000_000

    nome = _norm_termo(str(row.get("nome") or ""))
    blob = blob_busca_de_row_api(row)
    if t in nome:
        return 1_000_000
    if t in blob:
        return 500_000

    palavras = [w for w in t.split() if len(w) >= 2]
    s = 0
    for w in palavras:
        if w in nome:
            s += 50_000
        elif w in blob:
            s += 5_000
    return s


def ordenar_rows_api_estilo_pdv(rows: list[dict], termo: str) -> list[dict]:
    if not rows:
        return []
    return sorted(
        rows,
        key=lambda r: (
            -score_relevancia_row_api(r, termo),
            -float(r.get("media_venda_diaria_30d") or 0),
            str(r.get("nome") or "").lower(),
        ),
    )
