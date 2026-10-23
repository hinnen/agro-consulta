"""Fila de etiquetas — colar códigos e carregar mais vendidos do período."""

from __future__ import annotations

import re
from datetime import datetime
from typing import Any

from django.db.models import Q

from produtos.models import Produto, ProdutoGestaoOverlayAgro

MAX_CODIGOS = 400
MAX_MAIS_VENDIDOS = 200

_RE_SEP_CELULA = re.compile(r"[\t|;,]+")
_RE_WS = re.compile(r"\s+")
_SKIP_TOKENS = frozenset(
    {
        "codigo",
        "código",
        "cod",
        "gm",
        "produto",
        "nome",
        "pos",
        "#",
        "qtd",
        "total",
    }
)


def extrair_tokens_codigos(texto: str) -> list[str]:
    """Pega 1º token de cada linha (cola Excel / lista GM + nome). Mantém ordem, sem duplicar."""
    bruto = str(texto or "").replace("\r\n", "\n").replace("\r", "\n")
    out: list[str] = []
    seen: set[str] = set()
    for line in bruto.split("\n"):
        line = line.strip()
        if not line:
            continue
        cell = _RE_SEP_CELULA.split(line, maxsplit=1)[0].strip()
        if not cell:
            continue
        tok = _RE_WS.split(cell, maxsplit=1)[0].strip()
        if not tok or len(tok) > 64:
            continue
        key = tok.casefold()
        # cabeçalho / posição curta — ignora; códigos tipo 4680 (4+) passam
        if key in _SKIP_TOKENS or (key.isdigit() and len(tok) <= 2):
            continue
        if key in seen:
            continue
        seen.add(key)
        out.append(tok)
        if len(out) >= MAX_CODIGOS:
            break
    return out


def _rows_por_pids(pids: list[str], *, inativos: bool = True) -> dict[str, dict]:
    """Hidrata produtos do catálogo PG (overlay) na ordem dos ids pedidos."""
    from produtos.catalogo_agro import _rows_de_produtos

    clean = [str(p or "").strip()[:64] for p in pids if str(p or "").strip()]
    if not clean:
        return {}
    qs = Produto.objects.filter(
        Q(produto_externo_id__in=clean) | Q(erp_produto_id__in=clean)
    )
    if not inativos:
        qs = qs.filter(cadastro_inativo=False, ativo=True)
    by_pid: dict[str, Produto] = {}
    for p in qs:
        for key in (
            str(p.produto_externo_id or "").strip()[:64],
            str(p.erp_produto_id or "").strip()[:64],
        ):
            if key and key not in by_pid:
                by_pid[key] = p
    rows_list = _rows_de_produtos(list({id(p): p for p in by_pid.values()}.values()))
    out: dict[str, dict] = {}
    for row in rows_list:
        rid = str(row.get("id") or "").strip()
        if rid:
            out[rid] = row
    return out


def _candidatos_por_token(tok: str) -> list[str]:
    """Retorna produto_externo_id candidatos (melhor = exato no GM)."""
    t = (tok or "").strip()
    if not t:
        return []
    ids: list[str] = []
    seen: set[str] = set()

    def _add(pid: str) -> None:
        pid = str(pid or "").strip()[:64]
        if pid and pid not in seen:
            seen.add(pid)
            ids.append(pid)

    # 1) Produto: GM / interno / barras exatos
    q_prod = (
        Q(codigo_nfe__iexact=t)
        | Q(codigo_interno__iexact=t)
        | Q(codigo_barras__iexact=t)
    )
    for p in Produto.objects.filter(q_prod).only(
        "produto_externo_id", "erp_produto_id", "codigo_nfe", "codigo_interno"
    )[:8]:
        _add(p.produto_externo_id or p.erp_produto_id or "")

    # 2) Overlay (GM da loja)
    for ov in ProdutoGestaoOverlayAgro.objects.filter(
        Q(codigo_nfe__iexact=t) | Q(codigo_barras__iexact=t)
    ).only("produto_externo_id")[:8]:
        _add(ov.produto_externo_id)

    # 3) Fallback: busca leve do catálogo (prefixo GM / barras)
    if not ids:
        try:
            from produtos import catalogo_agro

            chunk = catalogo_agro.buscar(t, limit=5, inativos=True) or []
            for row in chunk:
                _add(str(row.get("id") or ""))
        except Exception:
            pass
    return ids


def resolver_produtos_por_codigos(
    texto_ou_lista: str | list[str],
    *,
    inativos: bool = True,
) -> dict[str, Any]:
    if isinstance(texto_ou_lista, list):
        tokens = []
        seen: set[str] = set()
        for raw in texto_ou_lista:
            t = str(raw or "").strip()
            if not t:
                continue
            k = t.casefold()
            if k in seen:
                continue
            seen.add(k)
            tokens.append(t[:64])
            if len(tokens) >= MAX_CODIGOS:
                break
    else:
        tokens = extrair_tokens_codigos(texto_ou_lista)

    encontrados: list[dict] = []
    nao_encontrados: list[str] = []
    ambiguos: list[dict] = []
    seen_pid: set[str] = set()

    for tok in tokens:
        cands = _candidatos_por_token(tok)
        if not cands:
            nao_encontrados.append(tok)
            continue
        rows_map = _rows_por_pids(cands, inativos=inativos)
        # preferir 1º candidato que hidratou
        escolhido = None
        for pid in cands:
            if pid in rows_map:
                escolhido = rows_map[pid]
                break
        if not escolhido:
            nao_encontrados.append(tok)
            continue
        pid = str(escolhido.get("id") or "")
        if len(cands) > 1:
            ambiguos.append({"codigo": tok, "qtd_matches": len(cands), "id": pid})
        if pid in seen_pid:
            continue
        seen_pid.add(pid)
        encontrados.append(escolhido)

    return {
        "produtos": encontrados,
        "nao_encontrados": nao_encontrados,
        "ambiguos": ambiguos,
        "pedidos": len(tokens),
        "achados": len(encontrados),
    }


def produtos_mais_vendidos_para_etiquetas(
    desde: datetime,
    ate: datetime,
    *,
    limite: int = 100,
    ordenar: str = "valor",
    sentido: str = "mais",
    categoria: object = None,
    subcategoria: object = None,
    subcategoria_2: object = None,
    subcategoria_3: object = None,
    subcategoria_4: object = None,
    inativos: bool = True,
) -> dict[str, Any]:
    from produtos import relatorios_vendas_util as ru

    lim = max(1, min(MAX_MAIS_VENDIDOS, int(limite or 100)))
    ordenar = (ordenar or "valor").strip().lower()
    if ordenar not in ("valor", "qtd"):
        ordenar = "valor"
    sentido = (sentido or "mais").strip().lower()
    if sentido not in ("mais", "menos"):
        sentido = "mais"

    facetas, rows_all = ru.facetas_categoria_sub(
        desde,
        ate,
        ordenar=ordenar,
        sentido=sentido,
        categoria=categoria,
        subcategoria=subcategoria,
        subcategoria_2=subcategoria_2,
        subcategoria_3=subcategoria_3,
        subcategoria_4=subcategoria_4,
    )
    rows = ru.limitar_ranking(rows_all, lim)
    pids = [str(r.get("produto_id") or "").strip() for r in rows if r.get("produto_id")]
    mapa = _rows_por_pids(pids, inativos=inativos)
    produtos: list[dict] = []
    for r in rows:
        pid = str(r.get("produto_id") or "").strip()
        prod = mapa.get(pid)
        if not prod:
            # fallback mínimo pra não perder o ranking
            prod = {
                "id": pid,
                "nome": r.get("nome") or pid,
                "codigo_nfe": r.get("codigo") or "",
                "codigo_gm": r.get("codigo") or "",
                "codigo": r.get("codigo") or "",
                "codigo_barras": "",
                "preco_venda": 0,
                "peso_etiqueta": "",
            }
        else:
            prod = dict(prod)
        prod["rank_pos"] = r.get("pos")
        prod["rank_qtd"] = r.get("qtd")
        prod["rank_valor"] = r.get("valor")
        produtos.append(prod)
    return {
        "produtos": produtos,
        "total": len(produtos),
        "facetas": {
            "categorias": facetas.get("categorias") or [],
            "subcategorias": facetas.get("subcategorias") or [],
        },
    }
