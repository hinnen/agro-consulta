"""Catálogo PostgreSQL (``Produto``) — ``AGRO_FONTE_CATALOGO=agro_pg``."""
from __future__ import annotations

from django.db.models import Q

from produtos.models import Produto

_SORT_MAP = {
    "nome": "nome",
    "marca": "marca",
    "unidade": "unidade",
    "categoria": "categoria",
    "subcategoria": "subcategoria",
    "preco_custo": "custo",
    "preco_venda": "preco_venda",
}


def _dec(v) -> float:
    try:
        return round(float(v or 0), 2)
    except (TypeError, ValueError):
        return 0.0


def produto_agro_para_row(p: Produto) -> dict:
    pid = (p.produto_externo_id or p.erp_produto_id or str(p.pk)).strip()
    return {
        "id": pid,
        "nome": (p.nome or "").strip(),
        "marca": (p.marca or "").strip(),
        "codigo": (p.codigo_interno or "").strip(),
        "codigo_nfe": (p.codigo_nfe or p.codigo_interno or "").strip(),
        "codigo_barras": (p.codigo_barras or "").strip(),
        "preco_venda": _dec(p.preco_venda),
        "preco_custo": _dec(p.custo),
        "categoria": (p.categoria or "").strip(),
        "subcategoria": (p.subcategoria or "").strip(),
        "subcategoria_2": (p.subcategoria_2 or "").strip(),
        "subcategoria_3": (p.subcategoria_3 or "").strip(),
        "subcategoria_4": (p.subcategoria_4 or "").strip(),
        "categoria_listagem": "",
        "prateleira": "",
        "fornecedor": (p.fornecedor_texto or "").strip(),
        "imagem": "",
        "inativo": bool(p.cadastro_inativo or not p.ativo),
        "unidade": (p.unidade or "UN").strip() or "UN",
        "descricao": (p.descricao or "").strip(),
        "ncm": (p.ncm or "").strip(),
        "cadastro_somente_agro": bool(p.cadastro_somente_agro),
        "fonte": "agro_pg",
    }


def queryset_catalogo_ativos(*, inativos: bool = False):
    qs = Produto.objects.all()
    if not inativos:
        qs = qs.filter(cadastro_inativo=False, ativo=True)
    return qs


def listar_paginado(
    *,
    pagina: int = 1,
    por_pagina: int = 72,
    sort_key: str = "nome",
    sort_direction: int = 1,
    inativos: bool = False,
) -> tuple[list[dict], int]:
    qs = queryset_catalogo_ativos(inativos=inativos)
    field = _SORT_MAP.get(sort_key, "nome")
    order = field if sort_direction >= 0 else f"-{field}"
    total = qs.count()
    skip = max(0, (pagina - 1) * por_pagina)
    rows = [produto_agro_para_row(p) for p in qs.order_by(order, "pk")[skip : skip + por_pagina]]
    return rows, total


def buscar(q: str, *, limit: int = 80, inativos: bool = False) -> list[dict]:
    termo = (q or "").strip()
    if not termo:
        return []
    qs = queryset_catalogo_ativos(inativos=inativos)
    digits = "".join(ch for ch in termo if ch.isdigit())
    q_obj = (
        Q(nome__icontains=termo)
        | Q(marca__icontains=termo)
        | Q(categoria__icontains=termo)
        | Q(codigo_interno__icontains=termo)
        | Q(codigo_nfe__icontains=termo)
        | Q(codigo_barras__icontains=termo)
        | Q(produto_externo_id__icontains=termo)
        | Q(erp_produto_id__icontains=termo)
    )
    if digits and len(digits) >= 4:
        q_obj |= Q(codigo_barras__icontains=digits) | Q(codigo_interno__icontains=digits)
    return [produto_agro_para_row(p) for p in qs.filter(q_obj).order_by("nome", "pk")[:limit]]


def obter_produto_model(produto_id: str) -> Produto | None:
    pid = (produto_id or "").strip()
    if not pid:
        return None
    p = (
        Produto.objects.filter(
            Q(produto_externo_id=pid) | Q(erp_produto_id=pid) | Q(codigo_interno=pid)
        )
        .order_by("pk")
        .first()
    )
    if p is None and pid.isdigit():
        try:
            p = Produto.objects.filter(pk=int(pid)).first()
        except (TypeError, ValueError):
            p = None
    return p


def produto_por_externo_id(produto_id: str) -> dict | None:
    p = obter_produto_model(produto_id)
    if p is None:
        return None
    return produto_agro_para_row(p)


def produto_model_para_detalhe(p: Produto) -> dict:
    row = produto_agro_para_row(p)
    pv = float(row.get("preco_venda") or 0)
    pc = float(row.get("preco_custo") or 0)
    mva_rs = round(pv - pc, 2) if pv and pc else 0.0
    mva_pct = round((mva_rs / pc) * 100, 2) if pc > 0 else 0.0
    return {
        **row,
        "preco_custo_final": pc,
        "mva_rs": mva_rs,
        "mva_percentual": mva_pct,
        "cadastro_somente_agro": bool(p.cadastro_somente_agro),
        "fonte": "agro_pg",
    }
