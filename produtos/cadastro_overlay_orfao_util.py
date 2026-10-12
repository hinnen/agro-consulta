"""Overlay «agro:vn:…» — validade sem código ERP; evita duplicar cadastro real."""
from __future__ import annotations

import re
import unicodedata

from django.db.models import Q

AGRO_VN_PREFIX = "agro:vn:"


def eh_id_overlay_sintetico_validade(pid: str) -> bool:
    s = str(pid or "").strip().lower()
    return s.startswith(AGRO_VN_PREFIX) and len(s) > len(AGRO_VN_PREFIX)


def _norm_nome_cadastro(nome: str) -> str:
    n = unicodedata.normalize("NFKD", (nome or "").strip())
    n = "".join(ch for ch in n if not unicodedata.combining(ch))
    n = re.sub(r"\s+", " ", n).strip().lower()
    return n[:300]


def produto_externo_id_catalogo_por_codigo_ou_nome(
    codigo: str = "",
    nome: str = "",
) -> str | None:
    """
    Resolve o ID canônico do catálogo (``Produto.produto_externo_id``) por GM/código ou nome.
    Ignora overlays «agro:vn:» — só produto real no Postgres.
    """
    from produtos.models import Produto

    cod = str(codigo or "").strip()[:64]
    if cod and not eh_id_overlay_sintetico_validade(cod):
        q_cod = (
            Q(codigo_nfe__iexact=cod)
            | Q(codigo_interno__iexact=cod)
            | Q(codigo_barras__iexact=cod)
            | Q(produto_externo_id__iexact=cod)
            | Q(erp_produto_id__iexact=cod)
        )
        al = re.sub(r"[^a-zA-Z0-9]", "", cod).lower()
        if al.startswith("gm") and len(al) >= 5:
            q_cod |= Q(codigo_nfe__iexact=al.upper()) | Q(codigo_interno__iexact=al.upper())
        p = (
            Produto.objects.filter(q_cod)
            .exclude(produto_externo_id__istartswith=AGRO_VN_PREFIX)
            .order_by("pk")
            .only("produto_externo_id")
            .first()
        )
        if p and (p.produto_externo_id or "").strip():
            return str(p.produto_externo_id).strip()[:64]

    nome_s = (nome or "").strip()[:300]
    if not nome_s:
        return None
    p = (
        Produto.objects.filter(nome__iexact=nome_s)
        .exclude(produto_externo_id__istartswith=AGRO_VN_PREFIX)
        .order_by("pk")
        .only("produto_externo_id", "nome")
        .first()
    )
    if p and (p.produto_externo_id or "").strip():
        return str(p.produto_externo_id).strip()[:64]
    return None


def overlay_produto_externo_id_canonico(
    codigo: str = "",
    nome: str = "",
) -> str | None:
    """
    Antes de criar «agro:vn:», tenta o produto real; senão devolve None (criar sintético).
    """
    pid = produto_externo_id_catalogo_por_codigo_ou_nome(codigo=codigo, nome=nome)
    if pid:
        return pid
    nome_s = (nome or "").strip()[:300]
    if not nome_s:
        return None
    from produtos.models import ProdutoGestaoOverlayAgro

    ovs = list(
        ProdutoGestaoOverlayAgro.objects.filter(nome__iexact=nome_s)
        .exclude(produto_externo_id__istartswith=AGRO_VN_PREFIX)
        .only("produto_externo_id")[:5]
    )
    for ov in ovs:
        ps = str(ov.produto_externo_id or "").strip()[:64]
        if ps and not eh_id_overlay_sintetico_validade(ps):
            return ps
    return None


def remover_cadastro_duplicatas_agro_vn(rows: list[dict]) -> list[dict]:
    """
    Na lista do cadastro, esconde linha «agro:vn:» quando já existe outro item com o mesmo nome
    (normalizado) e ID real.
    """
    if not rows:
        return rows
    nomes_reais: set[str] = set()
    for r in rows:
        pid = str(r.get("id") or "").strip()
        if eh_id_overlay_sintetico_validade(pid):
            continue
        nm = _norm_nome_cadastro(str(r.get("nome") or ""))
        if nm:
            nomes_reais.add(nm)
    if not nomes_reais:
        return rows
    out: list[dict] = []
    for r in rows:
        pid = str(r.get("id") or "").strip()
        if eh_id_overlay_sintetico_validade(pid):
            nm = _norm_nome_cadastro(str(r.get("nome") or ""))
            if nm and nm in nomes_reais:
                continue
        out.append(r)
    return out


def remapear_id_planilha_agro_vn(
    pid: str,
    *,
    codigo_gm: str = "",
    nome: str = "",
) -> tuple[str, str | None]:
    """
    Se a planilha traz ID «agro:vn:», tenta o produto real pelo Código GM ou Nome.
    Retorna (id_efetivo, aviso_ou_None).
    """
    pid = str(pid or "").strip()[:64]
    if not eh_id_overlay_sintetico_validade(pid):
        return pid, None
    canon = produto_externo_id_catalogo_por_codigo_ou_nome(codigo=codigo_gm, nome=nome)
    if not canon:
        return pid, (
            "ID agro:vn sem produto real — apague a linha ou preencha Código GM/nome igual ao cadastro GM."
        )
    return canon, f"ID remapeado de {pid[:20]}… para o produto real ({canon[:24]}…)."
