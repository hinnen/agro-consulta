"""Overlay «agro:vn:…» — validade sem código ERP; evita duplicar cadastro real."""
from __future__ import annotations

import logging
import re
import unicodedata
from decimal import Decimal

from django.db import transaction
from django.db.models import Exists, OuterRef, Q

logger = logging.getLogger(__name__)

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


def queryset_produto_sem_fantasmas_agro_vn(qs):
    """Exclui ``Produto`` com ID agro:vn quando já existe cadastro real com o mesmo nome."""
    from produtos.models import Produto

    real_mesmo_nome = Produto.objects.filter(nome__iexact=OuterRef("nome")).exclude(
        produto_externo_id__istartswith=AGRO_VN_PREFIX
    )
    return qs.exclude(
        Q(produto_externo_id__istartswith=AGRO_VN_PREFIX) & Exists(real_mesmo_nome)
    )


def _mesclar_cadastro_extras_validade(dest_ex: dict, orig_ex: dict) -> dict:
    out = dict(dest_ex or {})
    src = orig_ex if isinstance(orig_ex, dict) else {}
    for k in ("validade", "lote", "validade_alerta", "validade_msg"):
        if not str(out.get(k) or "").strip() and str(src.get(k) or "").strip():
            out[k] = src[k]
    return out


def _migrar_lotes_entre_overlays(origem, destino) -> int:
    from produtos.models import EstoqueLote, sync_overlay_validade_resumo_de_lotes

    movidos = 0
    for lote in EstoqueLote.objects.filter(overlay=origem):
        dup = EstoqueLote.objects.filter(overlay=destino, lote_codigo=lote.lote_codigo).first()
        if dup:
            try:
                dup.quantidade_atual = Decimal(dup.quantidade_atual or 0) + Decimal(
                    lote.quantidade_atual or 0
                )
            except Exception:
                dup.quantidade_atual = lote.quantidade_atual
            dup.save(update_fields=["quantidade_atual"])
            lote.delete()
        else:
            lote.overlay = destino
            lote.save(update_fields=["overlay"])
        movidos += 1
    if movidos:
        sync_overlay_validade_resumo_de_lotes(destino)
    return movidos


@transaction.atomic
def fundir_overlay_agro_vn_no_canonico(
    ghost_pid: str,
    canon_pid: str,
    *,
    nome: str = "",
) -> bool:
    """
    Move lotes/extras do overlay fantasma para o overlay do produto real e remove o fantasma.
    """
    from produtos.models import EstoqueLote, Produto, ProdutoGestaoOverlayAgro

    ghost_pid = str(ghost_pid or "").strip()[:64]
    canon_pid = str(canon_pid or "").strip()[:64]
    if not eh_id_overlay_sintetico_validade(ghost_pid) or not canon_pid:
        return False
    if ghost_pid == canon_pid:
        return False

    ghost = ProdutoGestaoOverlayAgro.objects.filter(produto_externo_id=ghost_pid).first()
    if ghost is None:
        Produto.objects.filter(produto_externo_id=ghost_pid).delete()
        return True

    dest, _ = ProdutoGestaoOverlayAgro.objects.get_or_create(
        produto_externo_id=canon_pid,
        defaults={"nome": (nome or ghost.nome or "")[:300]},
    )
    if nome and not (dest.nome or "").strip():
        dest.nome = nome[:300]
        dest.save(update_fields=["nome"])

    ex_d = dict(dest.cadastro_extras) if isinstance(dest.cadastro_extras, dict) else {}
    ex_g = dict(ghost.cadastro_extras) if isinstance(ghost.cadastro_extras, dict) else {}
    merged = _mesclar_cadastro_extras_validade(ex_d, ex_g)
    if merged != ex_d:
        dest.cadastro_extras = merged
        dest.save(update_fields=["cadastro_extras", "atualizado_em"])

    _migrar_lotes_entre_overlays(ghost, dest)
    EstoqueLote.objects.filter(overlay=ghost).delete()
    ghost.delete()
    Produto.objects.filter(produto_externo_id=ghost_pid).delete()
    logger.info("Overlay agro:vn fundido: %s → %s", ghost_pid, canon_pid)
    return True


def dissolver_fantasma_agro_vn_por_nome(
    nome: str,
    *,
    codigo: str = "",
) -> str | None:
    """
    Se existir overlay/produto agro:vn com esse nome e cadastro real, funde e devolve o ID canônico.
    """
    from produtos.models import ProdutoGestaoOverlayAgro

    canon = overlay_produto_externo_id_canonico(codigo=codigo, nome=nome)
    if not canon:
        return None
    nome_s = (nome or "").strip()[:300]
    if not nome_s:
        return canon
    fantasmas = ProdutoGestaoOverlayAgro.objects.filter(
        nome__iexact=nome_s, produto_externo_id__istartswith=AGRO_VN_PREFIX
    )
    for ov in fantasmas:
        fundir_overlay_agro_vn_no_canonico(ov.produto_externo_id, canon, nome=nome_s)
    return canon


def limpar_todos_fantasmas_agro_vn_com_canonico() -> dict[str, int]:
    """Varre overlays agro:vn e funde nos produtos reais quando o nome bate."""
    from produtos.models import ProdutoGestaoOverlayAgro

    stats = {"fundidos": 0, "sem_canon": 0}
    for ov in ProdutoGestaoOverlayAgro.objects.filter(
        produto_externo_id__istartswith=AGRO_VN_PREFIX
    ).iterator(chunk_size=200):
        canon = overlay_produto_externo_id_canonico(nome=ov.nome or "")
        if not canon:
            stats["sem_canon"] += 1
            continue
        if fundir_overlay_agro_vn_no_canonico(ov.produto_externo_id, canon, nome=ov.nome or ""):
            stats["fundidos"] += 1
    return stats
