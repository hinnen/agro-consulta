"""Devolve nome/marca do cadastro apagados pelo vínculo NF (xProd no nome)."""
from __future__ import annotations

import re
from typing import Any

from produtos.models import (
    Produto,
    ProdutoCadastroAlteracaoAgro,
    ProdutoGestaoOverlayAgro,
)

_RE_EAN_COLCHETE = re.compile(r"\[[0-9]{8,}\]")
_NOME_LIXO = frozenset({"", "—", "-", "–", "[NOME QUEBRADO]", "..."})

_CAMPOS_TXT = (
    ("nome", "nome", 300),
    ("marca", "marca", 120),
    ("categoria", "categoria", 200),
    ("unidade", "unidade", 20),
    ("codigo_nfe", "codigo_nfe", 64),
    ("codigo_barras", "codigo_barras", 50),
)


def parece_nome_nf(texto: str) -> bool:
    """Nome da nota: descrição do XML com EAN entre colchetes."""
    return bool(_RE_EAN_COLCHETE.search(str(texto or "")))


def _limpo(val: Any) -> str:
    return str(val or "").strip()


def _hist_bom(val: str) -> bool:
    s = _limpo(val)
    if s in _NOME_LIXO or parece_nome_nf(s):
        return False
    return len(s) >= 3


def _ultimo_antes_wipe(pid: str, campo: str) -> str:
    rows = list(
        ProdutoCadastroAlteracaoAgro.objects.filter(produto_externo_id=pid[:64], campo=campo)
        .order_by("-criado_em", "-id")[:80]
    )
    wipe_row = None
    for row in rows:
        depois = _limpo(row.valor_depois)
        antes = _limpo(row.valor_antes)
        if depois in ("", "—", "-", "–") and _hist_bom(antes):
            wipe_row = row
            break
    if wipe_row is None:
        return ""
    for row in rows:
        mais_novo = row.criado_em > wipe_row.criado_em or (
            row.criado_em == wipe_row.criado_em and int(row.pk or 0) > int(wipe_row.pk or 0)
        )
        if not mais_novo:
            continue
        depois = _limpo(row.valor_depois)
        if depois not in ("", "—", "-", "–") and _hist_bom(depois):
            return ""
    return _limpo(wipe_row.valor_antes)


def _candidatos_queryset(pid: str = ""):
    pids: set[str] = set()
    ov_qs = ProdutoGestaoOverlayAgro.objects.all()
    p_qs = Produto.objects.all()
    if pid:
        ov_qs = ov_qs.filter(produto_externo_id=pid[:64])
        p_qs = p_qs.filter(produto_externo_id=pid[:64])
    for ov in ov_qs.only("produto_externo_id", "nome").iterator(chunk_size=300):
        if parece_nome_nf(ov.nome):
            pids.add((ov.produto_externo_id or "").strip())
    for p in p_qs.only("produto_externo_id", "nome").iterator(chunk_size=300):
        if parece_nome_nf(p.nome):
            pids.add((p.produto_externo_id or "").strip())
    pids.discard("")
    return Produto.objects.filter(produto_externo_id__in=list(pids))


def planejar_reparo_vinculo_nf(*, pid: str = "") -> list[dict[str, Any]]:
    """Lista o que devolveria (não grava)."""
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for p in _candidatos_queryset(pid).iterator(chunk_size=100):
        pid_k = (p.produto_externo_id or "").strip()
        if not pid_k or pid_k in seen:
            continue
        seen.add(pid_k)
        ov = ProdutoGestaoOverlayAgro.objects.filter(produto_externo_id=pid_k[:64]).first()
        patch_p: dict[str, str] = {}
        patch_ov: dict[str, str] = {}

        if ov is not None and parece_nome_nf(ov.nome):
            patch_ov["nome"] = ""
        if parece_nome_nf(p.nome):
            bom = _ultimo_antes_wipe(pid_k, "nome")
            if _hist_bom(bom):
                patch_p["nome"] = bom[:300]

        for campo, attr, mx in _CAMPOS_TXT:
            if campo == "nome":
                continue
            cur_p = _limpo(getattr(p, attr, None))
            cur_ov = _limpo(getattr(ov, attr, None) if ov is not None else "")
            if campo == "unidade" and cur_p.upper() not in ("", "UN") and cur_ov.upper() not in ("", "UN"):
                continue
            if campo != "unidade" and (cur_p or cur_ov):
                continue
            bom = _ultimo_antes_wipe(pid_k, campo)
            if not _hist_bom(bom):
                continue
            if campo == "unidade" and bom.upper() == "UN" and cur_p.upper() == "UN":
                continue
            patch_p[attr] = bom[:mx]

        if not patch_p and not patch_ov:
            continue
        out.append(
            {
                "pid": pid_k,
                "codigo_nfe": _limpo(p.codigo_nfe) or _limpo(getattr(ov, "codigo_nfe", None) if ov else ""),
                "nome_agora": _limpo((ov.nome if ov and _limpo(ov.nome) else None) or p.nome),
                "nome_volta": patch_p.get("nome") or (_limpo(p.nome) if patch_ov.get("nome") == "" else ""),
                "patch_produto": patch_p,
                "limpar_overlay_nome": "nome" in patch_ov,
                "patch_overlay": patch_ov,
            }
        )
    return out


def aplicar_reparo_vinculo_nf(planos: list[dict[str, Any]], *, usuario=None) -> int:
    """Grava os patches. Retorna quantos produtos mexeu."""
    from produtos.cadastro_alteracao_historico_util import registrar_diffs_cadastro

    n = 0
    for pl in planos:
        pid = str(pl.get("pid") or "").strip()[:64]
        if not pid:
            continue
        p = Produto.objects.filter(produto_externo_id=pid).first()
        if p is None:
            continue
        ov = ProdutoGestaoOverlayAgro.objects.filter(produto_externo_id=pid).first()
        antes: dict[str, Any] = {"nome": (ov.nome if ov and _limpo(ov.nome) else p.nome)}
        depois: dict[str, Any] = dict(antes)
        changed = False
        for attr, val in (pl.get("patch_produto") or {}).items():
            setattr(p, attr, val)
            depois[attr] = val
            changed = True
        if changed:
            p.save()
        ov_fields: list[str] = []
        for attr, val in (pl.get("patch_overlay") or {}).items():
            if ov is None:
                break
            setattr(ov, attr, val)
            ov_fields.append(attr)
            if attr == "nome":
                depois["nome"] = val or _limpo(p.nome)
        if ov is not None and ov_fields:
            ov.save(update_fields=ov_fields + ["atualizado_em"])
            changed = True
        if changed:
            n += 1
            try:
                registrar_diffs_cadastro(
                    produto_id=pid,
                    antes=antes,
                    depois=depois,
                    usuario=usuario,
                    origem="outro",
                )
            except Exception:
                pass
    return n
