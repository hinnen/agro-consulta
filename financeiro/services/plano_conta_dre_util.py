"""Resolve grafia de título → PlanoContaAgro (cadastro vivo) para o DRE.

Não altera ``TituloFinanceiroAgro.plano_conta``. Sem cadastro/DB, devolve None
e o DRE cai na planilha CSV + heurística.
"""
from __future__ import annotations

import time
import unicodedata

_CACHE_TTL_S = 60.0
_cache_at = 0.0
_cache_data: dict[str, tuple[str, str, str]] | None = None


def norm_plano_chave(nome: str) -> str:
    s = unicodedata.normalize("NFKD", (nome or "").strip())
    s = "".join(c for c in s if not unicodedata.combining(c))
    return " ".join(s.casefold().split())


def _fold_grupo(s: str) -> str:
    s = unicodedata.normalize("NFKD", (s or "").strip())
    s = "".join(c for c in s if not unicodedata.combining(c))
    return s.casefold()


def _carregar_cadastro_dre() -> dict[str, tuple[str, str, str]]:
    """norm → (nome_oficial, tipo, grupo). Cache curto (TTL) por request/worker."""
    global _cache_at, _cache_data
    now = time.monotonic()
    if _cache_data is not None and (now - _cache_at) < _CACHE_TTL_S:
        return _cache_data
    out: dict[str, tuple[str, str, str]] = {}
    try:
        from produtos.models import PlanoContaAgro, PlanoContaAliasAgro

        for nome, tipo, grupo in PlanoContaAgro.objects.filter(ativo=True).values_list(
            "nome", "tipo", "grupo"
        ):
            n = (nome or "").strip()
            if not n:
                continue
            out[norm_plano_chave(n)] = (n, (tipo or "").strip(), (grupo or "").strip())
        for grafia, nome, tipo, grupo in PlanoContaAliasAgro.objects.filter(
            plano__ativo=True
        ).values_list("grafia", "plano__nome", "plano__tipo", "plano__grupo"):
            g = (grafia or "").strip()
            n = (nome or "").strip()
            if not g or not n:
                continue
            out[norm_plano_chave(g)] = (n, (tipo or "").strip(), (grupo or "").strip())
    except Exception:
        out = {}
    _cache_data = out
    _cache_at = now
    return out


def invalidar_cache_cadastro_dre() -> None:
    global _cache_at, _cache_data
    _cache_at = 0.0
    _cache_data = None


def resolver_plano_cadastro(nome_plano: str) -> dict[str, str] | None:
    """Nome oficial + tipo + grupo, ou None se não estiver no cadastro."""
    g = (nome_plano or "").strip()
    if not g:
        return None
    hit = _carregar_cadastro_dre().get(norm_plano_chave(g))
    if not hit:
        return None
    nome, tipo, grupo = hit
    return {"nome": nome, "tipo": tipo, "grupo": grupo}


def natureza_dre_por_cadastro(nome_plano: str) -> str | None:
    """Natureza DRE a partir do cadastro vivo; None se plano não cadastrado."""
    info = resolver_plano_cadastro(nome_plano)
    if not info:
        return None
    from financeiro.models import LancamentoFinanceiro as NF

    t = (info.get("tipo") or "").strip().casefold()
    g = _fold_grupo(info.get("grupo") or "")
    if t == "fixa":
        return NF.NATUREZA_DESPESA_FIXA
    if t in ("variavel", "variável"):
        return NF.NATUREZA_DESPESA_VARIAVEL
    if "cmv" in g or "mercadoria" in g:
        return NF.NATUREZA_CMV
    if "emprestimo" in g:
        return NF.NATUREZA_EMPRESTIMO_AMORTIZACAO
    if "socio" in g:
        return NF.NATUREZA_RETIRADA_SOCIO
    return NF.NATUREZA_DESPESA_FINANCEIRA


def nome_oficial_plano(nome_plano: str) -> str:
    info = resolver_plano_cadastro(nome_plano)
    if info and info.get("nome"):
        return str(info["nome"])
    return (nome_plano or "").strip()
