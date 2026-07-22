"""Cache do motor BCA (/api/buscar) — compartilhado por PDV, gestão, cadastro, compras, NF, ajuste."""
from __future__ import annotations

from django.core.cache import cache

BCA_BUSCA_CACHE_PREFIX = "bca_busca_v2:"
BCA_BUSCA_TTL_SEC = 90


def bca_busca_cache_key(
    q: str,
    *,
    limit: int,
    wizard: bool,
    entrada_nfe: bool,
    cadastro: bool,
    compras: bool,
) -> str:
    termo = (q or "").strip().lower()[:80]
    return (
        f"{BCA_BUSCA_CACHE_PREFIX}{termo}:{int(limit)}:"
        f"{int(wizard)}:{int(entrada_nfe)}:{int(cadastro)}:{int(compras)}"
    )


def bca_busca_cache_get(key: str):
    try:
        return cache.get(key)
    except Exception:
        return None


def bca_busca_cache_set(key: str, payload: dict, ttl: int = BCA_BUSCA_TTL_SEC) -> None:
    try:
        cache.set(key, payload, int(ttl or BCA_BUSCA_TTL_SEC))
    except Exception:
        pass


def bca_busca_cache_bump_invalidate() -> None:
    """
    Não há delete_pattern portátil em LocMem/Redis sem django-redis.
    Bump via versão embutida no prefixo (trocar BCA_BUSCA_CACHE_PREFIX / vN).
    Para limpar já: sobe versão no código.
    """
    return
