"""Versão do cache curto de ``api/buscar/`` (BCA) — invalidar após mudança em código de barras."""
from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

# Chave em views: f"bca_busca_v{BCA_BUSCA_CACHE_VERSION}:..."
BCA_BUSCA_CACHE_VERSION = 2


def bca_busca_cache_bump_invalidate() -> None:
    """Sobe a versão do prefixo (novas buscas ignoram entradas antigas)."""
    global BCA_BUSCA_CACHE_VERSION
    BCA_BUSCA_CACHE_VERSION += 1
    logger.info("BCA busca cache version → %s", BCA_BUSCA_CACHE_VERSION)
