"""URL pública do WhatsApp gravada pelo PC da loja (túnel) no Postgres do PDV."""
from __future__ import annotations

import logging
import re
from urllib.parse import urlparse

from django.conf import settings
from django.core.cache import cache
from django.utils import timezone

logger = logging.getLogger(__name__)

_CACHE_KEY = "agro_wa_public_origin_registrada"
_CHAVE = "default"
_ORIGIN_RE = re.compile(r"^https://[a-z0-9._-]+(?::\d+)?(?:/|$)", re.I)


def _validar_origin(origin: str) -> str | None:
    o = (origin or "").strip().rstrip("/")
    if not o or len(o) > 480:
        return None
    if not _ORIGIN_RE.match(o + "/"):
        return None
    parsed = urlparse(o)
    host = (parsed.netloc or "").strip().lower()
    if not host or "@" in host:
        return None
    return o


def agro_wa_public_origin_registrada() -> str:
    cached = cache.get(_CACHE_KEY)
    if isinstance(cached, str):
        return cached.rstrip("/")
    origin = ""
    try:
        from produtos.models import WhatsAppPublicOriginAgro

        row = WhatsAppPublicOriginAgro.objects.filter(chave=_CHAVE).first()
        if row:
            origin = (row.origin or "").strip().rstrip("/")
    except Exception:
        logger.exception("agro_wa_public_origin_registrada: leitura")
    cache.set(_CACHE_KEY, origin, 30)
    return origin


def registrar_origin_publica(origin: str, *, fonte: str = "pc") -> tuple[bool, str]:
    o = _validar_origin(origin)
    if not o:
        return False, "URL inválida (use HTTPS, ex.: https://xxx.trycloudflare.com)."
    try:
        from produtos.models import WhatsAppPublicOriginAgro

        row, _ = WhatsAppPublicOriginAgro.objects.get_or_create(chave=_CHAVE, defaults={"origin": o})
        if row.origin != o or row.fonte != fonte:
            row.origin = o
            row.fonte = (fonte or "pc")[:32]
            row.save(update_fields=["origin", "fonte", "atualizado_em"])
        else:
            WhatsAppPublicOriginAgro.objects.filter(pk=row.pk).update(atualizado_em=timezone.now())
    except Exception as exc:
        logger.exception("registrar_origin_publica")
        return False, str(exc)[:200]
    cache.set(_CACHE_KEY, o, 30)
    return True, o


def agro_wa_public_origin_efetiva() -> str:
    """Prioridade: registro do PC no PDV → variável AGRO_WA_PUBLIC_ORIGIN no Render."""
    reg = agro_wa_public_origin_registrada()
    if reg:
        return reg
    return (getattr(settings, "AGRO_WA_PUBLIC_ORIGIN", "") or "").strip().rstrip("/")
