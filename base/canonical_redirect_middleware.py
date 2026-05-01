"""
Redireciona o hostname público Render (RENDER_EXTERNAL_HOSTNAME) para AGRO_CANONICAL_ORIGIN.

Use no serviço de produção: ``AGRO_CANONICAL_ORIGIN=https://www.sistvale.com.br``.
Preview de PR (``IS_PULL_REQUEST=true``): não redireciona para não jogar pré-visualização para o domínio oficial.
Saúde: ``/healthz`` não redireciona (probe direto ao host ``onrender.com``).

Em homolog/staging próprio omita ``AGRO_CANONICAL_ORIGIN`` para não apontar tudo ao domínio de produção.
"""

from __future__ import annotations

import logging
import os

from django.conf import settings
from django.http import HttpResponsePermanentRedirect

logger = logging.getLogger(__name__)


class AgroCanonicalHostRedirectMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        if not getattr(
            settings, "AGRO_CANONICAL_REDIRECT_FROM_RENDER_ENABLED", False
        ):
            return self.get_response(request)

        path = getattr(request, "path", "") or ""
        if path == "/healthz" or path.startswith("/healthz/"):
            return self.get_response(request)

        meta = getattr(request, "META", None) or {}
        raw_host = meta.get("HTTP_HOST") or ""
        incoming = raw_host.split(":")[0].strip().lower()

        legacy = (os.environ.get("RENDER_EXTERNAL_HOSTNAME") or "").strip().lower()
        if not legacy or incoming != legacy:
            return self.get_response(request)

        base = getattr(settings, "AGRO_CANONICAL_ORIGIN", "").strip().rstrip("/")
        if not base:
            return self.get_response(request)

        if not base.lower().startswith("https://"):
            logger.warning(
                "AGRO_CANONICAL_ORIGIN deve ser https; redirect não aplicado (%s)",
                base[:48],
            )
            return self.get_response(request)

        return HttpResponsePermanentRedirect(base + request.get_full_path())
