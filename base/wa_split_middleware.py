"""
Separa tráfego WhatsApp do serviço principal (PDV).

No serviço **loja** (PDV): defina ``AGRO_WA_PUBLIC_ORIGIN`` = URL HTTPS do serviço
``agro-consulta-wa`` no Render. Navegador e ponte mal configurada deixam de bater no worker do PDV.

No serviço **WhatsApp**: **não** defina ``AGRO_WA_PUBLIC_ORIGIN`` (senão redireciona em loop).
"""

from __future__ import annotations

import json
import logging
from urllib.parse import urlparse

from django.conf import settings
from django.http import HttpResponse, HttpResponseRedirect

from produtos.agro_wa_origin_util import agro_wa_path_is_split_target, agro_wa_public_origin

logger = logging.getLogger(__name__)


class AgroWhatsAppSplitMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        self._marcar_proxy_sistapp(request)
        origin = agro_wa_public_origin()
        if not origin:
            return self.get_response(request)

        path = getattr(request, "path", "") or ""
        if path == "/healthz" or path.startswith("/healthz/"):
            return self.get_response(request)

        if not agro_wa_path_is_split_target(path):
            return self.get_response(request)

        wa_host = (urlparse(origin).netloc or "").strip().lower()
        incoming = ((request.META.get("HTTP_HOST") or "").split(":")[0] or "").strip().lower()
        if wa_host and incoming == wa_host:
            return self.get_response(request)

        # Ponte no PC ainda apontando para o host do PDV: resposta barata (não processa fila).
        if path.startswith("/api/atendimento-whatsapp/bridge/"):
            body = {
                "ok": False,
                "erro": (
                    "WhatsApp está no serviço dedicado. "
                    "No .env da ponte use AGRO_WA_DJANGO_URL="
                    + origin
                    + " (mesmo AGRO_WA_BRIDGE_TOKEN da loja)."
                ),
                "wa_origin": origin,
            }
            return HttpResponse(
                json.dumps(body, ensure_ascii=False),
                status=503,
                content_type="application/json; charset=utf-8",
            )

        target = origin + request.get_full_path()
        return HttpResponseRedirect(target)

    @staticmethod
    def _marcar_proxy_sistapp(request):
        path = getattr(request, "path", "") or ""
        if not path.startswith("/api/atendimento-whatsapp/"):
            return
        token = (request.META.get("HTTP_X_AGRO_WA_SISTAPP") or "").strip()
        esperado = (getattr(settings, "AGRO_WA_BRIDGE_TOKEN", "") or "").strip()
        if not token or not esperado or token != esperado:
            return
        request._dont_enforce_csrf_checks = True
        request.agro_wa_sistapp_operador = (request.META.get("HTTP_X_AGRO_WA_OPERADOR") or "SistApp").strip()[:120]


class AgroWaSistappUserMiddleware:
    """Depois do login: o SistApp entra no Zap com a chave do servidor, sem tela de entrar."""

    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        nome = (getattr(request, "agro_wa_sistapp_operador", "") or "").strip()
        if nome:
            request.user = _OperadorSistapp(nome)
        return self.get_response(request)


class _OperadorSistapp:
    is_authenticated = True
    is_active = True
    is_anonymous = False
    is_staff = False
    is_superuser = False
    pk = 0
    id = 0

    def __init__(self, nome):
        self.username = nome
        self.nome = nome

    def get_full_name(self):
        return self.nome

    def get_username(self):
        return self.nome

    def __str__(self):
        return self.nome
