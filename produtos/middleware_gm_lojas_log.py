"""Middleware — log de requisições do PWA GM Lojas."""

from __future__ import annotations

from produtos.gm_lojas_app_log_util import registrar_request_gm_lojas


class GmLojasAppLogMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        response = self.get_response(request)
        try:
            registrar_request_gm_lojas(request, response)
        except Exception:
            pass
        return response
