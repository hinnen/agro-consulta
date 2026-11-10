"""Middleware — log de APIs do PDV web."""

from __future__ import annotations

from produtos.pdv_operacao_log_util import registrar_request_pdv


class PdvOperacaoLogMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        response = self.get_response(request)
        try:
            registrar_request_pdv(request, response)
        except Exception:
            pass
        return response
