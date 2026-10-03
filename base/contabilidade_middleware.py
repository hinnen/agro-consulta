"""Restringe usuários «só contabilidade» às rotas do escritório."""
from __future__ import annotations

from django.shortcuts import redirect

from produtos.contabilidade_acesso_util import (
    path_permitido_somente_contabilidade,
    usuario_somente_contabilidade,
)


class AgroContabilidadeMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        user = getattr(request, "user", None)
        if user and usuario_somente_contabilidade(user):
            path = request.path or ""
            if path in ("/", "/atalhos/"):
                return redirect("/contabilidade/")
            if not path_permitido_somente_contabilidade(path):
                return redirect("/contabilidade/")
        return self.get_response(request)
