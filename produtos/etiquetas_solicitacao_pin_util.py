"""PIN do app Etiquetas (hub GM Lojas) — mesma sessão que Fotos."""

from __future__ import annotations

from functools import wraps
from typing import Any

from django.http import HttpRequest, JsonResponse
from django.shortcuts import redirect

from produtos.fotos_produto_pin_util import (
    SESSION_OPERADOR,
    gravar_operador_sessao,
    limpar_operador_sessao,
    operador_da_sessao,
    validar_pin_operador,
)

__all__ = [
    "SESSION_OPERADOR",
    "gravar_operador_sessao",
    "limpar_operador_sessao",
    "operador_da_sessao",
    "validar_pin_operador",
    "exigir_operador_html",
    "exigir_operador_api",
]


def exigir_operador_html(view_func):
    @wraps(view_func)
    def _wrap(request: HttpRequest, *args: Any, **kwargs: Any):
        if not operador_da_sessao(request):
            return redirect("etiquetas_solicitacao_pin")
        return view_func(request, *args, **kwargs)

    return _wrap


def exigir_operador_api(view_func):
    @wraps(view_func)
    def _wrap(request: HttpRequest, *args: Any, **kwargs: Any):
        nome = operador_da_sessao(request)
        if not nome:
            return JsonResponse(
                {"ok": False, "erro": "Informe o PIN para continuar.", "precisa_pin": True},
                status=401,
            )
        request.fotos_operador = nome  # type: ignore[attr-defined]
        return view_func(request, *args, **kwargs)

    return _wrap
