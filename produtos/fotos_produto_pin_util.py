"""Sessão PIN do app Fotos — usa sessão única do hub GM Lojas."""

from __future__ import annotations

from functools import wraps
from typing import Any

from django.http import HttpRequest, JsonResponse

from produtos.vendas_lojas_hub_sessao_util import (
    exigir_hub_operador_api,
    exigir_hub_operador_html,
    gravar_operador_sessao,
    limpar_operador_sessao,
    operador_da_sessao,
    redirect_para_pin,
)

# Compat — testes e imports antigos
SESSION_OPERADOR = "gm_lojas_operador_nome"
SESSION_USER_ID = "gm_lojas_operador_user_id"


def exigir_operador_html(view_func):
    return exigir_hub_operador_html(view_func)


def exigir_operador_api(view_func):
    @wraps(view_func)
    def _wrap(request: HttpRequest, *args: Any, **kwargs: Any):
        inner = exigir_hub_operador_api(view_func)
        response = inner(request, *args, **kwargs)
        if hasattr(request, "gm_lojas_operador"):
            request.fotos_operador = request.gm_lojas_operador  # type: ignore[attr-defined]
        return response

    return _wrap


__all__ = [
    "SESSION_OPERADOR",
    "SESSION_USER_ID",
    "exigir_operador_api",
    "exigir_operador_html",
    "gravar_operador_sessao",
    "limpar_operador_sessao",
    "operador_da_sessao",
    "redirect_para_pin",
]
