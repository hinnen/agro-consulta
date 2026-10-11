"""Vendas lojas — exige PIN da gerência (sessão hub compartilhada, 7 dias)."""

from __future__ import annotations

from functools import wraps
from typing import Any

from django.http import HttpRequest, JsonResponse
from produtos.vendas_lojas_hub_acesso_util import (
    mensagem_pin_sem_acesso_vendas_tarefas,
    pin_liberado_vendas_tarefas,
)
from produtos.vendas_lojas_sem_acesso_util import resposta_sem_acesso_html
from produtos.vendas_lojas_hub_sessao_util import (
    gravar_operador_sessao,
    limpar_operador_sessao,
    operador_da_sessao,
    pin_da_sessao,
    redirect_para_pin,
)

SESSION_OPERADOR = "gm_lojas_operador_nome"
SESSION_USER_ID = "gm_lojas_operador_user_id"
SESSION_PIN = "gm_lojas_operador_pin"


def _gerencia_liberada(request: HttpRequest) -> bool:
    pin = pin_da_sessao(request)
    return bool(pin and pin_liberado_vendas_tarefas(pin))


def exigir_operador_html(view_func):
    @wraps(view_func)
    def _wrap(request: HttpRequest, *args: Any, **kwargs: Any):
        if not operador_da_sessao(request):
            return redirect_para_pin(request)
        if not _gerencia_liberada(request):
            return resposta_sem_acesso_html(request, "vendas")
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
        if not _gerencia_liberada(request):
            return JsonResponse(
                {
                    "ok": False,
                    "erro": mensagem_pin_sem_acesso_vendas_tarefas(),
                    "sem_acesso_gerencia": True,
                },
                status=403,
            )
        request.vendas_lojas_operador = nome  # type: ignore[attr-defined]
        return view_func(request, *args, **kwargs)

    return _wrap


__all__ = [
    "SESSION_OPERADOR",
    "SESSION_PIN",
    "SESSION_USER_ID",
    "exigir_operador_api",
    "exigir_operador_html",
    "gravar_operador_sessao",
    "limpar_operador_sessao",
    "operador_da_sessao",
]
