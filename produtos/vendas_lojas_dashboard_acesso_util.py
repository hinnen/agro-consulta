"""Acesso ao dashboard mobile: PIN gerência (SistApp) **ou** staff/superuser no Agro desktop.

Mantém o painel antigo intacto. Botão separado no launchpad usa a sessão Django;
o hub SistApp continua com PIN.
"""
from __future__ import annotations

from functools import wraps
from typing import Any

from django.http import HttpRequest, JsonResponse

from produtos.vendas_lojas_hub_acesso_util import (
    mensagem_pin_sem_acesso_vendas_tarefas,
    pin_liberado_vendas_tarefas,
)
from produtos.vendas_lojas_hub_sessao_util import (
    operador_da_sessao,
    pin_da_sessao,
    redirect_para_pin,
)
from produtos.vendas_lojas_sem_acesso_util import resposta_sem_acesso_html


def usuario_desktop_pode_dashboard(request: HttpRequest) -> bool:
    user = getattr(request, "user", None)
    if user is None or not getattr(user, "is_authenticated", False):
        return False
    return bool(getattr(user, "is_staff", False) or getattr(user, "is_superuser", False))


def operador_rotulo_dashboard(request: HttpRequest) -> str:
    nome = (operador_da_sessao(request) or "").strip()
    if nome:
        return nome
    user = getattr(request, "user", None)
    if user is not None and getattr(user, "is_authenticated", False):
        full = (getattr(user, "get_full_name", lambda: "")() or "").strip()
        return full or (getattr(user, "username", "") or "").strip()
    return ""


def _pin_gerencia_ok(request: HttpRequest) -> bool:
    pin = pin_da_sessao(request)
    return bool(pin and pin_liberado_vendas_tarefas(pin))


def exigir_dashboard_mobile_html(view_func):
    @wraps(view_func)
    def _wrap(request: HttpRequest, *args: Any, **kwargs: Any):
        if usuario_desktop_pode_dashboard(request):
            return view_func(request, *args, **kwargs)
        if not operador_da_sessao(request):
            return redirect_para_pin(request)
        if not _pin_gerencia_ok(request):
            return resposta_sem_acesso_html(request, "vendas")
        return view_func(request, *args, **kwargs)

    return _wrap


def exigir_dashboard_mobile_api(view_func):
    @wraps(view_func)
    def _wrap(request: HttpRequest, *args: Any, **kwargs: Any):
        if usuario_desktop_pode_dashboard(request):
            return view_func(request, *args, **kwargs)
        nome = operador_da_sessao(request)
        if not nome:
            return JsonResponse(
                {"ok": False, "erro": "Informe o PIN para continuar.", "precisa_pin": True},
                status=401,
            )
        if not _pin_gerencia_ok(request):
            return JsonResponse(
                {
                    "ok": False,
                    "erro": mensagem_pin_sem_acesso_vendas_tarefas(),
                    "sem_acesso_gerencia": True,
                },
                status=403,
            )
        return view_func(request, *args, **kwargs)

    return _wrap
