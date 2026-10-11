"""Sessão PIN — tela Vendas por loja (hub GM Lojas)."""

from __future__ import annotations

from functools import wraps
from typing import Any

from django.http import HttpRequest, JsonResponse
from django.shortcuts import redirect

from base.models import PerfilUsuario
from produtos.caixa_util import rotulo_operador_pin, validar_pin_operador
from produtos.vendas_lojas_hub_acesso_util import (
    mensagem_pin_sem_acesso_vendas_tarefas,
    pin_liberado_vendas_tarefas,
)

SESSION_OPERADOR = "vendas_lojas_operador_nome"
SESSION_USER_ID = "vendas_lojas_operador_user_id"
SESSION_PIN = "vendas_lojas_operador_pin"


def operador_da_sessao(request: HttpRequest) -> str:
    try:
        pin = str(request.session.get(SESSION_PIN) or "").strip()
        if not pin or not pin_liberado_vendas_tarefas(pin):
            if request.session.get(SESSION_OPERADOR):
                limpar_operador_sessao(request)
            return ""
        return str(request.session.get(SESSION_OPERADOR) or "").strip()
    except Exception:
        return ""


def gravar_operador_sessao(request: HttpRequest, pin: str) -> tuple[bool, str, str]:
    pin = (pin or "").strip()
    ok, err = validar_pin_operador(pin)
    if not ok:
        return False, "", err or "PIN incorreto."
    if not pin_liberado_vendas_tarefas(pin):
        return False, "", mensagem_pin_sem_acesso_vendas_tarefas()
    nome = (rotulo_operador_pin(pin) or "").strip()
    if not nome:
        return False, "", "PIN sem nome no cadastro."
    request.session[SESSION_OPERADOR] = nome[:120]
    request.session[SESSION_PIN] = pin[:32]
    perfil = (
        PerfilUsuario.objects.select_related("user")
        .filter(senha_rapida=pin, ativo=True)
        .first()
    )
    uid = getattr(getattr(perfil, "user", None), "pk", None) if perfil else None
    if uid:
        request.session[SESSION_USER_ID] = int(uid)
    else:
        request.session.pop(SESSION_USER_ID, None)
    request.session.modified = True
    return True, nome[:120], ""


def limpar_operador_sessao(request: HttpRequest) -> None:
    request.session.pop(SESSION_OPERADOR, None)
    request.session.pop(SESSION_USER_ID, None)
    request.session.pop(SESSION_PIN, None)
    request.session.modified = True


def exigir_operador_html(view_func):
    @wraps(view_func)
    def _wrap(request: HttpRequest, *args: Any, **kwargs: Any):
        if not operador_da_sessao(request):
            return redirect("vendas_lojas_pin")
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
        request.vendas_lojas_operador = nome  # type: ignore[attr-defined]
        return view_func(request, *args, **kwargs)

    return _wrap
