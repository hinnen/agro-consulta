"""Sessão de operador (PIN) no app GM Pendências."""

from __future__ import annotations

from functools import wraps
from typing import Any

from django.http import HttpRequest, JsonResponse
from django.shortcuts import redirect

from base.models import PerfilUsuario
from produtos.caixa_util import rotulo_operador_pin, validar_pin_operador

SESSION_OPERADOR = "tarefas_operador_nome"
SESSION_USER_ID = "tarefas_operador_user_id"


def operador_da_sessao(request: HttpRequest) -> str:
    try:
        return str(request.session.get(SESSION_OPERADOR) or "").strip()
    except Exception:
        return ""


def gravar_operador_sessao(request: HttpRequest, pin: str) -> tuple[bool, str, str]:
    """Valida PIN e grava nome na sessão. Retorno: (ok, nome, erro)."""
    ok, err = validar_pin_operador(pin)
    if not ok:
        return False, "", err or "PIN incorreto."
    nome = (rotulo_operador_pin(pin) or "").strip()
    if not nome:
        return False, "", "PIN sem nome no cadastro."
    request.session[SESSION_OPERADOR] = nome[:120]
    perfil = (
        PerfilUsuario.objects.select_related("user")
        .filter(senha_rapida=(pin or "").strip(), ativo=True)
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
    request.session.modified = True


def exigir_operador_html(view_func):
    @wraps(view_func)
    def _wrap(request: HttpRequest, *args: Any, **kwargs: Any):
        if not operador_da_sessao(request):
            return redirect("tarefas_pin")
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
        request.tarefas_operador = nome  # type: ignore[attr-defined]
        return view_func(request, *args, **kwargs)

    return _wrap
