"""Acesso à área Contabilidade — usuários dedicados ao escritório."""
from __future__ import annotations

from functools import wraps

from decouple import config
from django.contrib.auth.decorators import login_required
from django.http import HttpResponseForbidden
from django.shortcuts import redirect


def _usernames_contabilidade() -> frozenset[str]:
    raw = (config("AGRO_CONTABILIDADE_USERNAMES", default="") or "").strip()
    if not raw:
        return frozenset()
    return frozenset(u.strip().lower() for u in raw.split(",") if u.strip())


def usuario_listado_contabilidade(user) -> bool:
    if not user or not user.is_authenticated:
        return False
    uname = (user.get_username() or "").strip().lower()
    return bool(uname and uname in _usernames_contabilidade())


def usuario_pode_acessar_contabilidade(user) -> bool:
    if not user or not user.is_authenticated:
        return False
    if user.is_superuser or user.is_staff:
        return True
    return usuario_listado_contabilidade(user)


def usuario_somente_contabilidade(user) -> bool:
    """Perfil restrito: só telas/exportações do escritório."""
    if not user or not user.is_authenticated:
        return False
    if user.is_superuser or user.is_staff:
        return False
    return usuario_listado_contabilidade(user)


CONTABILIDADE_LOGIN_URL = "/contabilidade/login/"


CONTABILIDADE_PATH_PREFIXES = (
    "/contabilidade",
    "/api/nfce/",
    "/api/lancamentos/export-",
    "/vendas/exportar-csv",
    "/lancamentos/dre",
    "/financeiro/resumo-gerencial",
    "/static/",
    "/healthz",
)


def path_permitido_somente_contabilidade(path: str) -> bool:
    p = (path or "").split("?", 1)[0]
    for prefix in CONTABILIDADE_PATH_PREFIXES:
        if p == prefix.rstrip("/") or p.startswith(prefix):
            return True
    return False


def contabilidade_login_required(view_func):
    """Login + permissão contabilidade (staff ou usuário listado no .env)."""

    @login_required(login_url=CONTABILIDADE_LOGIN_URL)
    @wraps(view_func)
    def wrapper(request, *args, **kwargs):
        if not usuario_pode_acessar_contabilidade(request.user):
            return HttpResponseForbidden("Sem permissão para Contabilidade.")
        return view_func(request, *args, **kwargs)

    return wrapper
