"""Acesso ao laboratório de análise de crédito (shadow) — padrão prévia BI."""

from __future__ import annotations

from functools import wraps

from django.conf import settings
from django.contrib.auth.decorators import login_required
from django.http import Http404


def credito_score_shadow_enabled() -> bool:
    return bool(getattr(settings, "AGRO_CREDITO_SCORE_SHADOW_ENABLED", False))


def _usernames_autorizados() -> frozenset[str]:
    raw = (getattr(settings, "AGRO_CREDITO_SCORE_SHADOW_USERNAMES", "") or "").strip()
    if not raw:
        return frozenset()
    return frozenset(u.strip().lower() for u in raw.split(",") if u.strip())


def usuario_pode_acessar_credito_score_shadow(user) -> bool:
    """Superuser ou username na allowlist. Staff sozinho NÃO entra."""
    if not user or not getattr(user, "is_authenticated", False):
        return False
    if not credito_score_shadow_enabled():
        return False
    if getattr(user, "is_superuser", False):
        return True
    uname = (user.get_username() if hasattr(user, "get_username") else "") or ""
    return uname.strip().lower() in _usernames_autorizados()


def credito_score_shadow_required(view_func):
    """Login + flag + allowlist. Demais usuários: 404 (sem menu)."""

    @login_required(login_url="/entrar/")
    @wraps(view_func)
    def wrapper(request, *args, **kwargs):
        if not usuario_pode_acessar_credito_score_shadow(request.user):
            raise Http404()
        return view_func(request, *args, **kwargs)

    return wrapper
