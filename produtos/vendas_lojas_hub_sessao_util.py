"""Sessão PIN única do PWA GM Lojas (hub) — válida por 7 dias."""

from __future__ import annotations

import time
from functools import wraps
from typing import Any
from urllib.parse import quote

from django.http import HttpRequest, HttpResponse, JsonResponse
from django.shortcuts import redirect
from django.urls import reverse

from base.models import PerfilUsuario
from produtos.caixa_util import rotulo_operador_pin, validar_pin_operador

SESSION_OPERADOR = "gm_lojas_operador_nome"
SESSION_USER_ID = "gm_lojas_operador_user_id"
SESSION_PIN = "gm_lojas_operador_pin"
SESSION_DESDE = "gm_lojas_operador_desde"

PIN_TTL_SEGUNDOS = 7 * 24 * 3600

# Chaves antigas (apps separados) — limpas ao gravar; lidas na migração suave.
_LEGACY = (
    ("fotos_produto_operador_nome", "fotos_produto_operador_user_id", None),
    ("vendas_lojas_operador_nome", "vendas_lojas_operador_user_id", "vendas_lojas_operador_pin"),
    ("tarefas_operador_nome", "tarefas_operador_user_id", "tarefas_operador_pin"),
)


def _agora() -> float:
    return time.time()


def _sessao_expirada(request: HttpRequest) -> bool:
    try:
        desde = request.session.get(SESSION_DESDE)
        if desde is None:
            if request.session.get(SESSION_OPERADOR):
                request.session[SESSION_DESDE] = _agora()
                request.session.set_expiry(PIN_TTL_SEGUNDOS)
                request.session.modified = True
                return False
            return True
        return (_agora() - float(desde)) > PIN_TTL_SEGUNDOS
    except (TypeError, ValueError):
        return True


def _limpar_chaves_sessao(request: HttpRequest) -> None:
    for k in (SESSION_OPERADOR, SESSION_USER_ID, SESSION_PIN, SESSION_DESDE):
        request.session.pop(k, None)
    for nome_k, uid_k, pin_k in _LEGACY:
        request.session.pop(nome_k, None)
        request.session.pop(uid_k, None)
        if pin_k:
            request.session.pop(pin_k, None)
    request.session.modified = True


def _migrar_legado_para_hub(request: HttpRequest) -> bool:
    """Se só existir sessão antiga (ex.: deploy anterior), promove para hub."""
    nome = ""
    pin = ""
    uid = None
    for nome_k, uid_k, pin_k in _LEGACY:
        n = str(request.session.get(nome_k) or "").strip()
        if n:
            nome = n
            uid = request.session.get(uid_k)
            if pin_k:
                pin = str(request.session.get(pin_k) or "").strip()
            break
    if not nome:
        return False
    request.session[SESSION_OPERADOR] = nome[:120]
    if pin:
        request.session[SESSION_PIN] = pin[:32]
    if uid:
        request.session[SESSION_USER_ID] = int(uid)
    request.session[SESSION_DESDE] = _agora()
    request.session.set_expiry(PIN_TTL_SEGUNDOS)
    request.session.modified = True
    return True


def _sessao_hub_ativa(request: HttpRequest) -> bool:
    try:
        if _sessao_expirada(request):
            if request.session.get(SESSION_OPERADOR) or any(
                request.session.get(n[0]) for n in _LEGACY
            ):
                _limpar_chaves_sessao(request)
            return False
        if request.session.get(SESSION_OPERADOR):
            return True
        return _migrar_legado_para_hub(request)
    except Exception:
        return False


def pin_da_sessao(request: HttpRequest) -> str:
    if not _sessao_hub_ativa(request):
        return ""
    return str(request.session.get(SESSION_PIN) or "").strip()


def operador_da_sessao(request: HttpRequest) -> str:
    if not _sessao_hub_ativa(request):
        return ""
    nome = str(request.session.get(SESSION_OPERADOR) or "").strip()
    pin = str(request.session.get(SESSION_PIN) or "").strip()
    if not nome and pin:
        nome = (rotulo_operador_pin(pin) or "").strip()
        if nome:
            request.session[SESSION_OPERADOR] = nome[:120]
            request.session.modified = True
    return nome


def gravar_operador_sessao(request: HttpRequest, pin: str) -> tuple[bool, str, str]:
    pin = (pin or "").strip()
    ok, err = validar_pin_operador(pin)
    if not ok:
        return False, "", err or "PIN incorreto."
    nome = (rotulo_operador_pin(pin) or "").strip()
    if not nome:
        return False, "", "PIN sem nome no cadastro."
    _limpar_chaves_sessao(request)
    request.session[SESSION_OPERADOR] = nome[:120]
    request.session[SESSION_PIN] = pin[:32]
    request.session[SESSION_DESDE] = _agora()
    perfil = (
        PerfilUsuario.objects.select_related("user")
        .filter(senha_rapida=pin, ativo=True)
        .first()
    )
    uid = getattr(getattr(perfil, "user", None), "pk", None) if perfil else None
    if uid:
        request.session[SESSION_USER_ID] = int(uid)
    request.session.set_expiry(PIN_TTL_SEGUNDOS)
    request.session.modified = True
    return True, nome[:120], ""


def limpar_operador_sessao(request: HttpRequest) -> None:
    _limpar_chaves_sessao(request)


def next_url_seguro(request: HttpRequest, fallback: str = "vendas_lojas_hub") -> str:
    raw = str(request.GET.get("next") or request.POST.get("next") or "").strip()
    if raw.startswith("/vendas/lojas/") and "//" not in raw and "://" not in raw:
        return raw
    return reverse(fallback)


def redirect_para_pin(request: HttpRequest) -> HttpResponse:
    nxt = quote(request.get_full_path() or reverse("vendas_lojas_hub"), safe="")
    return redirect(f"{reverse('vendas_lojas_pin')}?next={nxt}")


def exigir_hub_operador_html(view_func):
    @wraps(view_func)
    def _wrap(request: HttpRequest, *args: Any, **kwargs: Any):
        if not operador_da_sessao(request):
            return redirect_para_pin(request)
        return view_func(request, *args, **kwargs)

    return _wrap


def exigir_hub_operador_api(view_func):
    @wraps(view_func)
    def _wrap(request: HttpRequest, *args: Any, **kwargs: Any):
        nome = operador_da_sessao(request)
        if not nome:
            return JsonResponse(
                {"ok": False, "erro": "Informe o PIN para continuar.", "precisa_pin": True},
                status=401,
            )
        request.gm_lojas_operador = nome  # type: ignore[attr-defined]
        return view_func(request, *args, **kwargs)

    return _wrap
