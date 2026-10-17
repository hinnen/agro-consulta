"""Ajuste Mobile dentro do SistApp — sessão do hub GM Lojas (qualquer PIN válido)."""

from __future__ import annotations

from django.http import HttpRequest

from base.models import PerfilUsuario
from produtos.vendas_lojas_hub_sessao_util import (
    SESSION_USER_ID,
    operador_da_sessao,
    pin_da_sessao,
)


def sessao_ajuste_mobile_ok(request: HttpRequest) -> bool:
    if operador_da_sessao(request):
        return True
    return bool(
        str(request.session.get("ajuste_mobile_operador") or "").strip()
        or request.session.get("ajuste_mobile_user_id")
    )


def sincronizar_ajuste_mobile_desde_hub(request: HttpRequest) -> bool:
    """Copia operador do hub para chaves legadas do Ajuste Mobile (APIs / SESSAO)."""
    nome = operador_da_sessao(request)
    if not nome:
        return False
    request.session["ajuste_mobile_operador"] = nome[:120]
    uid = request.session.get(SESSION_USER_ID)
    if uid:
        request.session["ajuste_mobile_user_id"] = int(uid)
    else:
        pin = pin_da_sessao(request)
        if pin:
            perfil = (
                PerfilUsuario.objects.select_related("user")
                .filter(senha_rapida=pin, ativo=True)
                .first()
            )
            if perfil and perfil.user_id:
                request.session["ajuste_mobile_user_id"] = int(perfil.user_id)
    request.session.modified = True
    return True


def operador_ajuste_rotulo(request: HttpRequest) -> str:
    if operador_da_sessao(request):
        sincronizar_ajuste_mobile_desde_hub(request)
        return operador_da_sessao(request)
    return str(request.session.get("ajuste_mobile_operador") or "").strip()


def user_id_ajuste_mobile(request: HttpRequest):
    if operador_da_sessao(request):
        sincronizar_ajuste_mobile_desde_hub(request)
    return request.session.get("ajuste_mobile_user_id")
