"""
PIN gerencial permanente — GM Agro.

Usuários autorizados a forçar ações sensíveis (ex.: liberar venda com Point órfão):
Geraldo, Geraldinho e Renan Hinnen.

Reutilizar ``validar_pin_gerencial`` / ``PIN_GERENCIAL_NOMES_UI`` em qualquer tela nova
que precise de «forçar com PIN de gerência».
"""

from __future__ import annotations

import re
import time
from typing import Any

from django.contrib.auth.models import User

SESSION_MP_POINT_FORCAR_KEY = "agro_mp_point_forcar_bypass_v1"
PIN_GERENCIAL_BYPASS_TTL_S = 30 * 60  # 30 min

# Texto fixo para overlays (ordem pedida pelo Renan).
PIN_GERENCIAL_NOMES_UI = "Geraldo, Geraldinho ou Renan Hinnen"

PIN_GERENCIAL_HINT = (
    "Para forçar esta ação, peça o PIN de um usuário gerencial: "
    f"{PIN_GERENCIAL_NOMES_UI}."
)


def _norm(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip().lower())


def rotulo_gerencial_do_user(user: User | None) -> str | None:
    """
    Se o User for um dos três gerenciais, devolve o rótulo canônico.
    Caso contrário None.
    """
    if user is None:
        return None
    un = _norm(getattr(user, "username", "") or "")
    fn = _norm(getattr(user, "first_name", "") or "")
    ln = _norm(getattr(user, "last_name", "") or "")
    full = _norm(f"{fn} {ln}")

    # Geraldinho antes de Geraldo (evita «geraldo» dentro de «geraldinho»).
    if un == "geraldinho" or fn == "geraldinho" or "geraldinho" in full:
        return "Geraldinho"
    if un == "geraldo" or fn == "geraldo" or (full.startswith("geraldo") and "geraldinho" not in full):
        return "Geraldo"
    if ("renan" in full and "hinnen" in full) or (fn == "renan" and "hinnen" in ln):
        return "Renan Hinnen"
    return None


def is_usuario_gerencial(user: User | None) -> bool:
    return rotulo_gerencial_do_user(user) is not None


def validar_pin_gerencial(pin: str) -> tuple[bool, str, str]:
    """
    Valida PIN (``PerfilUsuario.senha_rapida``) e exige que o dono seja gerencial.
    Retorna ``(ok, rotulo_ou_vazio, erro)``.
    """
    from produtos.caixa_util import _perfil_usuario_por_pin

    pin = (pin or "").strip()
    if not pin:
        return False, "", "Informe o PIN gerencial."
    if pin == "1234":
        return False, "", "Senha padrão (1234) bloqueada. Use o PIN do gerente."
    perfil = _perfil_usuario_por_pin(pin)
    if perfil is None:
        return False, "", "PIN incorreto."
    rotulo = rotulo_gerencial_do_user(perfil.user)
    if not rotulo:
        return (
            False,
            "",
            f"Este PIN não é gerencial. {PIN_GERENCIAL_HINT}",
        )
    return True, rotulo, ""


def mp_point_forcar_bypass_ativo(request) -> bool:
    raw = None
    try:
        raw = request.session.get(SESSION_MP_POINT_FORCAR_KEY)
    except Exception:
        return False
    if not isinstance(raw, dict):
        return False
    try:
        exp = float(raw.get("exp") or 0)
    except (TypeError, ValueError):
        exp = 0
    if exp < time.time():
        limpar_mp_point_forcar_bypass(request)
        return False
    return True


def gravar_mp_point_forcar_bypass(request, *, por: str, order_ids: list[str] | None = None) -> None:
    try:
        request.session[SESSION_MP_POINT_FORCAR_KEY] = {
            "exp": time.time() + PIN_GERENCIAL_BYPASS_TTL_S,
            "por": (por or "")[:80],
            "order_ids": [str(x) for x in (order_ids or [])][:20],
        }
        request.session.modified = True
    except Exception:
        pass


def limpar_mp_point_forcar_bypass(request) -> None:
    try:
        if SESSION_MP_POINT_FORCAR_KEY in request.session:
            del request.session[SESSION_MP_POINT_FORCAR_KEY]
            request.session.modified = True
    except Exception:
        pass


def payload_hint_pin_gerencial() -> dict[str, Any]:
    return {
        "pode_forcar_pin_gerencial": True,
        "pin_gerencial_nomes": PIN_GERENCIAL_NOMES_UI,
        "pin_gerencial_hint": PIN_GERENCIAL_HINT,
    }
