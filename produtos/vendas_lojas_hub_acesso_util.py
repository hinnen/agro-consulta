"""Quem pode abrir Vendas e Tarefas no hub GM Lojas (celular)."""

from __future__ import annotations

import os

from django.conf import settings

# Slots de operador (codigo_vendedor 0001→1 … 0008→8) com acesso Vendas/Tarefas no celular.
# Também aceita senha_rapida literal na lista (ex.: quem usa PIN curto «1»).
_DEFAULT_PINS = frozenset({"1", "2", "3", "4", "8"})

_MSG_SEM_ACESSO = (
    "Este PIN não tem acesso a Vendas nem Tarefas no celular. "
    "Use Fotos ou Etiquetas, ou fale com a gerência."
)


def pins_vendas_tarefas_hub() -> frozenset[str]:
    raw = (
        getattr(settings, "AGRO_VL_HUB_PINS_VENDAS_TAREFAS", None)
        or os.environ.get("AGRO_VL_HUB_PINS_VENDAS_TAREFAS")
        or ""
    )
    raw = str(raw).strip()
    if not raw:
        return _DEFAULT_PINS
    out = {p.strip() for p in raw.split(",") if p.strip()}
    return frozenset(out) if out else _DEFAULT_PINS


def _slot_operador_por_senha_rapida(pin: str) -> str:
    """Número do operador (ex. 0001 → «1») para o PIN digitado no hub."""
    from base.models import PerfilUsuario

    pin = (pin or "").strip()
    if not pin:
        return ""
    perfil = (
        PerfilUsuario.objects.filter(senha_rapida=pin, ativo=True)
        .only("codigo_vendedor")
        .first()
    )
    if not perfil:
        return ""
    cv = (perfil.codigo_vendedor or "").strip()
    if cv.isdigit():
        return str(int(cv))
    stripped = cv.lstrip("0")
    return stripped or cv


def pin_liberado_vendas_tarefas(pin: str) -> bool:
    pin = (pin or "").strip()
    if not pin:
        return False
    allowed = pins_vendas_tarefas_hub()
    if pin in allowed:
        return True
    slot = _slot_operador_por_senha_rapida(pin)
    return bool(slot and slot in allowed)


def mensagem_pin_sem_acesso_vendas_tarefas() -> str:
    return _MSG_SEM_ACESSO


def mensagem_sem_acesso_modulo(modulo: str) -> str:
    nome = "Vendas" if (modulo or "").strip().lower() == "vendas" else "Tarefas"
    return (
        f"Seu PIN não tem permissão para abrir {nome} no celular. "
        "Você pode usar Fotos e Etiquetas. Para Vendas ou Tarefas, use um PIN autorizado "
        "(gerência) ou fale com o responsável."
    )
