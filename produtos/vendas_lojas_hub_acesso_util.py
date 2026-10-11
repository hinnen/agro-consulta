"""Quem pode abrir Vendas e Tarefas no hub GM Lojas (celular)."""

from __future__ import annotations

import os

from django.conf import settings

# Códigos de operador (RH → Operadores do caixa, ex. «Renan #1» = codigo_vendedor «1»).
# Quem está na lista entra em Vendas/Tarefas com o PIN pessoal dele (senha_rapida), sem colocar o PIN na env.
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


def codigo_operador_por_pin_digitado(pin: str) -> str:
    """codigo_vendedor do operador que digitou o PIN (mesmo # da tela RH)."""
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
    return (perfil.codigo_vendedor or "").strip()


# alias interno (testes / scripts)
_slot_operador_por_senha_rapida = codigo_operador_por_pin_digitado


def pin_liberado_vendas_tarefas(pin: str) -> bool:
    """Libera pelo código do operador (#1, #2…), não pelo valor do PIN pessoal."""
    pin = (pin or "").strip()
    if not pin:
        return False
    codigo = codigo_operador_por_pin_digitado(pin)
    if not codigo:
        return False
    return codigo in pins_vendas_tarefas_hub()


def mensagem_pin_sem_acesso_vendas_tarefas() -> str:
    return _MSG_SEM_ACESSO


def mensagem_sem_acesso_modulo(modulo: str) -> str:
    nome = "Vendas" if (modulo or "").strip().lower() == "vendas" else "Tarefas"
    return (
        f"Seu operador não está liberado para {nome} no celular "
        "(só códigos configurados na gerência, ex. #1, #2…). "
        "Você pode usar Fotos e Etiquetas. Fale com o responsável se precisar de acesso."
    )
