"""Quem pode abrir Vendas e Tarefas no hub GM Lojas (celular)."""

from __future__ import annotations

import os

from django.conf import settings

# Renan #1, Geraldinho #2, Geraldo #3, Isabela #4, Zuleide #8 (senha_rapida no cadastro).
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


def pin_liberado_vendas_tarefas(pin: str) -> bool:
    return (pin or "").strip() in pins_vendas_tarefas_hub()


def mensagem_pin_sem_acesso_vendas_tarefas() -> str:
    return _MSG_SEM_ACESSO


def mensagem_sem_acesso_modulo(modulo: str) -> str:
    nome = "Vendas" if (modulo or "").strip().lower() == "vendas" else "Tarefas"
    return (
        f"Seu PIN não tem permissão para abrir {nome} no celular. "
        "Você pode usar Fotos e Etiquetas. Para Vendas ou Tarefas, use um PIN autorizado "
        "(gerência) ou fale com o responsável."
    )
