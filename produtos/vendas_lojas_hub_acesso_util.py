"""Quem pode abrir Vendas e Tarefas no hub GM Lojas (celular)."""

from __future__ import annotations

import os
import unicodedata

from django.conf import settings

# Códigos # na tela RH (Operadores do caixa) — alinhado à gerência loja.
_DEFAULT_CODIGOS = frozenset({"1", "2", "3", "4", "8"})

# Nomes liberados (qualquer PIN pessoal deles). Ordem = lista da gerência.
_DEFAULT_NOMES = (
    "Geraldinho",
    "Geraldo Hinnen",
    "Renan Hinnen",
    "Isabela Cugler",
    "Zuleide Hinnen",
)

_MSG_SEM_ACESSO = (
    "Este operador não tem acesso a Vendas nem Tarefas no celular. "
    "Use Fotos ou Etiquetas, ou fale com a gerência."
)


def _norm_nome(s: str) -> str:
    raw = (s or "").strip().lower()
    nfkd = unicodedata.normalize("NFKD", raw)
    sem_acento = "".join(c for c in nfkd if not unicodedata.combining(c))
    return " ".join(sem_acento.split())


def pins_vendas_tarefas_hub() -> frozenset[str]:
    """Códigos de operador (#1, #2…) permitidos."""
    raw = (
        getattr(settings, "AGRO_VL_HUB_PINS_VENDAS_TAREFAS", None)
        or os.environ.get("AGRO_VL_HUB_PINS_VENDAS_TAREFAS")
        or ""
    )
    raw = str(raw).strip()
    if not raw:
        return _DEFAULT_CODIGOS
    out = {p.strip() for p in raw.split(",") if p.strip()}
    return frozenset(out) if out else _DEFAULT_CODIGOS


def nomes_vendas_tarefas_hub() -> frozenset[str]:
    """Nomes (como no cadastro de operadores) permitidos — comparação sem acento."""
    raw = (
        getattr(settings, "AGRO_VL_HUB_NOMES_VENDAS_TAREFAS", None)
        or os.environ.get("AGRO_VL_HUB_NOMES_VENDAS_TAREFAS")
        or ""
    )
    raw = str(raw).strip()
    if not raw:
        return frozenset(_norm_nome(n) for n in _DEFAULT_NOMES)
    out = {_norm_nome(p) for p in raw.split(",") if p.strip()}
    return out if out else frozenset(_norm_nome(n) for n in _DEFAULT_NOMES)


def _perfil_por_pin(pin: str):
    from base.models import PerfilUsuario

    pin = (pin or "").strip()
    if not pin:
        return None
    return (
        PerfilUsuario.objects.filter(senha_rapida=pin, ativo=True)
        .select_related("user", "funcionario")
        .first()
    )


def codigo_operador_por_pin_digitado(pin: str) -> str:
    """codigo_vendedor do operador que digitou o PIN (mesmo # da tela RH)."""
    perfil = _perfil_por_pin(pin)
    if not perfil:
        return ""
    return (perfil.codigo_vendedor or "").strip()


_slot_operador_por_senha_rapida = codigo_operador_por_pin_digitado


def _nome_exibicao_por_pin(pin: str) -> str:
    from base.operador_util import nome_exibicao_perfil

    perfil = _perfil_por_pin(pin)
    if not perfil:
        return ""
    return nome_exibicao_perfil(perfil)


def operador_liberado_vendas_tarefas(pin: str) -> bool:
    """Libera por código # ou por nome no cadastro — não pelo valor do PIN pessoal."""
    pin = (pin or "").strip()
    if not pin:
        return False
    perfil = _perfil_por_pin(pin)
    if not perfil:
        return False
    codigo = (perfil.codigo_vendedor or "").strip()
    if codigo and codigo in pins_vendas_tarefas_hub():
        return True
    from base.operador_util import nome_exibicao_perfil

    nome = _norm_nome(nome_exibicao_perfil(perfil))
    if not nome:
        return False
    liberados = nomes_vendas_tarefas_hub()
    if nome in liberados:
        return True
    # «Geraldinho» no cadastro vs nome RH mais longo
    primeiro = nome.split(" ", 1)[0]
    return primeiro in liberados


def pin_liberado_vendas_tarefas(pin: str) -> bool:
    return operador_liberado_vendas_tarefas(pin)


def mensagem_pin_sem_acesso_vendas_tarefas() -> str:
    return _MSG_SEM_ACESSO


def mensagem_sem_acesso_modulo(modulo: str) -> str:
    nome = "Vendas" if (modulo or "").strip().lower() == "vendas" else "Tarefas"
    return (
        f"Seu operador não está liberado para {nome} no celular "
        "(acesso só para a gerência configurada). "
        "Você pode usar Fotos e Etiquetas. Fale com o responsável se precisar de acesso."
    )
