"""Telefone/WhatsApp único por ClienteAgro (identificador do cliente no PDV)."""
from __future__ import annotations

import re

from django.core.exceptions import ValidationError


def extrair_whatsapp_digits(raw: str | None) -> str:
    return re.sub(r"\D", "", str(raw or "").strip())[:20]


def validar_whatsapp_obrigatorio(raw: str | None) -> tuple[str, str | None]:
    """Retorna (dígitos, mensagem_erro)."""
    digits = extrair_whatsapp_digits(raw)
    if len(digits) < 10:
        return "", "Informe o telefone ou WhatsApp com DDD (mínimo 10 dígitos)."
    return digits, None


def cliente_agro_por_whatsapp(digits: str, *, excluir_pk=None):
    """Cliente ativo com o mesmo telefone (compara só dígitos)."""
    from produtos.models import ClienteAgro

    digits = extrair_whatsapp_digits(digits)
    if len(digits) < 10:
        return None
    qs = ClienteAgro.objects.filter(ativo=True).only("pk", "nome", "whatsapp")
    if excluir_pk is not None:
        qs = qs.exclude(pk=excluir_pk)
    for cli in qs:
        if extrair_whatsapp_digits(cli.whatsapp) == digits:
            return cli
    return None


def mensagem_whatsapp_duplicado(cliente) -> str:
    nome = (getattr(cliente, "nome", None) or "outro cliente").strip()
    return (
        f'Este telefone já está cadastrado para "{nome}". '
        "Abra o outro cadastro ou limpe o número dali."
    )


def info_whatsapp_duplicado(digits: str, *, excluir_pk=None) -> dict | None:
    dup = cliente_agro_por_whatsapp(digits, excluir_pk=excluir_pk)
    if not dup:
        return None
    nome = (dup.nome or "outro cliente").strip()
    return {
        "pk": dup.pk,
        "nome": nome,
        "whatsapp": extrair_whatsapp_digits(dup.whatsapp),
        "erro": mensagem_whatsapp_duplicado(dup),
    }


def erro_whatsapp_duplicado(digits: str, *, excluir_pk=None) -> str | None:
    info = info_whatsapp_duplicado(digits, excluir_pk=excluir_pk)
    if info:
        return info.get("erro")
    return None


def validar_whatsapp_unico_cliente(
    raw: str | None, *, excluir_pk=None, obrigatorio: bool = False
) -> tuple[str, str | None]:
    """Normaliza, exige mínimo (se obrigatório) e garante unicidade."""
    if obrigatorio:
        digits, err = validar_whatsapp_obrigatorio(raw)
        if err:
            return "", err
    else:
        digits = extrair_whatsapp_digits(raw)
        if digits and len(digits) < 10:
            return "", "Telefone ou WhatsApp inválido (mínimo 10 dígitos)."
    if digits:
        dup_err = erro_whatsapp_duplicado(digits, excluir_pk=excluir_pk)
        if dup_err:
            return "", dup_err
    return digits, None


def validar_whatsapp_modelo(cliente) -> None:
    """Para ClienteAgro.clean() — levanta ValidationError no campo whatsapp."""
    digits = extrair_whatsapp_digits(getattr(cliente, "whatsapp", ""))
    cliente.whatsapp = digits
    if not digits:
        return
    if len(digits) < 10:
        raise ValidationError(
            {"whatsapp": "Informe o telefone ou WhatsApp com DDD (mínimo 10 dígitos)."}
        )
    dup_err = erro_whatsapp_duplicado(digits, excluir_pk=getattr(cliente, "pk", None))
    if dup_err:
        raise ValidationError({"whatsapp": dup_err})
