"""Plano «Extravio após Depósito» — mesma natureza DRE de retirada de sócio.

Não altera lucro operacional / líquido; só o Saldo final (caixa gerencial).
"""
from __future__ import annotations

import unicodedata

PLANO_EXTRAVIO_APOS_DEPOSITO = "Extravio após Depósito"


def _fold(s: str) -> str:
    t = unicodedata.normalize("NFKD", (s or "").strip())
    t = "".join(c for c in t if not unicodedata.combining(c))
    return " ".join(t.casefold().split())


def plano_eh_extravio_apos_deposito(nome_plano: str) -> bool:
    f = _fold(nome_plano)
    return "extravio" in f and "deposito" in f


def forma_eh_dinheiro(nome_forma: str) -> bool:
    f = _fold(nome_forma)
    if not f:
        return False
    if f in ("dinheiro", "especie", "espécie", "cash"):
        return True
    # Ex.: «Dinheiro · Caixa 1»
    return "dinheiro" in f


def forma_eh_banco(nome_forma: str) -> bool:
    """Forma de pagamento «BANCO» (baixa CP) — não confundir com conta/banco destino."""
    f = _fold(nome_forma)
    if not f:
        return False
    if "adicionar" in f:
        return False
    if f == "banco":
        return True
    parts = f.split()
    return parts[0] == "banco" or ("banco" in parts and len(parts) <= 2)


def forma_permitida_baixa_cp(nome_forma: str) -> bool:
    """Contas a pagar: só Dinheiro ou Banco."""
    return forma_eh_dinheiro(nome_forma) or forma_eh_banco(nome_forma)


def filtrar_formas_baixa_cp(formas: list) -> list:
    """Mantém só opções Dinheiro / Banco na lista da baixa CP."""
    out = []
    for x in formas or []:
        if not isinstance(x, dict):
            continue
        nome = str(x.get("nome") or "")
        if forma_permitida_baixa_cp(nome):
            out.append(x)
    return out


def garantir_plano_extravio_apos_deposito() -> tuple[object | None, bool]:
    """Cria o plano oficial se faltar. Retorna (obj, created)."""
    from produtos.models import PlanoContaAgro

    obj, created = PlanoContaAgro.objects.get_or_create(
        nome=PLANO_EXTRAVIO_APOS_DEPOSITO,
        defaults={
            "tipo": "outra",
            "grupo": "Sócio",
            "observacao": (
                "Dinheiro saiu no depósito e não foi para boleto/banco — "
                "não reduz lucro operacional; mesma regra da retirada de sócio."
            ),
            "ativo": True,
            "exibir_pdv": True,
        },
    )
    if not created:
        dirty = False
        if (obj.tipo or "").strip().casefold() != "outra":
            obj.tipo = "outra"
            dirty = True
        if "socio" not in _fold(obj.grupo or ""):
            obj.grupo = "Sócio"
            dirty = True
        if not obj.ativo:
            obj.ativo = True
            dirty = True
        if not obj.exibir_pdv:
            obj.exibir_pdv = True
            dirty = True
        if dirty:
            obj.save(
                update_fields=["tipo", "grupo", "ativo", "exibir_pdv", "atualizado_em"]
            )
    try:
        from financeiro.services.plano_conta_dre_util import invalidar_cache_cadastro_dre

        invalidar_cache_cadastro_dre()
    except Exception:
        pass
    return obj, created
