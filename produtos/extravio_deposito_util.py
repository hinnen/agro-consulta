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


FORMA_CP_DINHEIRO = "DINHEIRO"
FORMA_CP_BANCO = "BANCO"


def forma_eh_banco(nome_forma: str) -> bool:
    """Forma «BANCO» / «DEPÓSITO» na baixa CP — não confundir com conta destino."""
    f = _fold(nome_forma)
    if not f:
        return False
    if "adicionar" in f:
        return False
    if f in ("banco", "deposito"):
        return True
    parts = f.split()
    if parts[0] == "banco" or ("banco" in parts and len(parts) <= 2):
        return True
    # «Depósito» / «DEPOSITO» (sem misturar com plano Extravio após Depósito)
    if "deposito" in parts and "extravio" not in f and len(parts) <= 2:
        return True
    return False


def forma_permitida_baixa_cp(nome_forma: str) -> bool:
    """Contas a pagar: só Dinheiro ou Banco/Depósito."""
    return forma_eh_dinheiro(nome_forma) or forma_eh_banco(nome_forma)


def _score_forma_dinheiro(nome: str) -> int:
    f = _fold(nome)
    if not f or "dinheiro" not in f:
        return 0
    if f == "dinheiro":
        return 100
    if f.startswith("dinheiro "):
        return 80
    if "vista" in f:
        return 10
    return 40


def _score_forma_banco(nome: str) -> int:
    f = _fold(nome)
    if not f or "adicionar" in f:
        return 0
    if f == "banco":
        return 100
    if f == "deposito":
        return 90
    parts = f.split()
    if parts and parts[0] == "banco":
        return 70
    if "banco" in parts and len(parts) <= 2:
        return 50
    if "deposito" in parts and "extravio" not in f and len(parts) <= 2:
        return 40
    return 0


def filtrar_formas_baixa_cp(formas: list) -> list:
    """Lista canônica da baixa CP: só DINHEIRO e BANCO (injeta se faltar no ERP)."""
    best_d: dict | None = None
    best_d_score = 0
    best_b: dict | None = None
    best_b_score = 0
    for x in formas or []:
        if not isinstance(x, dict):
            continue
        nome = str(x.get("nome") or "")
        sd = _score_forma_dinheiro(nome)
        if sd > best_d_score:
            best_d_score = sd
            best_d = x
        sb = _score_forma_banco(nome)
        if sb > best_b_score:
            best_b_score = sb
            best_b = x

    out: list[dict] = []
    if best_d and best_d_score > 0:
        row = dict(best_d)
        row["nome"] = FORMA_CP_DINHEIRO
        out.append(row)
    else:
        out.append({"id": "", "nome": FORMA_CP_DINHEIRO})
    if best_b and best_b_score > 0:
        row = dict(best_b)
        row["nome"] = FORMA_CP_BANCO
        out.append(row)
    else:
        out.append({"id": "", "nome": FORMA_CP_BANCO})
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
