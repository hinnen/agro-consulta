"""Padrão «Permitir venda com estoque negativo» no cadastro Agro / espelho Mongo."""

from __future__ import annotations

PERMITE_VENDA_ESTOQUE_NEGATIVO_MONGO_KEYS: tuple[str, ...] = (
    "PermitirEstoqueNegativo",
    "VendaComEstoqueNegativo",
    "PermiteVendaSemEstoque",
    "EstoqueNegativo",
    "PermiteVendaEstoqueNegativo",
)

CADASTRO_EXTRA_PERMITE_VENDA_NEGATIVO = "permite_venda_estoque_negativo"


def mongo_permite_venda_estoque_negativo_de_doc(p: dict | None) -> bool:
    """Lê o espelho ERP; ausência de campo = permitido (padrão Agro)."""
    if not p:
        return True
    for k in PERMITE_VENDA_ESTOQUE_NEGATIVO_MONGO_KEYS:
        if k not in p:
            continue
        v = p.get(k)
        if isinstance(v, bool):
            return v
        if v in (1, "1", "true", "True", "SIM", "Sim", "S", "s"):
            return True
        if v in (0, "0", "false", "False", "NAO", "Não", "N", "n"):
            return False
    return True


def mongo_set_permite_venda_negativo_payload(valor: bool = True) -> dict:
    """Campos a gravar no ``$set`` do Mongo (todos os aliases conhecidos)."""
    return {k: bool(valor) for k in PERMITE_VENDA_ESTOQUE_NEGATIVO_MONGO_KEYS}
