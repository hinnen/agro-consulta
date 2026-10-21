#!/usr/bin/env python3
"""Prova: alias fornecedor Entrada NF (unificação só daqui pra frente)."""
from __future__ import annotations

import os
import sys

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

import django

django.setup()

from produtos.fornecedor_entrada_nfe_util import (
    filtrar_itens_fornecedor_entrada_nfe,
    registrar_unificacao_fornecedores_entrada_nfe,
    resolver_fornecedor_canonico_id,
)
from produtos.models import FornecedorEntradaNfeAliasAgro

CANON = "aaaaaaaaaaaaaaaaaaaaaaaa"
DUP1 = "bbbbbbbbbbbbbbbbbbbbbbbb"
DUP2 = "cccccccccccccccccccccccc"


def main() -> int:
    FornecedorEntradaNfeAliasAgro.objects.filter(
        fornecedor_duplicado_id__in=[DUP1, DUP2]
    ).delete()
    itens = [
        {"id": CANON, "nome": "Principal", "documento": "", "origem": "mongo"},
        {"id": DUP1, "nome": "Dup 1", "documento": "", "origem": "mongo"},
        {"id": DUP2, "nome": "Dup 2", "documento": "", "origem": "mongo"},
    ]
    r = registrar_unificacao_fornecedores_entrada_nfe(
        fornecedor_canonico_id=CANON,
        duplicados_ids=[DUP1, DUP2],
        canonico_nome="Principal",
        usuario=None,
    )
    assert r.get("ok"), r
    assert resolver_fornecedor_canonico_id(DUP1) == CANON
    filtrado = filtrar_itens_fornecedor_entrada_nfe(itens)
    ids = {x["id"] for x in filtrado}
    assert CANON in ids
    assert DUP1 not in ids and DUP2 not in ids
    assert "aviso" in r
    FornecedorEntradaNfeAliasAgro.objects.filter(fornecedor_duplicado_id__in=[DUP1, DUP2]).delete()
    print("VERIFY_OK fornecedor_entrada_nfe_alias")
    return 0


if __name__ == "__main__":
    sys.exit(main())
