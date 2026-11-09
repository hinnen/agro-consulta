#!/usr/bin/env python3
"""Prova: alias fornecedor Entrada NF (unificação só daqui pra frente)."""
from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

import django

django.setup()

from produtos.fornecedor_entrada_nfe_util import (
    chave_nome_alias_fornecedor,
    estabilizar_ids_itens_fornecedor_entrada_nfe,
    filtrar_itens_fornecedor_entrada_nfe,
    fornecedor_id_unificavel,
    registrar_unificacao_fornecedores_entrada_nfe,
    resolver_fornecedor_canonico_id,
    tokens_busca_unificar_fornecedor,
)
from produtos.models import FornecedorEntradaNfeAliasAgro

CANON = "aaaaaaaaaaaaaaaaaaaaaaaa"
DUP1 = "bbbbbbbbbbbbbbbbbbbbbbbb"
DUP2 = "cccccccccccccccccccccccc"
DUP_LOCAL = "local:42"
NOME_DUP = "47883101000111 - Sn - Rbs"
DUP_NOME = chave_nome_alias_fornecedor(NOME_DUP)


def main() -> int:
    from django.db import connection

    assert fornecedor_id_unificavel(CANON)
    assert fornecedor_id_unificavel(DUP_LOCAL)
    assert fornecedor_id_unificavel(DUP_NOME)
    assert not fornecedor_id_unificavel("")
    assert not fornecedor_id_unificavel("xyz")

    toks = tokens_busca_unificar_fornecedor("Sn - Rbs")
    assert "rbs" in toks, toks
    assert DUP_NOME.startswith("n:") and len(DUP_NOME) <= 32

    if "produtos_fornecedorentradanfealiasagro" not in connection.introspection.table_names():
        print("VERIFY_OK fornecedor_entrada_nfe_alias (sem tabela — só unitário)")
        return 0

    FornecedorEntradaNfeAliasAgro.objects.filter(
        fornecedor_duplicado_id__in=[DUP1, DUP2, DUP_LOCAL, DUP_NOME]
    ).delete()
    itens = [
        {"id": CANON, "nome": "Principal", "documento": "", "origem": "mongo"},
        {"id": DUP1, "nome": "Dup 1", "documento": "", "origem": "mongo"},
        {"id": DUP2, "nome": "Dup 2", "documento": "", "origem": "mongo"},
        {"id": DUP_LOCAL, "nome": "RBS COMERCIO DE CEREAIS LTDA", "documento": "", "origem": "agro"},
        {"id": "", "nome": NOME_DUP, "documento": "", "origem": "titulo_pg"},
    ]
    est = estabilizar_ids_itens_fornecedor_entrada_nfe(itens)
    assert any(x.get("id") == DUP_NOME and x.get("id_sintetico") for x in est), est

    r = registrar_unificacao_fornecedores_entrada_nfe(
        fornecedor_canonico_id=CANON,
        duplicados_ids=[DUP1, DUP2, DUP_LOCAL, DUP_NOME],
        canonico_nome="Principal",
        usuario=None,
    )
    assert r.get("ok"), r
    assert resolver_fornecedor_canonico_id(DUP1) == CANON
    assert resolver_fornecedor_canonico_id(DUP_LOCAL) == CANON
    assert resolver_fornecedor_canonico_id(DUP_NOME) == CANON
    filtrado = filtrar_itens_fornecedor_entrada_nfe(itens)
    ids_visiveis = {str(x.get("id") or "") for x in filtrado}
    nomes = {str(x.get("nome") or "") for x in filtrado}
    assert CANON in ids_visiveis
    assert DUP1 not in ids_visiveis and DUP2 not in ids_visiveis
    assert DUP_LOCAL not in ids_visiveis
    assert NOME_DUP not in nomes
    assert "aviso" in r
    FornecedorEntradaNfeAliasAgro.objects.filter(
        fornecedor_duplicado_id__in=[DUP1, DUP2, DUP_LOCAL, DUP_NOME]
    ).delete()
    print("VERIFY_OK fornecedor_entrada_nfe_alias")
    return 0


if __name__ == "__main__":
    sys.exit(main())
