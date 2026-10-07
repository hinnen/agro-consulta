#!/usr/bin/env python
"""Prova path ContaBancariaLojaAgro (CP — gestão contas Postgres)."""
from __future__ import annotations

import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

import django

django.setup()

from produtos.conta_bancaria_loja_util import (
    criar_conta_loja,
    listar_bancos_para_baixa,
    listar_contas_loja,
    renomear_conta_loja,
    set_ativo_conta_loja,
)
from produtos.models import ContaBancariaLojaAgro

FAILS = 0


def ok(cond: bool, msg: str) -> None:
    global FAILS
    if cond:
        print(f"  OK  {msg}")
    else:
        FAILS += 1
        print(f" FAIL {msg}")


def main() -> int:
    print("== ContaBancariaLojaAgro path ==")
    tag = f"__TESTE_CONTA_LOJA_{os.getpid()}__"
    ContaBancariaLojaAgro.objects.filter(nome__startswith="__TESTE_CONTA_LOJA_").delete()

    obj = criar_conta_loja(tag)
    ok(obj.pk > 0, "criar gera pk")
    ok(bool(obj.codigo) and obj.codigo.startswith("loja-"), f"codigo estável ({obj.codigo})")
    ok(obj.ativo is True, "nova conta ativa")

    bancos = listar_bancos_para_baixa()
    ids = {str(b.get("id") or "") for b in bancos}
    nomes = {str(b.get("nome") or "") for b in bancos}
    ok(obj.codigo in ids, "aparece na lista da baixa")
    ok(tag in nomes, "nome na lista da baixa")
    ok(any("ADICIONAR CONTA" in str(b.get("nome") or "").upper() for b in bancos), "placeholder no início/lista")

    novo = tag + "_REN"
    obj2 = renomear_conta_loja(obj.pk, novo)
    ok(obj2.nome == novo, "renomear")

    set_ativo_conta_loja(obj.pk, False)
    bancos2 = listar_bancos_para_baixa()
    ok(obj.codigo not in {str(b.get("id") or "") for b in bancos2}, "desativada some da baixa")
    gestao = listar_contas_loja(incluir_inativos=True)
    hit = next((x for x in gestao if x["pk"] == obj.pk), None)
    ok(hit is not None and hit["ativo"] is False, "ainda aparece na gestão (inativa)")

    set_ativo_conta_loja(obj.pk, True)
    ok(obj.codigo in {str(b.get("id") or "") for b in listar_bancos_para_baixa()}, "reativar volta")

    ContaBancariaLojaAgro.objects.filter(pk=obj.pk).delete()
    print(f"\nFAILS={FAILS}")
    return 1 if FAILS else 0


if __name__ == "__main__":
    raise SystemExit(main())
