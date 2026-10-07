#!/usr/bin/env python3
"""Bug #31 — Cashback/Vale minoritário não pode puxar tabela cara (crédito)."""
from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

import django

django.setup()

PASS = 0
FAIL = 0


def check(ok: bool, msg: str) -> None:
    global PASS, FAIL
    if ok:
        PASS += 1
        print(f"  OK  {msg}")
    else:
        FAIL += 1
        print(f" FAIL {msg}")


def main() -> int:
    from produtos.precos_forma_pagamento_util import forma_principal_para_preco

    print("== Bug #31 cashback × tabela forma ==")

    check(
        forma_principal_para_preco(
            "Cashback",
            [
                {"forma": "Dinheiro", "valor": 90},
                {"forma": "Cashback", "valor": 10},
            ],
        )
        == "Dinheiro",
        "90% Dinheiro + 10% Cashback = Dinheiro",
    )
    check(
        forma_principal_para_preco(
            "Cashback + Dinheiro",
            [
                {"formaPagamento": "Cashback", "valorPagamento": 10},
                {"formaPagamento": "Dinheiro", "valorPagamento": 77},
            ],
        )
        == "Dinheiro",
        "payload formaPagamento: pula Cashback = Dinheiro",
    )
    check(
        forma_principal_para_preco(
            "Vale crédito",
            [
                {"forma": "PIX", "valor": 200},
                {"forma": "Vale crédito", "valor": 15},
            ],
        )
        == "PIX",
        "PIX + Vale = PIX",
    )
    check(
        forma_principal_para_preco(
            "Cashback",
            [{"forma": "Cashback", "valor": 50}],
        )
        == "Cashback",
        "so Cashback = Cashback",
    )
    check(
        forma_principal_para_preco(
            "Cashback + Vale crédito",
            [
                {"forma": "Cashback", "valor": 5},
                {"forma": "Vale crédito", "valor": 20},
            ],
        )
        == "Vale crédito",
        "so CB+Vale = maior valor (Vale)",
    )
    check(
        forma_principal_para_preco(
            "",
            [
                {"forma": "Dinheiro", "valor": 40},
                {"forma": "Cartão de crédito", "valor": 60},
            ],
        )
        == "Cartão de crédito",
        "entre mercadoria manda maior valor",
    )
    check(
        forma_principal_para_preco("Cashback + Dinheiro", None) == "Dinheiro",
        "rotulo Cashback + Dinheiro sem lista = Dinheiro",
    )

    js_preco = (ROOT / "produtos/static/produtos/js/precos_forma_pagamento.js").read_text(
        encoding="utf-8"
    )
    check("function formaPrincipalParaPreco" in js_preco, "JS formaPrincipalParaPreco")
    check("FORMAS_SKIP_PRECO" in js_preco, "JS skip Cashback/Vale")
    check("formaPrincipalParaPreco: formaPrincipalParaPreco" in js_preco, "JS exporta API")

    js_state = (ROOT / "produtos/static/produtos/js/pdv_state.js").read_text(encoding="utf-8")
    check("formaPrecoDoState" in js_state, "pdv_state usa formaPrecoDoState")
    check("formaPrincipalParaPreco" in js_state, "pdv_state chama principal")

    js_wiz = (ROOT / "produtos/static/produtos/js/pdv_wizard.js").read_text(encoding="utf-8")
    check("formaPrincipalParaPreco(state)" in js_wiz, "sync antes gravar usa principal")

    print(f"\nResultado: {PASS} ok, {FAIL} fail")
    return 1 if FAIL else 0


if __name__ == "__main__":
    raise SystemExit(main())
