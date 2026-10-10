#!/usr/bin/env python3
"""Bug #24 — milho grande: Dinheiro não pode gravar preço de lista (crédito)."""
from __future__ import annotations

import os
import sys
from pathlib import Path
from types import SimpleNamespace

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
    from produtos.precos_forma_pagamento_util import (
        corrigir_precos_itens_lista_sem_forma,
        forma_principal_para_preco,
        preco_venda_para_forma,
    )

    print("== Bug #24 preço forma Dinheiro ==")

    g = {
        "preco_a": 87.0,
        "preco_b": 92.0,
        "formas_a": ["Dinheiro", "PIX", "Cartão de débito"],
        "formas_b": [],  # como no milho grande da loja
    }
    din = preco_venda_para_forma(92, None, "Dinheiro", precos_modo="grupos", precos_grupos=g)
    cred = preco_venda_para_forma(92, None, "Cartão de crédito", precos_modo="grupos", precos_grupos=g)
    check(abs(din - 87.0) < 0.01, f"grupos Dinheiro → 87 (got {din})")
    check(abs(cred - 92.0) < 0.01, f"grupos crédito → 92 via B efetivo (got {cred})")

    check(
        forma_principal_para_preco("Dinheiro", [{"forma": "Dinheiro", "valor": 174}]) == "Dinheiro",
        "forma principal Dinheiro",
    )
    check(
        forma_principal_para_preco(
            "Cashback + Dinheiro",
            [{"forma": "Cashback", "valor": 10}, {"forma": "Dinheiro", "valor": 77}],
        )
        == "Dinheiro",
        "pula Cashback → Dinheiro",
    )

    ov = SimpleNamespace(
        produto_externo_id="699382cb4f504f3b9b794a26",
        preco_venda=92.0,
        cadastro_extras={
            "precos_modo": "grupos",
            "precos_grupos": g,
            "precos_por_forma": {"Dinheiro": 82.9, "Cartão de crédito": 92.0},
        },
    )
    itens = [{"id": ov.produto_externo_id, "nome": "milho grande 47 kg", "qtd": 2, "preco": 92.0}]
    n = corrigir_precos_itens_lista_sem_forma(
        itens, "Dinheiro", overlays_by_pid={ov.produto_externo_id: ov}
    )
    check(n == 1, f"corrigiu 1 item (n={n})")
    check(abs(float(itens[0]["preco"]) - 87.0) < 0.01, f"unitário ficou 87 (got {itens[0]['preco']})")

    itens2 = [{"id": ov.produto_externo_id, "preco": 87.0}]
    n2 = corrigir_precos_itens_lista_sem_forma(
        itens2, "Dinheiro", overlays_by_pid={ov.produto_externo_id: ov}
    )
    check(n2 == 0, "já em 87 — não mexe")

    itens3 = [{"id": ov.produto_externo_id, "preco": 92.0, "preco_manual": True}]
    n3 = corrigir_precos_itens_lista_sem_forma(
        itens3, "Dinheiro", overlays_by_pid={ov.produto_externo_id: ov}
    )
    check(n3 == 0 and abs(float(itens3[0]["preco"]) - 92) < 0.01, "preco_manual respeitado")

    itens4 = [{"id": ov.produto_externo_id, "preco": 80.0}]  # promo/digitado ≠ lista
    n4 = corrigir_precos_itens_lista_sem_forma(
        itens4, "Dinheiro", overlays_by_pid={ov.produto_externo_id: ov}
    )
    check(n4 == 0 and abs(float(itens4[0]["preco"]) - 80) < 0.01, "promo/digitado ≠ lista — não mexe")

    js = (ROOT / "produtos/static/produtos/js/pdv_wizard.js").read_text(encoding="utf-8")
    check("sincronizarPrecosFormaAntesGravar" in js, "PDV sync antes de gravar")
    check("sincronizarPrecosFormaAntesGravar(state)" in js, "buildErpPayload/cupom chama sync")
    check("formasGrupoBEfetivas" in (ROOT / "produtos/static/produtos/js/precos_forma_pagamento.js").read_text(encoding="utf-8"), "JS formas B efetivas")

    cat = (ROOT / "produtos/catalogo_agro.py").read_text(encoding="utf-8")
    check("if modo == \"grupos\" and not pg:" in cat, "slim não manda grupos órfão")

    views = (ROOT / "produtos/views.py").read_text(encoding="utf-8")
    check("corrigir_precos_itens_lista_sem_forma" in views, "views chama correção no save")
    check("pdv_catalogo_slim_v6" in views, "cache slim v6")

    print(f"\nResultado: {PASS} ok, {FAIL} fail")
    return 1 if FAIL else 0


if __name__ == "__main__":
    raise SystemExit(main())
