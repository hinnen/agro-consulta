# -*- coding: utf-8 -*-
"""Prova path — Bug #20 / #24: tabela A/B na notinha (dinheiro ≠ preço crédito).

Caso loja (milho 47kg): A=87 (Dinheiro/PIX/Débito), B=92, formas_b vazio.
Antes: Dinheiro/Fiado caiam no padrao 92; notinha saía com preço de crédito.
Depois: B vazio herda o resto das formas; Dinheiro=87 · Fiado=92.

  python scripts/verify_bug20_tabela_preco_print_path.py
"""
from __future__ import annotations

import os
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

import django

django.setup()

from produtos.precos_forma_pagamento_util import (
    formas_b_efetivas,
    normalizar_precos_grupos_payload,
    preco_venda_para_forma,
)

ok = 0
fail = 0


def check(name: str, cond: bool, detail: str = "") -> None:
    global ok, fail
    msg = name + ((" -- " + detail) if detail else "")
    safe = msg.encode("ascii", "replace").decode("ascii")
    if cond:
        ok += 1
        print("  OK ", safe)
    else:
        fail += 1
        print(" FAIL", safe)


def main() -> int:
    print("=== Bug #20 tabela preco impressao / notinha ===")

    g_vazio_b = {
        "preco_a": 87.0,
        "preco_b": 92.0,
        "formas_a": ["Dinheiro", "PIX", "Cartão de débito"],
        "formas_b": [],
    }
    ppf = {
        "Dinheiro": 82.9,
        "PIX": 82.9,
        "Cartão de débito": 87.0,
        "Fiado": 92.0,
        "Cartão de crédito": 92.0,
    }

    fb = formas_b_efetivas(g_vazio_b)
    check("formas_b_efetivas inclui Fiado", "Fiado" in fb, str(fb))
    check(
        "formas_b_efetivas inclui Credito",
        "Cartão de crédito" in fb,
        str(fb),
    )
    check("formas_b_efetivas nao inclui Dinheiro", "Dinheiro" not in fb)

    norm = normalizar_precos_grupos_payload(g_vazio_b)
    check("normalize preenche formas_b", bool(norm and norm.get("formas_b")))
    check(
        "normalize B tem Fiado",
        bool(norm and "Fiado" in (norm.get("formas_b") or [])),
    )

    base = 92.0
    din = preco_venda_para_forma(
        base, ppf, "Dinheiro", precos_modo="grupos", precos_grupos=g_vazio_b
    )
    fiado = preco_venda_para_forma(
        base, ppf, "Fiado", precos_modo="grupos", precos_grupos=g_vazio_b
    )
    pix = preco_venda_para_forma(
        base, ppf, "PIX", precos_modo="grupos", precos_grupos=g_vazio_b
    )
    cred = preco_venda_para_forma(
        base,
        ppf,
        "Cartão de crédito",
        precos_modo="grupos",
        precos_grupos=g_vazio_b,
    )
    check("Dinheiro = 87 (grupo A)", abs(din - 87.0) < 0.001, str(din))
    check("PIX = 87 (grupo A)", abs(pix - 87.0) < 0.001, str(pix))
    check("Fiado = 92 (grupo B inferido)", abs(fiado - 92.0) < 0.001, str(fiado))
    check("Credito = 92 (grupo B inferido)", abs(cred - 92.0) < 0.001, str(cred))

    # Caso completo (milho 24kg) — nao muda
    g_ok = {
        "preco_a": 49.0,
        "preco_b": 52.0,
        "formas_a": ["Dinheiro", "PIX", "Cartão de débito"],
        "formas_b": [
            "Cartão de crédito",
            "Cartão de crédito parcelado",
            "Fiado",
            "Vale crédito",
            "Cashback",
            "Outro",
        ],
    }
    check(
        "milho24 Dinheiro=49",
        abs(
            preco_venda_para_forma(52, None, "Dinheiro", precos_modo="grupos", precos_grupos=g_ok)
            - 49.0
        )
        < 0.001,
    )
    check(
        "milho24 Fiado=52",
        abs(
            preco_venda_para_forma(52, None, "Fiado", precos_modo="grupos", precos_grupos=g_ok)
            - 52.0
        )
        < 0.001,
    )

    js = (ROOT / "produtos/static/produtos/js/precos_forma_pagamento.js").read_text(
        encoding="utf-8"
    )
    wiz = (ROOT / "produtos/static/produtos/js/pdv_wizard.js").read_text(encoding="utf-8")
    cad = (
        ROOT / "produtos/templates/produtos/_modal_editar_produto_cadastro_erp.inc.html"
    ).read_text(encoding="utf-8")

    check("JS formasGrupoBEfetivas", "function formasGrupoBEfetivas" in js)
    check("JS precoViaMapaPorForma", "function precoViaMapaPorForma" in js)
    check(
        "wizard sincronizarPrecosFormaAntesGravar",
        "function sincronizarPrecosFormaAntesGravar" in wiz,
    )
    check(
        "wizard cupom chama sync",
        "sincronizarPrecosFormaAntesGravar(state)" in wiz,
    )
    check(
        "cadastro auto B ao salvar",
        "resto das formas" in cad or "formasLista().filter" in cad,
    )

    util_py = (ROOT / "produtos/precos_forma_pagamento_util.py").read_text(encoding="utf-8")
    check("python formas_b_efetivas", "def formas_b_efetivas" in util_py)

    print(f"\n{ok} OK · {fail} FAIL")
    if fail:
        print("VERIFY_BUG20_TABELA_PRECO_PRINT_PATH_FAIL")
        return 1
    print("VERIFY_OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
