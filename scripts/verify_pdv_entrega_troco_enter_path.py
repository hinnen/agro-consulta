#!/usr/bin/env python3
"""PDV-ENT-TROCO-ENTER — Enter/F7 vazio preenche o total (sem troco)."""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
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
    html = (ROOT / "produtos/templates/produtos/partials/pdv/entrega_wizard_overlay.html").read_text(
        encoding="utf-8"
    )
    js = (ROOT / "produtos/static/produtos/js/pdv_wizard.js").read_text(encoding="utf-8")

    print("=== entrega troco enter ===")
    check("function preencherEntregaTrocoSemTroco" in js, "preenche total da venda")
    check("moneyFieldDisplay(total)" in js, "usa o total no campo")
    check("if (!val)" in js and "preencherEntregaTrocoSemTroco" in js, "campo vazio auto-preenche")
    check("use 0 ou 0,00 se não precisar" not in js, "sumiu o alerta antigo de 0,00")
    check("Enter = sem troco" in html, "placeholder Enter")
    check("Enter</strong> vazio = sem troco" in html, "dica na tela")

    print(f"\n{PASS} ok · {FAIL} fail")
    if FAIL:
        print("FAILED")
        return 1
    print("VERIFY_OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
