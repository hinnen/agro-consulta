#!/usr/bin/env python3
"""PDV-CHAT-ENTREGA-DOCK — aba Chat não cobre Voltar/F7 na etapa Entrega."""
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
    html = (ROOT / "produtos/templates/produtos/partials/pdv/chat_loja_overlay.html").read_text(
        encoding="utf-8"
    )
    js = (ROOT / "produtos/static/produtos/js/pdv_chat_loja.js").read_text(encoding="utf-8")
    wiz = (ROOT / "produtos/static/produtos/js/pdv_wizard.js").read_text(encoding="utf-8")

    print("=== chat entrega dock ===")
    check("--pdv-chat-dock-left" in html, "CSS usa --pdv-chat-dock-left")
    check('data-pdv-step="entrega"] #pdv-chat-loja-dock' in html, "regra só na etapa Entrega")
    check("27.5rem" in html, "fallback CSS depois do F7 (~27.5rem)")
    check("function reposicionarDock" in js, "JS mede o botão F7")
    check("pdv-btn-next" in js and "getBoundingClientRect" in js, "JS lê a borda direita do F7")
    check("step !== 'entrega'" in js, "fora da Entrega volta ao lugar padrão")
    check("entrega: ''" in wiz, "hint da Entrega continua vazio (F7 à esquerda)")

    print(f"\n{PASS} ok · {FAIL} fail")
    if FAIL:
        print("FAILED")
        return 1
    print("VERIFY_OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
