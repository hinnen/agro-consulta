#!/usr/bin/env python3
"""PDV-CHAT-FAB-UX — aba um pouco maior + alerta pisca 2 cores."""
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

    print("=== chat fab ===")
    check("min-height: 3.15rem" in html, "aba um pouco maior (3.15rem)")
    check("width: min(30rem" in html, "janela um pouco maior (30rem)")
    check("height: min(34rem" in html, "janela um pouco mais alta (34rem)")
    check("cl-pisca-alerta" in html, "animacao pisca 2 cores")
    check("#ffedd5" in html and "#fecaca" in html, "laranja ↔ vermelho no alerta")
    check("cl-badge-pop" in html, "badge pulsa com mensagem")
    check("is-alerta" in js and "mensagens novas" in js, "title/aria com contagem")
    check("classList.add('is-alerta')" in js, "JS aplica is-alerta")

    print(f"\n{PASS} ok · {FAIL} fail")
    return 1 if FAIL else 0


if __name__ == "__main__":
    raise SystemExit(main())
