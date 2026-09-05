#!/usr/bin/env python
"""Prova path — chip nome do PIN na barra de mensagem do Zap."""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ok = 0
fail = 0


def check(name: str, cond: bool) -> None:
    global ok, fail
    if cond:
        ok += 1
        print(f"  OK  {name}")
    else:
        fail += 1
        print(f" FAIL {name}")


def main() -> int:
    composer = (ROOT / "produtos/templates/produtos/_wa_composer.html").read_text(encoding="utf-8")
    skin = (ROOT / "produtos/templates/produtos/_wa_skin.html").read_text(encoding="utf-8")
    page = (ROOT / "produtos/templates/produtos/atendimento_whatsapp.html").read_text(encoding="utf-8")
    cel = (ROOT / "produtos/templates/produtos/atendimento_whatsapp_celular.html").read_text(
        encoding="utf-8"
    )
    js = (ROOT / "produtos/static/produtos/js/atendimento_whatsapp.js").read_text(encoding="utf-8")

    print("WA-PIN-COMPOSER path checks")
    check("composer_btn", 'id="wa-operador-pin"' in composer)
    check("composer_card_lbl", "wa-op-pin-lbl" in composer and "Quem" in composer)
    check("composer_card_nome", 'id="wa-operador-pin-nome"' in composer)
    check("composer_antes_input", composer.find("wa-operador-pin") < composer.find('id="wa-input"'))
    check("skin_op_pin", ".wa-op-pin" in skin and "wa-op-pin-nome" in skin)
    check("skin_input_cede", "flex: 1 1 0%" in skin or "flex: 1 1 0" in skin)
    check("skin_hide_rec", "is-rec #wa-operador-pin" in skin)
    check("page_sspin", "_screensaver_pin.html" in page)
    check("cel_sspin", "_screensaver_pin.html" in cel)
    check("js_pintar", "pintarOperadorPin" in js and "nomeCurtoOperador" in js)
    check("js_trocar", "gmSspinSairEAbrirPin" in js)
    check("js_event", "gm-sspin-operador" in js)
    check("js_ls", "gm_sspin_operador" in js)
    check("js_autor_pin", "nomeOperadorPin() || 'Você'" in js)

    print(f"\nVERIFY_{'OK' if fail == 0 else 'FAIL'} {ok}/{ok + fail}")
    return 0 if fail == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
