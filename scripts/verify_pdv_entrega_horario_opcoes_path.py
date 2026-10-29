#!/usr/bin/env python3
"""PDV-ENT-HORARIO-OPCOES — overlay maior + horário 9h–17h obrigatório."""
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
    css = (ROOT / "produtos/templates/produtos/pdv_wizard.html").read_text(encoding="utf-8")
    js = (ROOT / "produtos/static/produtos/js/pdv_wizard.js").read_text(encoding="utf-8")

    print("=== entrega horario opcoes ===")
    check('data-pdv-ed-painel="detalhes"]' in css and "52rem" in css, "overlay detalhes maior (52rem)")
    check('type="time"' not in html, "sem relógio livre")
    check('id="pdv-ed-horario-opcoes"' in html, "grade de horários")
    check('type="hidden"' in html and 'id="pdv-entrega-horario"' in html, "valor HH:MM escondido")
    for h in range(9, 18):
        val = f"{h:02d}:00"
        check(f'value="{val}"' in html, f"opção {val}")
    check("Escolha o horário da entrega" in js, "F7 exige horário")
    check("entregaHorariosFixos" in js and "commitEntregaHorarioOpcao" in js, "JS valida 9h–17h")
    check("if (!hor)" in js, "bloqueia confirmar sem opção")

    print(f"\n{PASS} ok · {FAIL} fail")
    if FAIL:
        print("FAILED")
        return 1
    print("VERIFY_OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
