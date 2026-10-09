# -*- coding: utf-8 -*-
"""
Prova — PDV-VALOR-RS-MODAL (wizard): após Enter no produto, pergunta «Valor em R$?».
Enter vazio / Esc / Pular → qty normal. Valor + Enter → qty = valor÷preço.
"""
from __future__ import annotations

import re
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
fails: list[str] = []
oks: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    if cond:
        oks.append(name)
        print(f"  OK  {name}" + (f" — {detail}" if detail else ""))
    else:
        fails.append(name)
        print(f"  FAIL {name}" + (f" — {detail}" if detail else ""))


def test_contratos() -> None:
    print("== Contratos ==")
    wiz = (ROOT / "produtos/static/produtos/js/pdv_wizard.js").read_text(encoding="utf-8")
    help_html = (ROOT / "produtos/templates/produtos/pdv_wizard.html").read_text(encoding="utf-8")
    check("fn_ask", "function askValorReaisParaProduto" in wiz)
    check("fn_parse", "function parseValorReaisDigitado" in wiz)
    check("titulo_modal", "Valor em R$?" in wiz)
    check("enter_vazio_pula", "Enter vazio = lançar sem valor" in wiz)
    check("btn_pular", "data-pdv-valor-rs-skip" in wiz)
    check("esc_fecha", "ev.key === 'Escape'" in wiz and "askValorReaisParaProduto" in wiz)
    check("hook_explicit", "precisaPerguntarValor" in wiz and "skipValorPrompt" in wiz)
    check("pula_etiqueta", "valor_etiqueta_balanca" in wiz and "precisaPerguntarValor" in wiz)
    check("help_tela", "Valor em R$?" in help_html)
    check(
        "version_26_76",
        (ROOT / "VERSION").read_text(encoding="utf-8").strip() == "26.76",
    )


def test_parse_node() -> None:
    print("== Parse JS (node) ==")
    wiz = (ROOT / "produtos/static/produtos/js/pdv_wizard.js").read_text(encoding="utf-8")
    m = re.search(
        r"function parseValorReaisDigitado\(raw\) \{[\s\S]*?\n    \}",
        wiz,
    )
    check("extract_parse", bool(m))
    if not m:
        return
    calc = re.search(
        r"function calcularQtdPorValorTotal\(precoUnit, valorTotal\) \{[\s\S]*?\n    \}",
        wiz,
    )
    check("extract_calc", bool(calc))
    js = (
        (m.group(0) if m else "")
        + "\n"
        + (calc.group(0) if calc else "")
        + """
const assert = (c, msg) => { if (!c) { console.error('FAIL', msg); process.exit(2); } };
assert(parseValorReaisDigitado('') === null, 'vazio');
assert(parseValorReaisDigitado('  ') === null, 'espaco');
assert(parseValorReaisDigitado('10') === 10, '10');
assert(parseValorReaisDigitado('10,50') === 10.5, '10,50');
assert(parseValorReaisDigitado('R$10') === 10, 'R$10');
assert(parseValorReaisDigitado('0') === null, 'zero');
assert(calcularQtdPorValorTotal(9.4, 10) === 1.064, 'qty');
console.log('OK js_valor_rs_modal');
"""
    )
    with tempfile.NamedTemporaryFile("w", suffix=".js", delete=False, encoding="utf-8") as f:
        f.write(js)
        path = f.name
    r = subprocess.run(["node", path], capture_output=True, text=True)
    check("js_node", r.returncode == 0 and "OK js_valor_rs_modal" in r.stdout, (r.stderr or r.stdout)[:120])


def main() -> int:
    print("PDV-VALOR-RS-MODAL — prova de path\n")
    test_contratos()
    test_parse_node()
    print(f"\nOK={len(oks)} FAIL={len(fails)}")
    print("PREP_FAILS=" + str(len(fails)))
    return 1 if fails else 0


if __name__ == "__main__":
    raise SystemExit(main())
