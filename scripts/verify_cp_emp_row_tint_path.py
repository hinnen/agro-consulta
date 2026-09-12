#!/usr/bin/env python
"""Prova estática: fundo laranja nas linhas Pagamento/Juros de Empréstimo no CP."""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
FAIL = 0
OK = 0


def check(name: str, cond: bool, detail: str = "") -> None:
    global FAIL, OK
    if cond:
        OK += 1
        print(f"  OK  {name}" + (f" — {detail}" if detail else ""))
    else:
        FAIL += 1
        print(f" FAIL {name}" + (f" — {detail}" if detail else ""))


def main() -> int:
    print("=== PATH CP-EMP-ROW-TINT ===\n")
    html = (ROOT / "produtos/templates/produtos/lancamentos_contas_pagar_teste.html").read_text(
        encoding="utf-8"
    )
    check("CSS classe sv-row-emp-pg", ".sv-list-item.sv-row-emp-pg" in html)
    check("fundo #fff7ed", "#fff7ed" in html)
    check("helper isRowEmprestimoPagamentoOuJuros", "function isRowEmprestimoPagamentoOuJuros" in html)
    check("regex pagamento|juros de emprest", "pagamento|juros) de emprest" in html)
    check("aplica classe no renderItem", "sv-row-emp-pg" in html and "empCls" in html)
    check("defaults planos dívida/juros", "plano_divida_interno" in html and "plano_juros_interno" in html)

    print(f"\n=== RESULTADO: {OK} ok · {FAIL} falha ===")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
