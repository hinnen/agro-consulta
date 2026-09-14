#!/usr/bin/env python
"""PDV: modal Entregas fora do painel Produtos (evita trava no Pagamento)."""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ok = 0
fail = 0


def check(cond: bool, msg: str) -> None:
    global ok, fail
    if cond:
        ok += 1
        print(f"  OK  {msg}")
    else:
        fail += 1
        print(f"  FAIL {msg}")


def main() -> int:
    html = (
        ROOT / "produtos" / "templates" / "produtos" / "partials" / "pdv" / "step_produtos.html"
    ).read_text(encoding="utf-8")
    wiz = (ROOT / "produtos" / "static" / "produtos" / "js" / "pdv_wizard.js").read_text(
        encoding="utf-8"
    )

    print("--- PDV entregas modal fora do painel produtos ---")
    idx_panel = html.find('data-step-panel="produtos"')
    idx_close = html.find("</section>", idx_panel)
    idx_dlg = html.find('id="pdv-entregas-pendentes-modal"')
    check(idx_panel > 0 and idx_close > 0 and idx_dlg > 0, "marcadores HTML presentes")
    check(idx_dlg > idx_close, "dialog Entregas depois do </section> do painel produtos")
    check(html.count("<section") == html.count("</section>"), "sections balanceadas")

    check("function ensureEntregasModalNoBody" in wiz, "helper move modal pro body")
    check("ensureEntregasModalNoBody()" in wiz, "openEntregas chama ensure")
    check("appendChild(dlg)" in wiz, "appendChild no body")

    total = ok + fail
    print(f"\nVERIFY_OK {ok}/{total}" if fail == 0 else f"\nVERIFY_FAIL {ok}/{total}")
    return 0 if fail == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
