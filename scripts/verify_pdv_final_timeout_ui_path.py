# -*- coding: utf-8 -*-
"""Prova path PDV-FINAL-TIMEOUT-UI (bug loja #22).

Nathan · Caixa Centro · 05/09 · «não esta finalizando normalmente».
Confirmar sem timeout + painel PAGAR esmagado em 1440×900.

  python scripts/verify_pdv_final_timeout_ui_path.py
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

fails: list[str] = []
oks: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    if cond:
        oks.append(name)
        print("  OK  " + name + ((" — " + detail) if detail else ""))
    else:
        fails.append(name)
        print("  FAIL " + name + ((" — " + detail) if detail else ""))


def main() -> int:
    wiz = (ROOT / "produtos/static/produtos/js/pdv_wizard.js").read_text(encoding="utf-8")
    html = (ROOT / "produtos/templates/produtos/pdv_wizard.html").read_text(encoding="utf-8")
    step = (
        ROOT / "produtos/templates/produtos/partials/pdv/step_produtos.html"
    ).read_text(encoding="utf-8")

    print("== Timeout Confirmar ==")
    check("const_timeout_55s", "PDV_CONFIRM_POST_TIMEOUT_MS = 55000" in wiz)
    check("jsonPost_opts_timeoutMs", "opts.timeoutMs" in wiz and "AbortController" in wiz)
    check("jsonPost_pdvTimeout_flag", "eTo.pdvTimeout = true" in wiz)
    check(
        "confirm_draft_timeout",
        "apiPdvSalvarCheckoutDraft" in wiz
        and "confirmPostOpts" in wiz
        and "timeoutMs: PDV_CONFIRM_POST_TIMEOUT_MS" in wiz,
    )
    check(
        "confirm_erp_timeout",
        "apiEnviarPedidoErp" in wiz and "confirmPostOpts" in wiz,
    )
    check(
        "mp_draft_timeout",
        "Demorou demais ao gravar após o Point" in wiz,
    )
    check(
        "msg_loja",
        "Demorou demais para gravar a venda" in wiz,
    )

    print("== Layout PAGAR 1440×900 ==")
    check(
        "media_1500x920",
        "@media (max-width: 1500px) and (max-height: 920px)" in html,
    )
    check(
        "media_old_1400x800_gone",
        "@media (max-width: 1400px) and (max-height: 800px)" not in html,
    )
    check("max_height_920_subtotal", "@media (max-height: 920px)" in html)
    check(
        "dock_overflow_visible",
        "#pdv-step1-subtotal-dock .pdv-wiz-panel.pdv-wiz-panel--orange" in html
        and "overflow: visible" in html,
    )
    check("side_actions_no_shrink", "#pdv-step1-subtotal-dock .pdv-step1-side-actions" in html)
    # Painel laranja do subtotal sem overflow-hidden (corta PAGAR)
    m = re.search(
        r'id="pdv-step1-subtotal-dock"[\s\S]{0,800}?pdv-wiz-panel--orange([^>]*)>',
        step,
    )
    cls = (m.group(1) if m else "") or ""
    check("step_orange_sem_overflow_hidden", "overflow-hidden" not in cls, cls.strip()[:80])

    print("== Contagem ==")
    print(f"OK {len(oks)}  FAIL {len(fails)}")
    if fails:
        print("VERIFY_FAIL")
        return 1
    print(f"VERIFY_OK {len(oks)}/{len(oks)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
