#!/usr/bin/env python
"""Prova path PDV produtos rápidos (9 slots)."""
from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

ok = 0
fail = 0


def check(name: str, cond: bool, detail: str = ""):
    global ok, fail
    if cond:
        ok += 1
        print(f"  OK  {name}" + (f" — {detail}" if detail else ""))
    else:
        fail += 1
        print(f" FAIL {name}" + (f" — {detail}" if detail else ""))


def main():
    print("=== PDV-RAPIDO ===")
    html = (ROOT / "produtos/templates/produtos/pdv_wizard.html").read_text(encoding="utf-8")
    step = (ROOT / "produtos/templates/produtos/partials/pdv/step_produtos.html").read_text(
        encoding="utf-8"
    )
    js = (ROOT / "produtos/static/produtos/js/pdv_rapido.js").read_text(encoding="utf-8")
    wiz = (ROOT / "produtos/static/produtos/js/pdv_wizard.js").read_text(encoding="utf-8")
    urls = (ROOT / "produtos/urls.py").read_text(encoding="utf-8")
    layout = (ROOT / "produtos/pdv_topbar_layout_util.py").read_text(encoding="utf-8")
    clique = (ROOT / "produtos/pdv_topbar_clique_util.py").read_text(encoding="utf-8")

    check("overlay", 'id="pdv-rapido-overlay"' in html)
    check("grid_90", "90vw" in html and "90dvh" in html)
    check("topbar_btn", 'data-pdv-topbar-node="rapido"' in html)
    check("step_btn", 'id="pdv-btn-rapido"' in step)
    check("js_include", "pdv_rapido.js" in html)
    check("js_f11", "F11" in js and "Digit1" in js)
    check("js_add", "AgroPdvAddProductById" in js)
    check("wiz_add_id", "window.AgroPdvAddProductById" in wiz)
    check("urls", "api_pdv_rapido_slots" in urls and "api_pdv_rapido_foto" in urls)
    check("layout_key", '"rapido"' in layout)
    check("clique_key", '"rapido"' in clique)
    check("mig", (ROOT / "produtos/migrations/0150_pdv_rapido_slot.py").exists())
    check("model", "class PdvRapidoSlotAgro" in (ROOT / "produtos/models.py").read_text(encoding="utf-8"))

    sys.path.insert(0, str(ROOT))
    from produtos.pdv_rapido_util import SLOTS, normalizar_apelido, slot_int

    check("nove", list(SLOTS) == list(range(1, 10)))
    check("slot_ok", slot_int("3") == 3 and slot_int(0) is None)
    check("apelido", normalizar_apelido("  a  b ") == "a b")

    print(f"== {ok} ok · {fail} fail ==")
    return 0 if fail == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
