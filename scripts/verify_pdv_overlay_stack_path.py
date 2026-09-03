"""Contratos estáticos do pacote PDV-OVERLAY-STACK."""
from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
fails = 0


def check(name: str, ok: bool) -> None:
    global fails
    print(("  OK  " if ok else "  FAIL ") + name)
    if not ok:
        fails += 1


def main() -> int:
    print("verify_pdv_overlay_stack_path")
    stack = (ROOT / "produtos/static/produtos/js/agro_overlay_stack.js").read_text(encoding="utf-8")
    overlay = (ROOT / "produtos/static/produtos/js/agro_pdv_overlay.js").read_text(encoding="utf-8")
    ui = (ROOT / "produtos/templates/produtos/_agro_consulta_ui.html").read_text(encoding="utf-8")

    check("stack_file", "AgroOverlayStack" in stack and "setNested" in stack and "autoWire" in stack)
    check("overlay_v7", "agro-pdv-overlay-styles-v7" in overlay)
    check("overlay_chrome_locked", "chromeLocked" in overlay)
    check("overlay_esc_lock", "if (chromeLocked)" in overlay)
    check("overlay_caixa_layer", "agro-caixa-modal-layer" in overlay)
    check("ui_includes_stack", "agro_overlay_stack.js" in ui)
    check("repasse_stack", "AgroOverlayStack" in (ROOT / "produtos/static/produtos/js/pdv_repasse_vila.js").read_text(encoding="utf-8"))
    check("pedir_stack", "AgroOverlayStack" in (ROOT / "produtos/static/produtos/js/pdv_pedir_loja.js").read_text(encoding="utf-8"))
    check("uso_stack", "AgroOverlayStack" in (ROOT / "produtos/static/produtos/js/pdv_uso_loja.js").read_text(encoding="utf-8"))
    check("transf_stack", "AgroOverlayStack" in (ROOT / "produtos/static/produtos/js/pdv_transf_forcada.js").read_text(encoding="utf-8"))
    check("wizard_stack", "AgroOverlayStack" in (ROOT / "produtos/static/produtos/js/pdv_wizard.js").read_text(encoding="utf-8"))
    check("compras_auto", 'data-agro-stack="auto"' in (ROOT / "produtos/templates/produtos/compras.html").read_text(encoding="utf-8"))

    print(("OK" if fails == 0 else "FAIL") + f" verify_pdv_overlay_stack_path — {12 - fails}/12")
    return 1 if fails else 0


if __name__ == "__main__":
    raise SystemExit(main())
