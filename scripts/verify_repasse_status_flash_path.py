#!/usr/bin/env python
"""Prova — status central Repasse PDV (REPASSE-STATUS-FLASH)."""
from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

fails: list[str] = []
oks = 0


def ok(msg: str) -> None:
    global oks
    oks += 1
    print(f"OK {msg}")


def fail(msg: str) -> None:
    fails.append(msg)
    print(f"FAIL {msg}")


def main() -> int:
    overlay = (
        ROOT / "produtos/templates/produtos/partials/pdv/repasse_vila_overlay.html"
    ).read_text(encoding="utf-8", errors="replace")
    js = (ROOT / "produtos/static/produtos/js/pdv_repasse_vila.js").read_text(
        encoding="utf-8", errors="replace"
    )

    if 'id="pdv-rp-status-flash"' in overlay and "rp-status-flash-panel" in overlay:
        ok("markup flash central")
    else:
        fail("sem flash central")

    if "rp-status-pulse" in overlay and "#pdv-rp-status-flash" in overlay:
        ok("css pulse + z-index flash")
    else:
        fail("sem css flash")

    if "min(42rem" in overlay and "pdv-rp-aviso-msg" in overlay:
        ok("aviso modal maior")
    else:
        fail("aviso ainda pequeno")

    if "function setStatus(" in js and "function requestCloseOverlay(" in js:
        ok("helpers setStatus + requestCloseOverlay")
    else:
        fail("sem helpers status")

    if "setStatus('Transferindo…', 'busy')" in js:
        ok("Transferindo usa flash busy")
    else:
        fail("Transferindo sem flash")

    if "showNestedPopup(avisoModal)" in js and "hideNestedPopup(avisoModal)" in js:
        ok("aviso via nested popup (sem stack freeze)")
    else:
        fail("aviso ainda no AgroOverlayStack")

    if "requestCloseOverlay" in js and "transferência em andamento" in js:
        ok("bloqueia fechar durante busy")
    else:
        fail("não bloqueia fechar")

    if 'id="pdv-rp-status"' in overlay and "border-4 border-rose-600" in overlay:
        ok("faixa status vermelha reforçada")
    else:
        fail("faixa status fraca")

    print(f"\n{oks} OK · {len(fails)} FAIL")
    for f in fails:
        print(f"  - {f}")
    return 1 if fails else 0


if __name__ == "__main__":
    raise SystemExit(main())
