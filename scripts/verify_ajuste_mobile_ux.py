#!/usr/bin/env python
"""Prova estática — Ajuste Mobile UX celular (overlays / teclado / scroll)."""
from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
fails: list[str] = []
oks = 0


def ok(msg: str) -> None:
    global oks
    oks += 1
    print("OK", msg)


def fail(msg: str) -> None:
    fails.append(msg)
    print("FAIL", msg)


def check(path: Path, *needles: str, label: str = "") -> None:
    text = path.read_text(encoding="utf-8")
    for n in needles:
        if n not in text:
            fail(f"{label or path.name}: falta «{n[:60]}»")
            return
    ok(f"{label or path.name}: {len(needles)} marcadores")


ma = ROOT / "produtos" / "templates" / "produtos" / "mobile_ajuste.html"
conf = ROOT / "produtos" / "templates" / "produtos" / "includes" / "agro_loja_confirm.html"
util = ROOT / "produtos" / "contagem_ciclica_util.py"

check(
    ma,
    "interactive-widget=resizes-content",
    "--ma-kb-inset",
    "window.maLockScroll",
    "ma-scroll-lock",
    "body.ma-page.ma-modal-open",
    "ma-head-actions",
    "padding-bottom: calc(0.75rem + var(--ma-safe-bottom) + var(--ma-kb-inset))",
    "maBip1On = false",
    "linhas_truncadas",
    "z-[155]",
    "z-[165]",
    "z-index: 170",
    "ma-ciclica-dias-custom",
    "Bip +1 off",
    label="mobile_ajuste.html",
)

# confirm usa maLockScroll + z 160 + kb inset
check(
    conf,
    "z-[160]",
    "maLockScroll",
    "--ma-kb-inset",
    "items-end",
    label="agro_loja_confirm.html",
)

check(
    util,
    "linhas_truncadas",
    "linhas_enviadas",
    "qs[:800]",
    label="contagem_ciclica_util.py",
)

# offer overlay alinhado ao teclado (bottom sheet no mobile)
text = ma.read_text(encoding="utf-8")
if ".ma-offer-overlay" in text and "align-items: flex-end" in text:
    ok("offer overlay bottom-sheet")
else:
    fail("offer overlay não é bottom-sheet")

if "Bip +1 off" in text:
    ok("rótulo Bip+1 off na cíclica")
else:
    fail("falta rótulo Bip+1 off")

if "flex-wrap" in text and "ma-head-actions" in text:
    ok("header actions wrap")
else:
    fail("header sem wrap")

print()
if fails:
    print(f"VERIFY_FAIL {len(fails)}")
    for f in fails:
        print(" -", f)
    raise SystemExit(1)
print(f"VERIFY_OK {oks}")
