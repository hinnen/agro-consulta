# -*- coding: utf-8 -*-
"""Lista telas HTML que usam UI Agro mas não incluem teclado PIN (sspin)."""
from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TEMPLATES = [
    ROOT / "produtos" / "templates",
    ROOT / "base" / "templates",
    ROOT / "rh" / "templates",
    ROOT / "estoque" / "templates",
    ROOT / "tarefas" / "templates",
    ROOT / "relatorios" / "templates",
]


def is_page(text: str, rel: str) -> bool:
    if any(x in rel.replace("\\", "/") for x in ("/includes/", "/partials/", "/_")):
        # allow top-level _ only skip deep partials named with _
        parts = rel.replace("\\", "/").split("/")
        if parts[-1].startswith("_") or "/includes/" in rel.replace("\\", "/") or "/partials/" in rel.replace("\\", "/"):
            return False
    return ("{% extends" in text) or ("<!DOCTYPE" in text) or ("<html" in text.lower())


def main() -> None:
    miss: list[str] = []
    ok: list[str] = []
    for base in TEMPLATES:
        if not base.exists():
            continue
        for p in sorted(base.rglob("*.html")):
            rel = str(p.relative_to(ROOT)).replace("\\", "/")
            text = p.read_text(encoding="utf-8", errors="replace")
            if not is_page(text, rel):
                continue
            has = (
                "_screensaver_pin.html" in text
                or "lancamentos_pin_entrada.html" in text
            )
            # só páginas “de loja” com UI / base / agro
            uses_ui = (
                "_agro_consulta_ui.html" in text
                or "extends \"base.html\"" in text
                or "extends 'base.html'" in text
                or 'extends "base.html"' in text
                or "block content" in text
            )
            if not uses_ui and not has:
                # standalone pages that still matter if they post financial
                if "alert(" not in text and "fetch(" not in text:
                    continue
            if has:
                ok.append(rel)
            else:
                miss.append(rel)

    print(f"COM_SSPIN {len(ok)}")
    print(f"SEM_SSPIN {len(miss)}")
    for r in miss:
        print("MISS", r)


if __name__ == "__main__":
    main()
