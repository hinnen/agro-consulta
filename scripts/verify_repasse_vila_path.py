#!/usr/bin/env python
"""Verify paths/strings for Repasse Vila → Centro."""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
fails: list[str] = []
oks = 0


def check(path: str, *needles: str) -> None:
    global oks
    p = ROOT / path
    if not p.exists():
        fails.append(f"MISSING {path}")
        return
    text = p.read_text(encoding="utf-8", errors="replace")
    for n in needles:
        if n not in text:
            fails.append(f"{path} missing {n!r}")
        else:
            oks += 1


check("produtos/repasse_vila_util.py", "calcular_disponivel", "confirmar_repasse", "aplicar_repasses_pendentes_centro")
check("produtos/views_repasse_vila.py", "repasse_vila_view", "api_repasse_vila_confirmar")
check("produtos/urls.py", "repasse_vila", "api/repasse-vila/confirmar/")
check("produtos/models.py", "RepasseVilaCentroAgro", "RepasseVilaConfigAgro")
check("produtos/migrations/0087_repasse_vila_centro.py", "RepasseVilaCentroAgro")
check("produtos/templates/produtos/repasse_vila.html", "Transferir no PDV", "repasse_help")
check("produtos/templates/produtos/partials/pdv/repasse_vila_overlay.html", "pdv-repasse-overlay")
check("produtos/static/produtos/js/pdv_repasse_vila.js", "api/repasse-vila/confirmar/")
check("produtos/templates/produtos/pdv_wizard.html", "pdv-topbar-repasse-btn", "pdv_repasse_vila.js")
check("produtos/templates/produtos/dashboard_gerencial.html", "repasse_vila")
check("produtos/templates/produtos/includes/repasse_help_agents.html", "O que é este repasse")
check("produtos/templates/produtos/includes/repasse_aviso_abertura.html", "Repasse da Vila")
check("produtos/views.py", "aplicar_repasses_pendentes_centro", "repasse_aviso_abertura")

print(f"checks_ok={oks} fails={len(fails)}")
for f in fails:
    print("FAIL", f)
if fails:
    sys.exit(1)
print("VERIFY_OK")
