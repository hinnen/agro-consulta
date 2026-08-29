#!/usr/bin/env python
"""Prova detalhada do overlay PDV limpo (REPASSE-PDV-OVERLAY-LIMPO)."""
from __future__ import annotations

import subprocess
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


def forbid(path: str, *needles: str) -> None:
    global oks
    p = ROOT / path
    if not p.exists():
        fails.append(f"MISSING {path}")
        return
    text = p.read_text(encoding="utf-8", errors="replace")
    for n in needles:
        if n in text:
            fails.append(f"{path} still has {n!r}")
        else:
            oks += 1


HTML = "produtos/templates/produtos/partials/pdv/repasse_vila_overlay.html"
JS = "produtos/static/produtos/js/pdv_repasse_vila.js"

# Shell grande + hero
check(HTML, "pdv-repasse-overlay", "rp-shell", "min(98rem", "96dvh", "rp-hero", "rp-hero-cofre")
check(HTML, "pdv-rp-hero-cofre", "Ainda separar · cofrinho", "Levar ao Centro (acumulado)", "pdv-rp-total", "pdv-rp-manual")
check(HTML, "pdv-rp-mes-dinheiro", "pdv-rp-card-cofre", "pdv-rp-mes-lucro-ficou", "pdv-rp-dia-todas")
check(HTML, "Enviado ao Centro (mês", "Cofrinho (saldo acumulado)", "Lucro ficou na Vila", "todas as formas")

# Detalhes recolhidos (não inline quem/forma no hero)
check(HTML, "rp-fold", "Detalhes do dia", "% lucro e opções", "pdv-rp-receita", "pdv-rp-acumulado")
check(HTML, "pdv-rp-btn-quem", "pdv-rp-btn-forma", "pdv-rp-btn-pin", "Toque para escolher", "Toque para digitar")

# Popups quem / forma / PIN
check(HTML, "pdv-rp-quem-modal", "pdv-rp-forma-modal", "pdv-rp-pin-modal", "pdv-rp-btn-quem", "pdv-rp-btn-forma", "pdv-rp-btn-pin")
check(HTML, "pdv-rp-quem-grid", "pdv-rp-forma-grid", "pdv-rp-pin", "pdv-rp-quem-ok", "pdv-rp-pin-ok", "pdv-rp-forma-ok")
check(HTML, "Padrão: Dinheiro", "pdv-rp-enviar-hint", "pdv-rp-cofre-aviso", "NÃO levar")

# JS: foco + Enter + fluxo
check(JS, "focusSoon", "openQuemModal", "openPinModal", "openFormaModal", "tryConfirmarFlow")
check(JS, "ev.key === 'Enter'", "closeQuemModal", "closePinModal", "closeFormaModal")
check(JS, "Inclui ", "dias anteriores", "fetchHistoricoMes", "api/repasse-vila/historico/")
check(JS, "formaPag = 'Dinheiro'", "pendingConfirmar", "Escape", "pdv-rp-hero-cofre")
check(JS, "Deixe ", "Confirma o repasse", "ainda separar (acumulado)", "notifyParentFecharAtualizar")
check(JS, "api/repasse-vila/confirmar/", "incluir_acumulado", "separar_reserva")

# Wiring PDV
check("produtos/templates/produtos/pdv_wizard.html", "pdv_repasse_vila.js", "repasse_vila_overlay", "pdv-topbar-repasse-btn")

print(f"checks_ok={oks} fails={len(fails)}")
for f in fails:
    print("FAIL", f)
if fails:
    sys.exit(1)

# Node syntax
r = subprocess.run(
    ["node", "--check", str(ROOT / JS)],
    capture_output=True,
    text=True,
)
if r.returncode != 0:
    print("FAIL node --check", r.stderr or r.stdout)
    sys.exit(1)
oks += 1
print("OK node --check")

print(f"checks_ok={oks} fails=0")
print("VERIFY_REPASSE_PDV_OVERLAY_OK")
