#!/usr/bin/env python
"""Path — Overlay: fundo escuro nao fecha (so X / FECHAR / Esc)."""
from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MARKER = "/* Fundo nao fecha — so X / FECHAR / Esc */"
fails: list[str] = []
oks = 0


def ok(msg: str) -> None:
    global oks
    oks += 1
    print("OK", msg)


def fail(msg: str) -> None:
    fails.append(msg)
    print("FAIL", msg)


def read(rel: str) -> str:
    return (ROOT / rel).read_text(encoding="utf-8")


def must_have(text: str, needle: str, label: str) -> None:
    if needle not in text:
        fail(f"{label} missing {needle!r}")
    else:
        ok(f"{label} has marker/contract")


def must_not(text: str, needle: str, label: str) -> None:
    if needle in text:
        fail(f"{label} still has bad pattern {needle!r}")
    else:
        ok(f"{label} no bad pattern")


# --- Hotspots com marcador ---
HOTSPOTS = [
    ("produtos/static/produtos/js/pdv_wizard.js", "pdv_wizard"),
    ("produtos/static/produtos/js/consulta_produtos.js", "consulta"),
    ("produtos/static/produtos/js/agro_pdv_overlay.js", "agro_pdv_overlay"),
    ("produtos/static/produtos/js/agro_perf_config.js", "agro_perf"),
    ("produtos/static/produtos/js/caixa_retiradas_export.js", "caixa_export"),
    ("produtos/static/produtos/js/cadastro_erp_panel.js", "cadastro_erp"),
    ("produtos/templates/produtos/emprestimos_consulta.html", "emprestimos"),
    ("produtos/templates/produtos/produtos_gestao.html", "gestao"),
    ("produtos/templates/produtos/mobile_ajuste.html", "mobile_ajuste"),
]

for rel, label in HOTSPOTS:
    must_have(read(rel), MARKER, label)

# --- Anti-padrões (fundo ainda fecha) ---
pdv = read("produtos/static/produtos/js/pdv_wizard.js")
must_not(
    pdv,
    "paymentFormaModalBackdrop.addEventListener('click', closePaymentFormaModal)",
    "pdv_wizard",
)

overlay = read("produtos/static/produtos/js/agro_pdv_overlay.js")
# dismiss no fundo nao pode chamar close()
if "data-agro-pdv-overlay-dismiss" in overlay and MARKER not in overlay:
    fail("agro_pdv_overlay dismiss sem marcador")
else:
    ok("agro_pdv_overlay dismiss com marcador")

mobile = read("produtos/templates/produtos/mobile_ajuste.html")
bad_ciclica = (
    "ma-ciclica-ctrl-backdrop')?.addEventListener('click', () => {\n"
    "  maCiclicaToggleCtrl(false);"
)
if bad_ciclica in mobile or (
    "ma-ciclica-ctrl-backdrop" in mobile
    and "maCiclicaToggleCtrl(false)" in mobile[
        mobile.find("ma-ciclica-ctrl-backdrop") : mobile.find("ma-ciclica-ctrl-backdrop") + 200
    ]
):
    # checagem estreita: bloco backdrop nao deve chamar toggle false
    idx = mobile.find("ma-ciclica-ctrl-backdrop")
    chunk = mobile[idx : idx + 220] if idx >= 0 else ""
    if "maCiclicaToggleCtrl(false)" in chunk:
        fail("mobile_ajuste ciclica backdrop ainda fecha")
    else:
        ok("mobile_ajuste ciclica backdrop nao fecha")
else:
    ok("mobile_ajuste ciclica backdrop nao fecha")

# Contagem minima de marcadores no app produtos
produtos = ROOT / "produtos"
count = 0
for p in produtos.rglob("*"):
    if p.suffix.lower() not in {".js", ".html"}:
        continue
    if "node_modules" in p.parts or "__pycache__" in p.parts:
        continue
    try:
        t = p.read_text(encoding="utf-8", errors="ignore")
    except OSError:
        continue
    count += t.count(MARKER)

if count < 20:
    fail(f"marcadores poucos no produtos/ ({count} < 20)")
else:
    ok(f"marcadores no produtos/ = {count}")

print()
print(f"{oks} OK · {len(fails)} FAIL")
raise SystemExit(1 if fails else 0)
