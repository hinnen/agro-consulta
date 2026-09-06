"""Verify CAD-XLSX-COLS loja: checkboxes + Peso, sem tocar PDV/caixa."""
from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
import django

django.setup()

ok = 0
fail = 0


def check(cond: bool, msg: str) -> None:
    global ok, fail
    if cond:
        ok += 1
        print(("OK  " + msg).encode("ascii", "replace").decode("ascii"))
    else:
        fail += 1
        print(("FAIL " + msg).encode("ascii", "replace").decode("ascii"))


util = (ROOT / "produtos" / "cadastro_planilha_util.py").read_text(encoding="utf-8")
js = (ROOT / "produtos" / "static" / "produtos" / "js" / "cadastro_erp_panel.js").read_text(encoding="utf-8")
html = (ROOT / "produtos" / "templates" / "produtos" / "produtos_cadastro_erp.html").read_text(encoding="utf-8")
cat = (ROOT / "produtos" / "catalogo_agro.py").read_text(encoding="utf-8")

from produtos.cadastro_planilha_util import (
    COL_MODELO,
    COL_PESO,
    COL_SUBCATEGORIA_2,
    COL_UNIDADE,
    EXPORT_COL_KEYS,
    IMPORT_KEYS,
    OVERLAY_IMPORT_KEYS,
    headers_export,
)

check(COL_PESO == "peso_etiqueta", "COL_PESO = peso_etiqueta")
check(COL_PESO in EXPORT_COL_KEYS, "export tem Peso")
check(COL_PESO in IMPORT_KEYS, "import aceita Peso")
check(COL_PESO in OVERLAY_IMPORT_KEYS, "overlay grava Peso")
check(COL_SUBCATEGORIA_2 in EXPORT_COL_KEYS, "export tem Sub 2")
check(COL_UNIDADE in EXPORT_COL_KEYS, "export tem Unidade")
check(COL_MODELO in EXPORT_COL_KEYS, "export tem Modelo")
labels = [lab for lab, _ in headers_export()]
check("Peso" in labels, "header Excel Peso")
check("ov.peso_etiqueta" in util, "gravar patch seta peso_etiqueta")
check('COL_PESO: ("peso"' in util or "COL_PESO: (" in util, "alias Peso no map headers")
check("key: 'peso_etiqueta', label: 'Peso'" in js, "checkbox Excel Peso")
check("key: 'subcategoria_2'" in js, "checkbox Sub 2")
check("key: 'unidade'" in js, "checkbox Unidade")
check("key: 'modelo'" in js, "checkbox Modelo")
check("peso_etiqueta: 'Peso'" in js, "rotulo historico Peso")
check("cadastro_erp_panel.js' %}?v=30" in html, "cache bust JS v=30")
check('"peso_etiqueta": ""' in cat, "catalogo PG tem peso_etiqueta vazio")
check("pdv_wizard" not in util, "util planilha nao puxa wizard")

pdv_js = ROOT / "produtos" / "static" / "produtos" / "js" / "pdv_wizard.js"
caixa = ROOT / "produtos" / "caixa_util.py"
check(pdv_js.exists(), "PDV wizard JS intacto no branch")
check(caixa.exists(), "caixa_util intacto no branch")

print(f"\n{ok} OK / {fail} FAIL")
sys.exit(1 if fail else 0)
