#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Prova estática — PDV edição rápida: 6 barras extras + etiqueta com preset."""
from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OK = 0
FAIL = 0


def check(name: str, cond: bool, detail: str = "") -> None:
    global OK, FAIL
    if cond:
        OK += 1
        print(f"OK  {name}")
    else:
        FAIL += 1
        print(f"FAIL {name}" + (f" — {detail}" if detail else ""))


def main() -> int:
    tpl = (ROOT / "produtos/templates/produtos/pdv_wizard.html").read_text(encoding="utf-8")
    js = (ROOT / "produtos/static/produtos/js/pdv_wizard.js").read_text(encoding="utf-8")
    views = (ROOT / "produtos/views.py").read_text(encoding="utf-8")

    for i in range(1, 7):
        check(f"html cb-op-{i}", f"pdv-quick-product-edit-cb-op-{i}" in tpl)
    check("html adicionar código", "Adicionar código" in tpl)
    check("html ja cadastrados", "Códigos já cadastrados" in tpl)
    check("html cb ops 1 linha", "repeat(6, minmax(0, 1fr))" in tpl and "pdv-pe-cb-ops-grid" in tpl)
    check("html painel largo", "96rem" in tpl and "pdv-product-edit-panel" in tpl)
    check("html botão etiqueta", 'id="pdv-quick-product-edit-etiqueta"' in tpl)
    check("html modal etq preset", 'id="pdv-pe-etq-preset"' in tpl)
    check("html modal etq imprimir", 'id="pdv-pe-etq-imprimir"' in tpl)
    check("html core etiquetas", "produtos_etiquetas_core.js" in tpl)
    check("html AGRO_ETQ_CFG", "AGRO_ETQ_CFG" in tpl)

    check("js fill barras", "fillQuickProductBarrasCadastro" in js)
    check("js collect barras", "collectQuickProductBarrasPayload" in js)
    check("js topo sempre vazio", "Campo de cima sempre vazio" in js)
    check("js principal lembrado", "quickProductEditPrincipalCb" in js)
    check("js max 6", "PDV_QUICK_CB_OPS_MAX = 6" in js)
    check("js save opcionais pack", "barrasPack.opcionais" in js)
    check("js novo vira adicional", "if (novo) pushOp(novo)" in js)
    check("js open etq", "openPdvPeEtqModal" in js)
    check("js imprimir etq", "imprimirPdvPeEtiqueta" in js)
    check("js origem pdv_edicao", "origem: 'pdv_edicao'" in js)
    check("js merge presets", "mergeServerPresets" in js)
    check("js Esc fecha etq primeiro", "closePdvPeEtqModal" in js and "peEtqModal" in js)

    check(
        "api edicao_rapida opcionais",
        "codigos_barras_opcionais" in views
        and "codigos_barras_opcionais_de_cadastro_extras" in views
        and re.search(
            r"def api_pdv_produto_edicao_rapida[\s\S]{0,8000}?codigos_barras_opcionais",
            views,
        )
        is not None,
    )

    print(f"\n{OK}/{OK + FAIL} provas")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
