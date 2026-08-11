#!/usr/bin/env python
"""Prova Folha Compras × família saco (exclui filho + rollup 5+0,5=5,5). VERIFY_OK / VERIFY_FAIL."""
from __future__ import annotations

import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

FAIL: list[str] = []
OK = 0


def check(name: str, cond: bool, detail: str = "") -> None:
    global OK
    if cond:
        OK += 1
        print(f"  OK  {name}" + (f" - {detail}" if detail else ""))
    else:
        FAIL.append(name + (f" - {detail}" if detail else ""))
        print(f"  FAIL {name}" + (f" - {detail}" if detail else ""))


def main() -> None:
    from produtos.compras_familia_folha_util import (
        arred_qtd_folha_compras,
        preparar_pids_folha_familia,
        rollup_qtds_filhos_no_pai,
    )

    # unit: 5 saco + 5×0,1 granel = 5,5
    filhos = {
        "200": {"pai_id": "100", "fator": 0.1, "pai_nome": "Saco 10kg", "filho_nome": "Granel"},
    }
    q = {"100": 5.0, "200": 5.0}
    out = rollup_qtds_filhos_no_pai(q, filhos, display_pais={"100"})
    check("rollup_5_5", abs(float(out.get("100") or 0) - 5.5) < 0.01, str(out.get("100")))
    check("arred_5_5", arred_qtd_folha_compras(5.5) == 5.5)
    check("arred_meio", arred_qtd_folha_compras(5.55) == 5.6)

    # exclude filho from display; force pai if missing
    disp, sales, fmap, hints = preparar_pids_folha_familia(
        ["200", "300"],
        nomes_hints={},
    )
    # without DB overlays, filhos_map empty — still ok
    check("preparar_sem_overlay_ok", isinstance(disp, list) and isinstance(sales, list))

    # mock index by patching
    import produtos.compras_familia_folha_util as mod

    real = mod.indice_filhos_custo_familia
    mod.indice_filhos_custo_familia = lambda: {
        "200": {"pai_id": "100", "fator": 0.1, "pai_nome": "Saco 10kg", "filho_nome": "Granel 1kg"},
    }
    try:
        disp2, sales2, _, hints2 = preparar_pids_folha_familia(["200", "300"])
        check("exclui_filho", "200" not in disp2, str(disp2))
        check("mantem_outro", "300" in disp2, str(disp2))
        check("injeta_pai", "100" in disp2, str(disp2))
        check("sales_tem_filho", "200" in sales2, str(sales2))
        check("hint_pai", bool(hints2.get("100")), str(hints2))
    finally:
        mod.indice_filhos_custo_familia = real

    # markers in views + print JS
    views = open(os.path.join(ROOT, "produtos", "views.py"), encoding="utf-8").read()
    check("views_import_familia", "compras_familia_folha_util" in views)
    check("views_display_pids", "display_pids" in views)
    check("views_arred_folha", "arred_qtd_folha_compras" in views)

    a4 = open(
        os.path.join(
            ROOT,
            "produtos",
            "templates",
            "produtos",
            "includes",
            "_compras_relatorio_a4_core_js.html",
        ),
        encoding="utf-8",
    ).read()
    check("a4_decimal", "maximumFractionDigits: 1" in a4)

    plan = open(
        os.path.join(
            ROOT,
            "produtos",
            "templates",
            "produtos",
            "includes",
            "_compras_planilha_print_js_snippet.html",
        ),
        encoding="utf-8",
    ).read()
    check("planilha_decimal", "maximumFractionDigits: 1" in plan)

    util_path = os.path.join(ROOT, "produtos", "compras_familia_folha_util.py")
    check("util_existe", os.path.isfile(util_path))

    print()
    print(f"OK {OK}  FAIL {len(FAIL)}")
    if FAIL:
        print("VERIFY_FAIL")
        for f in FAIL:
            print(" ", f)
        sys.exit(1)
    print("VERIFY_OK")


if __name__ == "__main__":
    main()
