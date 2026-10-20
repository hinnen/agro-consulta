# -*- coding: utf-8 -*-
"""REL-GRUPO-COLUNAS — colunas escolhíveis + ordenar cabeçalho (Vendas por grupo)."""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
sys.path.insert(0, str(ROOT))

import django

django.setup()

from django.test import Client, override_settings
from django.urls import reverse

FAILS: list[str] = []
OKS = 0


def ok(msg: str) -> None:
    global OKS
    OKS += 1
    print("OK", msg)


def fail(msg: str) -> None:
    FAILS.append(msg)
    print("FAIL", msg)


def check(cond: bool, msg: str) -> None:
    if cond:
        ok(msg)
    else:
        fail(msg)


def read(rel: str) -> str:
    return (ROOT / rel).read_text(encoding="utf-8", errors="replace")


def check_files() -> None:
    print("--- paths ---")
    paths = [
        "produtos/vendas_grupo_table.py",
        "produtos/static/produtos/js/vendas_grupo_table.js",
        "produtos/templates/produtos/includes/vendas_grupo_tabela.html",
        "produtos/relatorios_central_views.py",
        "produtos/relatorios_vendas_util.py",
        "produtos/templates/produtos/relatorios_generico.html",
        "produtos/templatetags/agro_exibir.py",
    ]
    for p in paths:
        check((ROOT / p).is_file(), f"exists {p}")
    vgt = read("produtos/vendas_grupo_table.py")
    js = read("produtos/static/produtos/js/vendas_grupo_table.js")
    tpl = read("produtos/templates/produtos/includes/vendas_grupo_tabela.html")
    util = read("produtos/relatorios_vendas_util.py")
    check("def period_table_from_rows" in vgt and "comparativo_col_defs" in vgt, "vgt builders")
    check("data-vg-sort-key" in tpl and "data-vg-col-toggle" in tpl, "tpl sort/col attrs")
    check("presetVendaMargem" in js and "margem_rs: true" in js, "js preset venda+margem")
    check('"ord_dir"' in util and "def get_item" in read("produtos/templatetags/agro_exibir.py"), "ord_dir + get_item")


def check_http() -> None:
    print("--- http ---")
    with override_settings(ALLOWED_HOSTS=["*"]):
        c = Client()
        r = c.get(reverse("relatorios_vendas_grupo"))
        check(r.status_code == 200, "GET periodo")
        html = r.content.decode()
        for needle in (
            "vg-table-root",
            "vg-col-picker",
            "data-vg-preset-venda-margem",
            "Venda R$",
            "Margem R$",
            "Lucro bruto %",
            "vendas_grupo_table.js",
        ):
            check(needle in html, f"html {needle[:24]}")
        r2 = c.get(reverse("relatorios_vendas_grupo") + "?visao=comparativo&n_meses=2")
        check(r2.status_code == 200 and "data-vg-group-toggle" in r2.content.decode(), "GET comparativo picker")
        r3 = c.get(reverse("relatorios_vendas_grupo") + "?ordenar=margem_pct&ord_dir=asc")
        check(r3.status_code == 200, "GET ord_dir asc")


def check_unit() -> None:
    print("--- django ---")
    proc = subprocess.run(
        [sys.executable, "manage.py", "test", "produtos.tests_relatorios_central_filtros", "-v0"],
        cwd=ROOT,
        capture_output=True,
        text=True,
    )
    check(proc.returncode == 0, "tests_relatorios_central_filtros")
    if proc.returncode != 0:
        print(proc.stdout[-1500:])
        print(proc.stderr[-1500:])


def main() -> int:
    check_files()
    check_http()
    check_unit()
    print(f"\n=== {OKS} OK, {len(FAILS)} FAIL ===")
    for f in FAILS:
        print(" -", f)
    return 1 if FAILS else 0


if __name__ == "__main__":
    raise SystemExit(main())
