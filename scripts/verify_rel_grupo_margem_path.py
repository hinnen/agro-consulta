# -*- coding: utf-8 -*-
"""REL-GRUPO-MARGEM-COMP — margem, filtros investimento, comparativo mensal em Vendas por grupo."""
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


def check_static() -> None:
    print("--- static ---")
    util = read("produtos/relatorios_vendas_util.py")
    v = read("produtos/relatorios_central_views.py")
    tpl = read("produtos/templates/produtos/relatorios_generico.html")
    tests = read("produtos/tests_relatorios_central_filtros.py")
    for nome in (
        "parse_vendas_grupo_request",
        "meses_comparativo_grupo",
        "vendas_grupo_comparativo_meses",
        "_finalize_grupo_rows",
    ):
        check(f"def {nome}" in util, f"util {nome}")
    check("def relatorios_vendas_grupo" in v and 'visao == "comparativo"' in v, "view comparativo")
    check('name="visao"' in tpl and "Comparativo mensal" in tpl, "tpl visao")
    check("row_alertas" in tpl and "aviso_sem_custo" in tpl, "tpl alertas")
    check("test_vendas_grupo_margem_ordenacao_e_filtros" in tests, "test margem")
    check("test_vendas_grupo_comparativo_agrega_variacao" in tests, "test comparativo")


def check_http() -> None:
    print("--- http ---")
    with override_settings(ALLOWED_HOSTS=["*"]):
        c = Client()
        for qs in ("", "?visao=comparativo&n_meses=2", "?ordenar=margem_rs&export=xlsx"):
            r = c.get(reverse("relatorios_vendas_grupo") + qs)
            check(r.status_code == 200, f"GET vendas-grupo {qs or 'default'}")
            if "export=xlsx" in qs:
                check("spreadsheetml" in r["Content-Type"], "xlsx content-type")


def check_unit_tests() -> None:
    print("--- django tests ---")
    proc = subprocess.run(
        [
            sys.executable,
            "manage.py",
            "test",
            "produtos.tests_relatorios_central_filtros",
            "-v1",
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
    )
    check(proc.returncode == 0, "tests_relatorios_central_filtros")
    if proc.returncode != 0:
        print(proc.stdout[-2000:])
        print(proc.stderr[-2000:])


def main() -> int:
    check_static()
    check_http()
    check_unit_tests()
    print(f"\n=== {OKS} OK, {len(FAILS)} FAIL ===")
    for f in FAILS:
        print(" -", f)
    return 1 if FAILS else 0


if __name__ == "__main__":
    raise SystemExit(main())
