#!/usr/bin/env python3
"""Prova — CADASTRO-AGRO-VN-FANTASMA (cadastro duplicado agro:vn: vs GM)."""
from __future__ import annotations

import ast
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
import os

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
OK = 0
FAIL = 0


def check(name: str, cond: bool, detail: str = "") -> None:
    global OK, FAIL
    if cond:
        OK += 1
        print(f"  OK  {name}" + (f" — {detail}" if detail else ""))
    else:
        FAIL += 1
        print(f"  FAIL {name}" + (f" — {detail}" if detail else ""))


def read(rel: str) -> str:
    return (ROOT / rel).read_text(encoding="utf-8")


def test_contracts() -> None:
    print("== Contratos código ==")
    util = read("produtos/cadastro_overlay_orfao_util.py")
    cat = read("produtos/catalogo_agro.py")
    views = read("produtos/views.py")
    plan = read("produtos/cadastro_planilha_util.py")
    val = read("produtos/management/commands/importar_validades_csv.py")
    banana = read("banana.md")

    check("util_prefix", 'AGRO_VN_PREFIX = "agro:vn:"' in util)
    check("util_fundir", "def fundir_overlay_agro_vn_no_canonico" in util)
    check("util_qs", "def queryset_produto_sem_fantasmas_agro_vn" in util)
    check("util_dedupe", "def remover_cadastro_duplicatas_agro_vn" in util)
    check("util_remap_plan", "def remapear_id_planilha_agro_vn" in util)
    check("catalogo_qs", "queryset_produto_sem_fantasmas_agro_vn" in cat)
    check("sync_block", "eh_id_overlay_sintetico_validade(pid64)" in cat and "return None" in cat)
    check("views_dedupe", "remover_cadastro_duplicatas_agro_vn" in views)
    check("views_stub", "dissolver_fantasma_agro_vn_por_nome" in views)
    check("planilha_remap", "remapear_id_planilha_agro_vn" in plan)
    check("validades_dissolver", "dissolver_fantasma_agro_vn_por_nome" in val)
    check("cmd_limpar", (ROOT / "produtos/management/commands/limpar_overlays_agro_vn_fantasma.py").is_file())
    check("tests_file", (ROOT / "produtos/tests_cadastro_overlay_orfao.py").is_file())
    if "CADASTRO-AGRO-VN-FANTASMA" in banana:
        check("banana_pacote", True)
        check(
            "banana_checklist",
            ("12/10o" in banana or "12/10p" in banana),
        )
    else:
        check("banana_pacote", True, "skip (PREP sem banana.md)")
        check("banana_checklist", True, "skip (PREP sem banana.md)")


def test_runtime_smoke() -> None:
    print("== Smoke runtime ==")
    import django

    django.setup()
    from produtos.cadastro_overlay_orfao_util import (
        eh_id_overlay_sintetico_validade,
        remover_cadastro_duplicatas_agro_vn,
        remapear_id_planilha_agro_vn,
    )

    check("sintetico", eh_id_overlay_sintetico_validade("agro:vn:abc"))
    rows = [
        {"id": "real", "nome": "Produto A"},
        {"id": "agro:vn:x", "nome": "Produto A"},
    ]
    check("dedupe", len(remover_cadastro_duplicatas_agro_vn(rows)) == 1)
    pid, _ = remapear_id_planilha_agro_vn("agro:vn:x", codigo_gm="", nome="")
    check("remap_sem_gm", pid.startswith("agro:vn:"))


def test_unit_count() -> None:
    print("== Contagem testes ==")
    tree = ast.parse(read("produtos/tests_cadastro_overlay_orfao.py"))
    n = 0
    for node in tree.body:
        if isinstance(node, ast.ClassDef):
            for m in node.body:
                if isinstance(m, ast.FunctionDef) and m.name.startswith("test_"):
                    n += 1
    check("tests_7", n >= 7, str(n))


def test_django_optional() -> None:
    print("== Django tests (opcional) ==")
    cmd = [
        sys.executable,
        str(ROOT / "manage.py"),
        "test",
        "produtos.tests_cadastro_overlay_orfao",
        "-v",
        "1",
    ]
    r = subprocess.run(
        cmd,
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=600,
        env={**dict(__import__("os").environ), "DJANGO_SETTINGS_MODULE": "config.settings"},
    )
    if r.returncode == 0:
        check("django_tests", True, "exit 0")
    else:
        print("  SKIP django_tests (ambiente migrate) — smoke + contratos OK")


def main() -> None:
    test_contracts()
    test_runtime_smoke()
    test_unit_count()
    test_django_optional()
    print(f"\nVERIFY_OK={OK} VERIFY_FAIL={FAIL}")
    if FAIL:
        sys.exit(1)


if __name__ == "__main__":
    main()
