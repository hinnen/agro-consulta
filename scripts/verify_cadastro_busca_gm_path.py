# -*- coding: utf-8 -*-
"""Prova — CADASTRO-BUSCA-GM-PDV: GM com hífen + alinhamento filtro PDV.

  .venv/bin/python scripts/verify_cadastro_busca_gm_path.py
"""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

fails: list[str] = []
oks: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    if cond:
        oks.append(name)
        print(f"  OK  {name}" + (f" — {detail}" if detail else ""))
    else:
        fails.append(name)
        print(f"  FAIL {name}" + (f" — {detail}" if detail else ""))


def test_arquivos() -> None:
    print("== Contratos ==")
    util = (ROOT / "produtos/cadastro_busca_codigo_util.py").read_text(encoding="utf-8")
    cat = (ROOT / "produtos/catalogo_agro.py").read_text(encoding="utf-8")
    views = (ROOT / "produtos/views.py").read_text(encoding="utf-8")
    motor = (ROOT / "produtos/motor_busca_unificado_util.py").read_text(encoding="utf-8")
    pdv = (ROOT / "produtos/busca_filtro_pdv_util.py").read_text(encoding="utf-8")
    js = (ROOT / "produtos/static/produtos/js/cadastro_erp_panel.js").read_text(encoding="utf-8")

    check("digitos_somente_barras_cadastro", "def digitos_somente_barras_cadastro" in util)
    check("termo_gm_etiqueta_completa", "def termo_gm_etiqueta_completa" in util)
    check("filtrar_rows_api", "filtrar_rows_api_estilo_pdv" in pdv)
    check("catalogo_filtra_ordenar", "filtrar_rows_api_estilo_pdv" in cat and "ordenar_rows_api_estilo_pdv" in cat)
    check("cadastro_sort_explicit", "_parse_sort_cadastro_request_explicit" in views)
    check("gm_estrito_sem_fallback", "return filtrados if filtrados else docs" not in motor)
    check("cadastro_complemento_mongo", "contexto_cadastro" in motor and "contexto_cadastro" in views)
    check("bca_cache_v4", "BCA_BUSCA_CACHE_VERSION = 4" in (ROOT / "produtos/bca_busca_cache_util.py").read_text())
    views_txt = (ROOT / "produtos/views.py").read_text()
    check("api_filtra_rows_pdv", "filtrar_rows_api_estilo_pdv(res, q)" in views_txt)
    check("api_cadastro_contexto", "contexto=cadastro" in js and "fetchBuscaCadastroApi" in js)
    check("test_gm_variante", (ROOT / "produtos/tests_cadastro_busca_gm_variante.py").is_file())
    check("test_pdv_alinhada", (ROOT / "produtos/tests_cadastro_busca_pdv_alinhada.py").is_file())


def test_python() -> None:
    print("== Lógica ==")
    from produtos.cadastro_busca_codigo_util import (
        digitos_somente_barras_cadastro,
        q_codigo_exato_cadastro,
        termo_bate_codigos_produto,
        termo_gm_etiqueta_completa,
    )
    from produtos.busca_filtro_pdv_util import filtrar_rows_api_estilo_pdv

    check("gm_etiqueta", termo_gm_etiqueta_completa("GM0037-1"))
    check("sem_digitos_gm", digitos_somente_barras_cadastro("GM0037-1") is None)
    check("nao_bate_ivomec", not termo_bate_codigos_produto(
        "GM0037-1", codigo_nfe="GM1097-1", codigo_barras="00371"
    ))
    check("bate_exato", termo_bate_codigos_produto("GM0037-1", codigo_nfe="GM0037-1"))
    q = q_codigo_exato_cadastro("GM0037-1")
    check("q_ex_sem_barras", q is not None and "codigo_barras" not in str(q))
    rows = [
        {"id": "1", "nome": "bebedouro grande", "marca": ""},
        {"id": "2", "nome": "milho grande 47 kg", "marca": ""},
    ]
    out = filtrar_rows_api_estilo_pdv(rows, "milho grande")
    check("filtro_milho_grande", len(out) == 1 and out[0]["id"] == "2")


def test_django() -> None:
    print("== Django tests ==")
    r = subprocess.run(
        [
            str(ROOT / ".venv/bin/python"),
            "manage.py",
            "test",
            "produtos.tests_cadastro_busca_gm_variante",
            "produtos.tests_cadastro_busca_pdv_alinhada",
            "-v",
            "0",
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
    )
    check("unit_tests", r.returncode == 0, (r.stdout + r.stderr)[-200:])


def main() -> int:
    print("CADASTRO-BUSCA-GM-PDV — verificação")
    test_arquivos()
    test_python()
    test_django()
    print(f"\n{'OK' if not fails else 'FAIL'} — {len(oks)}/{len(oks)+len(fails)}")
    return 1 if fails else 0


if __name__ == "__main__":
    raise SystemExit(main())
