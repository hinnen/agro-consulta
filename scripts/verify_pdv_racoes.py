"""VERIFY PDV-RACOES — botão Rações no PDV wizard.

Run: python scripts/verify_pdv_racoes.py
"""
from __future__ import annotations

import ast
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

fails = 0


def ok(msg: str) -> None:
    print(f"  OK  {msg}")


def fail(msg: str) -> None:
    global fails
    fails += 1
    print(f" FAIL {msg}")


def check_ast() -> None:
    for rel in (
        "produtos/pdv_racoes_util.py",
        "produtos/tests_pdv_racoes.py",
        "produtos/static/produtos/js/pdv_wizard.js",
    ):
        if rel.endswith(".js"):
            continue
        try:
            ast.parse((ROOT / rel).read_text(encoding="utf-8"))
            ok(f"AST {rel}")
        except SyntaxError as e:
            fail(f"AST {rel}: {e}")


def check_frontend() -> None:
    js = (ROOT / "produtos/static/produtos/js/pdv_wizard.js").read_text(encoding="utf-8")
    for t in (
        "wireRacoesUi",
        "pdvRacoesFiltrar",
        "pdvRacoesParsePeso",
        "pdv-btn-racoes",
        "pdvRacoesAdicionar",
        "pacote",
        "Pacote R$ 10",
    ):
        if t not in js:
            fail(f"pdv_wizard.js missing {t}")
        else:
            ok(f"pdv_wizard.js {t}")
    step = (ROOT / "produtos/templates/produtos/partials/pdv/step_produtos.html").read_text(
        encoding="utf-8"
    )
    if 'id="pdv-btn-racoes"' not in step:
        fail("botão Rações")
    else:
        ok("botão Rações")
    wiz = (ROOT / "produtos/templates/produtos/pdv_wizard.html").read_text(encoding="utf-8")
    for needle, label in (
        ('id="pdv-racoes-overlay"', "modal Rações"),
        ("pdv-btn-racoes", "css/botão Rações"),
        ("Gato filhote", "card gato filhote"),
        ("Cão Sênior", "card cão sênior"),
        ("pdv-racoes-todos-pesos", "todos os tamanhos btn"),
        ("Todas as marcas", "todas as marcas"),
        ("Todos os tamanhos", "todos os tamanhos"),
    ):
        if needle not in wiz:
            fail(label)
        else:
            ok(label)


def check_catalogo() -> None:
    views = (ROOT / "produtos/views.py").read_text(encoding="utf-8")
    chunk_start = views.find("def _catalogo_pdv_montar_produtos_somente_postgres")
    chunk = views[chunk_start : chunk_start + 4500] if chunk_start >= 0 else ""
    if '"subcategoria_2"' not in chunk or '"peso_etiqueta"' not in chunk:
        fail("catálogo PG sem sub2/peso")
    else:
        ok("catálogo PG sub2+peso")
    if 'CATALOGO_PDV_CACHE_ENTRY_KEY = "pdv_catalogo_produtos_por_dia_v3"' not in views:
        fail("cache catálogo v3")
    else:
        ok("cache catálogo v3")
    slim = (ROOT / "produtos/catalogo_agro.py").read_text(encoding="utf-8")
    slim_fn = slim.find("def listar_slim_rows_pdv")
    slim_chunk = slim[slim_fn : slim_fn + 8000] if slim_fn >= 0 else ""
    if '"subcategoria_2"' not in slim_chunk or '"peso_etiqueta"' not in slim_chunk:
        fail("slim sem sub2/peso")
    else:
        ok("slim sub2+peso")


def check_util() -> None:
    from produtos.pdv_racoes_util import filtrar_racoes, parse_peso_racoes, tipo_racoes_por_id

    if parse_peso_racoes("10 kg") != "kg:10":
        fail("parse 10 kg")
    else:
        ok("parse 10 kg")
    tipo = tipo_racoes_por_id("gato_castrado")
    rows = [
        {
            "id": "1",
            "categoria": "Rações",
            "subcategoria": "Gato",
            "subcategoria_2": "Castrado",
            "marca": "X",
            "peso_etiqueta": "1",
        }
    ]
    if len(filtrar_racoes(rows, tipo, peso_key="kg:1")) != 1:
        fail("filtrar gato castrado granel")
    else:
        ok("filtrar gato castrado granel")


def main() -> int:
    print("VERIFY PDV-RACOES")
    check_ast()
    check_frontend()
    check_catalogo()
    check_util()
    if fails:
        print(f"FAIL {fails}")
        return 1
    print("VERIFY_OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
