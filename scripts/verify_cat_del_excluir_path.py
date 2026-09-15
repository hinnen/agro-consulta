"""VERIFY CAT-DEL-EXCLUIR — excluir categoria Delivery/catálogo.

Run: python scripts/verify_cat_del_excluir_path.py
"""
from __future__ import annotations

import ast
import os
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

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


def read(rel: str) -> str:
    p = ROOT / rel
    if not p.is_file():
        fail(f"ausente {rel}")
        return ""
    return p.read_text(encoding="utf-8", errors="replace")


def must_contain(rel: str, needles: list[str], label: str = "") -> None:
    txt = read(rel)
    if not txt:
        return
    for n in needles:
        if n not in txt:
            fail(f"{label or rel}: falta `{n[:60]}`")
        else:
            ok(f"{label or rel}: `{n[:48]}`")


def check_ast() -> None:
    for rel in (
        "produtos/catalogo_delivery_util.py",
        "produtos/views_catalogo_delivery.py",
        "produtos/tests_catalogo_excluir_categoria.py",
    ):
        try:
            ast.parse(read(rel))
            ok(f"AST {rel}")
        except SyntaxError as e:
            fail(f"AST {rel}: {e}")


def check_fonte() -> None:
    must_contain(
        "produtos/catalogo_delivery_util.py",
        [
            "def excluir_categoria_catalogo",
            "def ids_subarvore_categoria",
            "def limpar_refs_delivery_categorias",
            "categoria_id",
            "subcategoria4_id",
        ],
        "util",
    )
    must_contain(
        "produtos/views_catalogo_delivery.py",
        [
            "def api_catalogo_categoria_excluir",
            "excluir_categoria_catalogo",
            "acao == \"excluir_categoria\"",
            "msg == \"cat_del\"",
        ],
        "views",
    )
    must_contain(
        "produtos/urls.py",
        [
            "catalogo/api/categorias/excluir/",
            "api_catalogo_categoria_excluir",
        ],
        "urls",
    )


def check_ui() -> None:
    must_contain(
        "produtos/templates/produtos/_modal_editar_produto_cadastro_erp.inc.html",
        [
            "btn-delivery-excluir-cat",
            "btn-delivery-excluir-sub",
            "btn-delivery-excluir-sub2",
            "btn-delivery-excluir-sub3",
            "btn-delivery-excluir-sub4",
            "excluirDeliveryCategoriaInline",
            "/catalogo/api/categorias/excluir/",
            "bindExcluir",
        ],
        "modal Delivery",
    )
    must_contain(
        "produtos/templates/produtos/catalogo/catalogo_gestao.html",
        [
            'value="excluir_categoria"',
            "Excluir",
            "níveis abaixo",
        ],
        "gestão",
    )
    must_contain(
        "produtos/templates/produtos/catalogo/_gestao_cat_filho.html",
        ['value="excluir_categoria"', ">X</button>"],
        "filho",
    )


def check_escopo() -> None:
    caixa = read("produtos/caixa_util.py")
    if "excluir_categoria_catalogo" in caixa or "api_catalogo_categoria_excluir" in caixa:
        fail("caixa toca exclusão de categoria")
    else:
        ok("caixa sem exclusão catálogo")
    pdv = read("produtos/static/produtos/js/pdv_wizard.js")
    if "categorias/excluir" in pdv:
        fail("pdv_wizard toca excluir categoria")
    else:
        ok("pdv_wizard sem excluir categoria")


def check_logica() -> None:
    import django

    django.setup()
    from django.urls import reverse

    from produtos.catalogo_delivery_util import (
        excluir_categoria_catalogo,
        ids_subarvore_categoria,
        limpar_refs_delivery_categorias,
    )

    url = reverse("api_catalogo_categoria_excluir")
    if url != "/catalogo/api/categorias/excluir/":
        fail(f"URL errada: {url}")
    else:
        ok(f"URL {url}")

    with patch("produtos.catalogo_delivery_util.CatalogoDeliveryCategoria") as Cat:
        rows = [
            MagicMock(pk=1, parent_id=None),
            MagicMock(pk=2, parent_id=1),
            MagicMock(pk=3, parent_id=2),
        ]
        Cat.objects.all.return_value.only.return_value = rows
        got = ids_subarvore_categoria(1)
        if got != {1, 2, 3}:
            fail(f"subarvore {got}")
        else:
            ok("ids_subarvore 1→2→3")

    with patch("produtos.catalogo_delivery_util.ProdutoGestaoOverlayAgro") as Ov:
        ov = MagicMock()
        ov.cadastro_extras = {
            "delivery": {
                "ativo": True,
                "titulo": "X",
                "descricao": "",
                "ordem": 0,
                "destaque": False,
                "permitir_estoque_negativo": False,
                "peso_texto": "",
                "imagem_base64": "",
                "imagem_mime": "image/jpeg",
                "categoria_id": 5,
                "subcategoria_id": 0,
                "subcategoria2_id": 0,
                "subcategoria3_id": 0,
                "subcategoria4_id": 0,
                "embalagens": [],
            }
        }
        Ov.objects.iterator.return_value = [ov]
        n = limpar_refs_delivery_categorias({5})
        if n != 1:
            fail(f"limpar_refs n={n}")
        else:
            ok("limpar_refs zera categoria_id")
        d = ov.cadastro_extras.get("delivery") or {}
        if int(d.get("categoria_id") or 0) != 0:
            fail("categoria_id não zerou")
        else:
            ok("categoria_id == 0 após limpar")

    with patch("produtos.catalogo_delivery_util.CatalogoDeliveryCategoria") as Cat:
        with patch("produtos.catalogo_delivery_util.limpar_refs_delivery_categorias", return_value=2):
            with patch(
                "produtos.catalogo_delivery_util.ids_subarvore_categoria",
                return_value={7, 8},
            ):
                cat = MagicMock()
                cat.nome = "Errada"
                cat.pk = 7
                Cat.objects.filter.return_value.first.return_value = cat
                Cat.objects.filter.return_value.count.return_value = 1
                res = excluir_categoria_catalogo(7)
                if not res.get("ok") or res.get("nome") != "Errada":
                    fail(f"excluir resumo {res}")
                else:
                    ok("excluir_categoria_catalogo resumo")
                cat.delete.assert_called_once()


def main() -> int:
    print("=== VERIFY CAT-DEL-EXCLUIR ===")
    check_ast()
    check_fonte()
    check_ui()
    check_escopo()
    check_logica()
    print("---")
    if fails:
        print(f"VERIFY_FAIL {fails}")
        return 1
    print("VERIFY_OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
