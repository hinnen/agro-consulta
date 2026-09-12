"""VERIFY CATALOGO-5N-PESO — path gestão → cadastro → vitrine → peso → carrinho.

Run: python scripts/verify_catalogo_5n_peso.py
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


def read(rel: str) -> str:
    p = ROOT / rel
    if not p.is_file():
        fail(f"ausente {rel}")
        return ""
    return p.read_text(encoding="utf-8")


def must_contain(rel: str, needles: list[str], label: str = "") -> None:
    txt = read(rel)
    if not txt:
        return
    for n in needles:
        if n not in txt:
            fail(f"{label or rel}: falta `{n}`")
        else:
            ok(f"{label or rel}: `{n[:48]}`")


def check_ast() -> None:
    for rel in (
        "produtos/catalogo_delivery_util.py",
        "produtos/views_catalogo_delivery.py",
    ):
        try:
            ast.parse(read(rel))
            ok(f"AST {rel}")
        except SyntaxError as e:
            fail(f"AST {rel}: {e}")


def check_backend() -> None:
    util = read("produtos/catalogo_delivery_util.py")
    views = read("produtos/views_catalogo_delivery.py")
    if "CATALOGO_MAX_NIVEIS = 5" not in util:
        fail("CATALOGO_MAX_NIVEIS != 5")
    else:
        ok("CATALOGO_MAX_NIVEIS = 5")
    for fld in ("subcategoria3_id", "subcategoria4_id", "subcategoria3_slug", "subcategoria4_slug"):
        if fld not in util:
            fail(f"util sem {fld}")
        else:
            ok(f"util {fld}")
    for fn in (
        "PESOS_GRADE_CATALOGO",
        "path_slugs_do_item",
        "pesos_keys_do_item",
        "profundidade_por_parent_id",
        "arvore_navegacao_catalogo",
    ):
        if f"def {fn}" not in util and f"{fn}:" not in util and f"{fn} =" not in util:
            # PESOS is list const
            if fn == "PESOS_GRADE_CATALOGO" and "PESOS_GRADE_CATALOGO" in util:
                ok(fn)
            elif f"def {fn}" in util:
                ok(fn)
            else:
                fail(f"util sem {fn}")
        else:
            ok(fn)
    if "profundidade_por_parent_id" not in views:
        fail("views sem profundidade_por_parent_id")
    else:
        ok("views profundidade_por_parent_id")
    if "Máximo 3 níveis" in views:
        fail("views ainda bloqueia com mensagem 3 níveis")
    else:
        ok("views sem teto 3")
    if "pesos_grade_json" not in views:
        fail("views sem pesos_grade_json no context")
    else:
        ok("views pesos_grade_json")
    if '"path"' not in views and "'path'" not in views:
        fail("catalogo_json sem path")
    else:
        ok("catalogo_json path")
    # agrupar flat
    if 'produtos_sem_sub' in util and 'def agrupar_itens_por_categoria' in util:
        chunk = util.split("def agrupar_itens_por_categoria")[1].split("def ")[0]
        if "produtos_sem_sub" in chunk and '"produtos"' not in chunk.replace("produtos_sem_sub", ""):
            # still ok if flat uses produtos key
            pass
        if '"produtos"' in chunk or "'produtos'" in chunk:
            ok("agrupar flat produtos")
        else:
            fail("agrupar sem lista flat produtos")


def check_runtime() -> None:
    import django

    django.setup()
    from produtos.catalogo_delivery_util import (
        CATALOGO_MAX_NIVEIS,
        PESOS_GRADE_CATALOGO,
        normalizar_delivery,
        path_slugs_do_item,
        pesos_keys_do_item,
        profundidade_por_parent_id,
        agrupar_itens_por_categoria,
    )
    from produtos.pdv_racoes_util import parse_peso_racoes as parse_pdv

    if CATALOGO_MAX_NIVEIS != 5:
        fail(f"runtime MAX={CATALOGO_MAX_NIVEIS}")
    else:
        ok("runtime MAX=5")
    keys = {g["key"] for g in PESOS_GRADE_CATALOGO}
    want = {"kg:1", "kg:2.5", "kg:5", "kg:10", "kg:15", "kg:20", "kg:25"}
    if keys != want:
        fail(f"grade pesos {keys}")
    else:
        ok("grade 7 pesos PDV")
    d = normalizar_delivery(
        {
            "ativo": True,
            "categoria_id": 1,
            "subcategoria_id": 2,
            "subcategoria2_id": 3,
            "subcategoria3_id": 4,
            "subcategoria4_id": 5,
            "peso_texto": "15 kg",
        }
    )
    for k, v in (
        ("categoria_id", 1),
        ("subcategoria_id", 2),
        ("subcategoria2_id", 3),
        ("subcategoria3_id", 4),
        ("subcategoria4_id", 5),
    ):
        if d.get(k) != v:
            fail(f"normalizar {k}={d.get(k)}")
        else:
            ok(f"normalizar {k}")
    item = {
        "categoria_slug": "caes",
        "subcategoria_slug": "adulto",
        "subcategoria2_slug": "premium",
        "subcategoria3_slug": "grande",
        "subcategoria4_slug": "senior",
        "peso_texto": "15 kg",
        "embalagens": [{"rotulo": "10 kg", "peso_texto": "10 kg"}],
    }
    path = path_slugs_do_item(item)
    if path != ["caes", "adulto", "premium", "grande", "senior"]:
        fail(f"path_slugs {path}")
    else:
        ok("path 5 slugs")
    pkeys = pesos_keys_do_item(item)
    if "kg:15" not in pkeys or "kg:10" not in pkeys:
        fail(f"peso_keys {pkeys}")
    else:
        ok("peso_keys 10+15")
    if parse_pdv("15 kg") != "kg:15":
        fail("parse_peso PDV 15 kg")
    else:
        ok("parse_peso alinhado PDV")
    # profundidade: None parent → nível 1
    if profundidade_por_parent_id(None) != 1:
        fail("profundidade root != 1")
    else:
        ok("profundidade root=1")
    # arvore recursiva com item 5 níveis
    itens = [
        {
            "categoria_slug": "caes",
            "categoria_nome": "Cães",
            "subcategoria_slug": "adulto",
            "subcategoria_nome": "Adulto",
            "subcategoria2_slug": "premium",
            "subcategoria2_nome": "Premium",
            "subcategoria3_slug": "grande",
            "subcategoria3_nome": "Grande",
            "subcategoria4_slug": "senior",
            "subcategoria4_nome": "Sênior",
            "path_slugs": ["caes", "adulto", "premium", "grande", "senior"],
            "path": "caes/adulto/premium/grande/senior",
            "peso_texto": "15 kg",
            "peso_keys": ["kg:15"],
            "nome": "Teste",
            "id": "1",
            "preco": 10,
            "destaque": False,
            "ordem": 0,
            "embalagens": [],
        }
    ]
    # arvore usa listar_categorias_arvore do DB — sem DB cats, tree may be empty + _sem
    # Test agrupar flat
    secoes = agrupar_itens_por_categoria(itens)
    if not secoes or "produtos" not in secoes[0]:
        fail(f"agrupar {secoes}")
    else:
        ok("agrupar flat")
        if len(secoes[0]["produtos"]) != 1:
            fail("agrupar qtd")
        else:
            ok("agrupar 1 produto")


def check_templates_js() -> None:
    must_contain(
        "produtos/templates/produtos/catalogo/catalogo_delivery.html",
        ["view-pesos", "grade-pesos", "btn-voltar-pesos", "pesos_grade_json", "secao.produtos"],
        "html vitrine",
    )
    must_contain(
        "produtos/templates/produtos/catalogo/_card_produto.html",
        ["data-path", "data-pesos"],
        "card",
    )
    must_contain(
        "produtos/templates/produtos/_modal_editar_produto_cadastro_erp.inc.html",
        [
            "edit-delivery-subcategoria3",
            "edit-delivery-subcategoria4",
            "btn-delivery-nova-sub3",
            "btn-delivery-nova-sub4",
            "subcategoria3_id",
            "subcategoria4_id",
        ],
        "cadastro",
    )
    must_contain(
        "produtos/templates/produtos/catalogo/catalogo_gestao.html",
        ["5 níveis", "_gestao_cat_filho.html"],
        "gestao",
    )
    js = read("produtos/static/produtos/js/catalogo_delivery.js")
    for n in (
        "pathStack",
        "pesoAtual",
        "view-pesos",
        "mostrarPesos",
        "abrirListaComPeso",
        "pesosDisponiveis",
        "pathExact",
        "aplicarFiltros",
        "filhosReaisNo",
        "irParaPesosOuFilhos",
    ):
        if n not in js:
            fail(f"JS sem {n}")
        else:
            ok(f"JS {n}")
    # Add não muda view
    if "addToCart" in js and "mostrarHome" in js:
        # addToCart should only touch carrinho/barra
        chunk = js.split("function addToCart")[1].split("function ")[0]
        if "mostrarHome" in chunk or "mostrarPesos" in chunk or "mostrarProdutos" in chunk:
            fail("addToCart muda de view")
        else:
            ok("addToCart não muda view")
    # node check
    import subprocess

    r = subprocess.run(
        ["node", "--check", str(ROOT / "produtos/static/produtos/js/catalogo_delivery.js")],
        capture_output=True,
        text=True,
    )
    if r.returncode != 0:
        fail(f"node --check: {r.stderr}")
    else:
        ok("node --check")


def check_urls() -> None:
    urls = read("produtos/urls.py")
    for n in ("catalogo_delivery", "catalogo_gestao", "api_catalogo_categoria_criar"):
        if n not in urls:
            fail(f"urls sem {n}")
        else:
            ok(f"url {n}")


def main() -> None:
    print("=== VERIFY CATALOGO-5N-PESO ===")
    check_ast()
    check_backend()
    check_urls()
    check_templates_js()
    try:
        check_runtime()
    except Exception as e:
        fail(f"runtime: {type(e).__name__}: {e}")
    print("---")
    if fails:
        print(f"VERIFY_FAIL ({fails})")
        sys.exit(1)
    print("VERIFY_OK")
    sys.exit(0)


if __name__ == "__main__":
    main()
