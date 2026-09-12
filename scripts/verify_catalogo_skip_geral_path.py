"""VERIFY CATALOGO-SKIP-GERAL — path gestão/cadastro → árvore → vitrine → peso.

Cobre o bug: última categoria preenchida mostrava card sintético «Geral»
em vez de «Escolha o peso da embalagem».

Run: python3 scripts/verify_catalogo_skip_geral_path.py
"""
from __future__ import annotations

import ast
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
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
            fail(f"{label or rel}: falta `{n[:70]}`")
        else:
            ok(f"{label or rel}: `{n[:48]}`")


def must_not_contain(rel: str, needles: list[str], label: str = "") -> None:
    txt = read(rel)
    if not txt:
        return
    for n in needles:
        if n in txt:
            fail(f"{label or rel}: não deveria ter `{n[:70]}`")
        else:
            ok(f"{label or rel}: sem `{n[:40]}`")


def check_ast() -> None:
    for rel in (
        "produtos/catalogo_delivery_util.py",
        "produtos/views_catalogo_delivery.py",
        "scripts/verify_catalogo_skip_geral_path.py",
    ):
        try:
            ast.parse(read(rel))
            ok(f"AST {rel}")
        except SyntaxError as e:
            fail(f"AST {rel}: {e}")


def check_fonte() -> None:
    must_contain(
        "produtos/static/produtos/js/catalogo_delivery.js",
        [
            "function filhosReaisNo()",
            "function temProdutosNoNivelAtual()",
            "if (!filhosReaisNo().length)",
            "pathExact = temProdutosNoNivelAtual()",
            'slug === "_geral"',
            "function irParaPesosOuFilhos()",
            "function voltarPesos()",
            "function mostrarPesos",
        ],
        "JS",
    )
    js = read("produtos/static/produtos/js/catalogo_delivery.js")
    chunk = js.split("function irParaPesosOuFilhos()")[1].split("function abrirNivel")[0]
    if "filhosReaisNo().length" not in chunk:
        fail("irParaPesosOuFilhos sem gate filhosReaisNo")
    else:
        ok("irParaPesosOuFilhos gate filhos reais")
    if 'nome: "Geral"' in chunk:
        fail("irParaPesosOuFilhos ainda injeta Geral")
    else:
        ok("irParaPesosOuFilhos não injeta Geral")

    voltar = js.split("function voltarPesos()")[1].split("function voltarGrade()")[0]
    if "filhosReaisNo().length" not in voltar:
        fail("voltarPesos sem ramo folha (pop + irParaPesosOuFilhos)")
    else:
        ok("voltarPesos trata folha sem Geral")

    must_contain(
        "produtos/templates/produtos/catalogo/catalogo_delivery.html",
        [
            "view-pesos",
            "grade-pesos",
            "btn-voltar-pesos",
            "Escolha o peso da embalagem",
            "catalogo_delivery.js",
            "agro_asset_v",
            "arvore_json",
            "pesos_grade_json",
        ],
        "html",
    )
    html = read("produtos/templates/produtos/catalogo/catalogo_delivery.html")
    if "catalogo_delivery.js" in html and "?v=" not in html:
        fail("html JS sem cache-bust ?v=")
    else:
        ok("html cache-bust JS")

    must_contain(
        "produtos/catalogo_delivery_util.py",
        [
            "def arvore_navegacao_catalogo",
            "qtd_exata",
            "def path_slugs_do_item",
            "PESOS_GRADE_CATALOGO",
        ],
        "util",
    )
    must_contain(
        "produtos/views_catalogo_delivery.py",
        ["arvore_navegacao_catalogo", "pesos_grade_json", "arvore_json"],
        "views",
    )
    must_contain(
        "produtos/templates/produtos/catalogo/_card_produto.html",
        ["data-path", "data-pesos"],
        "card",
    )


def path_slugs_do_item(item: dict) -> list[str]:
    path: list[str] = []
    for key in (
        "categoria_slug",
        "subcategoria_slug",
        "subcategoria2_slug",
        "subcategoria3_slug",
        "subcategoria4_slug",
    ):
        s = (item.get(key) or "").strip()
        if not s:
            break
        path.append(s)
    return path or ["_sem"]


def arvore_navegacao_pura(cats: list[dict], itens: list[dict]) -> list[dict]:
    """Espelho de arvore_navegacao_catalogo sem Django/DB."""
    paths = [path_slugs_do_item(it) if "path_slugs" not in it else list(it["path_slugs"]) for it in itens]
    # Prefer explicit path_slugs; fallback to slug fields
    paths2 = []
    for it, p in zip(itens, paths):
        if it.get("path_slugs"):
            paths2.append(list(it["path_slugs"]))
        elif it.get("path"):
            paths2.append(str(it["path"]).split("/"))
        else:
            paths2.append(p)
    paths = paths2

    def _conta_prefixo(prefix: tuple[str, ...]) -> int:
        n = 0
        plen = len(prefix)
        for p in paths:
            if len(p) >= plen and tuple(p[:plen]) == prefix:
                n += 1
        return n

    def _conta_exata(prefix: tuple[str, ...]) -> int:
        n = 0
        for p in paths:
            if tuple(p) == prefix:
                n += 1
        return n

    def _montar_no(node: dict, prefix: tuple[str, ...]) -> dict:
        slug = node["slug"]
        aqui = prefix + (slug,)
        filhos_out = []
        for f in node.get("filhos") or []:
            filhos_out.append(_montar_no(f, aqui))
        conhecidos = {x["slug"] for x in filhos_out}
        extras: dict[str, int] = {}
        plen = len(aqui)
        for p in paths:
            if len(p) > plen and tuple(p[:plen]) == aqui:
                nxt = p[plen]
                if nxt not in conhecidos:
                    extras[nxt] = extras.get(nxt, 0) + 1
        for slug_e, q in sorted(extras.items()):
            filhos_out.append(
                {
                    "id": 0,
                    "slug": slug_e,
                    "nome": slug_e,
                    "qtd": q,
                    "qtd_exata": _conta_exata(aqui + (slug_e,)),
                    "filhos": [],
                }
            )
        return {
            "id": node.get("id") or 0,
            "slug": slug,
            "nome": node["nome"],
            "qtd": _conta_prefixo(aqui),
            "qtd_exata": _conta_exata(aqui),
            "filhos": filhos_out,
        }

    return [_montar_no(c, ()) for c in cats]


def check_arvore_loja() -> None:
    cats = [
        {
            "id": 1,
            "slug": "cao",
            "nome": "Cão",
            "filhos": [
                {
                    "id": 2,
                    "slug": "adulto",
                    "nome": "Adulto",
                    "filhos": [
                        {
                            "id": 3,
                            "slug": "racas-medias-e-grandes",
                            "nome": "Raças Médias e Grandes",
                            "filhos": [],
                        }
                    ],
                }
            ],
        }
    ]
    itens = [
        {
            "path_slugs": ["cao", "adulto", "racas-medias-e-grandes"],
            "categoria_slug": "cao",
            "subcategoria_slug": "adulto",
            "subcategoria2_slug": "racas-medias-e-grandes",
        }
        for _ in range(14)
    ]
    tree = arvore_navegacao_pura(cats, itens)
    if not tree:
        fail("árvore vazia")
        return
    cao = tree[0]
    if cao["qtd"] != 14 or cao["qtd_exata"] != 0:
        fail(f"Cão qtd={cao['qtd']} exata={cao['qtd_exata']}")
    else:
        ok("Cão qtd=14 qtd_exata=0")
    adulto = cao["filhos"][0]
    if adulto["qtd"] != 14 or adulto["qtd_exata"] != 0:
        fail(f"Adulto qtd={adulto['qtd']} exata={adulto['qtd_exata']}")
    else:
        ok("Adulto qtd=14 qtd_exata=0")
    racas = adulto["filhos"][0]
    if racas["qtd"] != 14 or racas["qtd_exata"] != 14:
        fail(f"Raças qtd={racas['qtd']} exata={racas['qtd_exata']}")
    else:
        ok("Raças qtd=14 qtd_exata=14 (folha)")
    if racas["filhos"]:
        fail(f"Raças não deveria ter filhos: {racas['filhos']}")
    else:
        ok("Raças sem filhos reais (Geral só existiria no JS antigo)")

    # Produto extra mais fundo → vira filho real, não some no skip
    itens2 = itens + [
        {
            "path_slugs": ["cao", "adulto", "racas-medias-e-grandes", "senior"],
            "categoria_slug": "cao",
            "subcategoria_slug": "adulto",
            "subcategoria2_slug": "racas-medias-e-grandes",
            "subcategoria3_slug": "senior",
        }
    ]
    tree2 = arvore_navegacao_pura(cats, itens2)
    racas2 = tree2[0]["filhos"][0]["filhos"][0]
    slugs = [f["slug"] for f in racas2["filhos"]]
    if slugs != ["senior"]:
        fail(f"extra slug deveria ser filho real: {slugs}")
    else:
        ok("slug extra (senior) vira filho real da árvore")


def check_path_slugs() -> None:
    p = path_slugs_do_item(
        {
            "categoria_slug": "cao",
            "subcategoria_slug": "adulto",
            "subcategoria2_slug": "racas-medias-e-grandes",
            "subcategoria3_slug": "",
            "subcategoria4_slug": "nao-deve-entrar",
        }
    )
    if p != ["cao", "adulto", "racas-medias-e-grandes"]:
        fail(f"path_slugs parou mal: {p}")
    else:
        ok("path_slugs para no 1º vazio (3 níveis)")
    if path_slugs_do_item({}) != ["_sem"]:
        fail("vazio deveria ser _sem")
    else:
        ok("sem categoria → _sem")


def check_js_runtime() -> None:
    r = subprocess.run(
        ["node", "--check", str(ROOT / "produtos/static/produtos/js/catalogo_delivery.js")],
        capture_output=True,
        text=True,
    )
    if r.returncode != 0:
        fail(f"node --check: {r.stderr.strip()}")
    else:
        ok("node --check catalogo_delivery.js")
    r2 = subprocess.run(
        ["node", str(ROOT / "scripts/verify_catalogo_skip_geral.js")],
        capture_output=True,
        text=True,
    )
    sys.stdout.write(r2.stdout)
    if r2.returncode != 0:
        fail("FSM JS VERIFY_FAIL")
        if r2.stderr:
            print(r2.stderr)
    else:
        ok("FSM JS VERIFY_OK")


def check_banana() -> None:
    txt = read("banana.md")
    if "CATALOGO-SKIP-GERAL" not in txt:
        fail("banana.md sem CATALOGO-SKIP-GERAL")
    else:
        ok("banana.md CATALOGO-SKIP-GERAL")
    bloco = txt.split("CATALOGO-SKIP-GERAL", 1)[-1][:800]
    if "pronto para envio" not in bloco and "enviado / Live" not in bloco:
        fail("banana.md CATALOGO-SKIP-GERAL sem status de fechamento")
    else:
        ok("banana.md CATALOGO-SKIP-GERAL com status de fechamento")


def check_version() -> None:
    v = read("VERSION").strip()
    try:
        major, minor = v.split(".", 1)
        ok_ver = int(major) > 17 or (int(major) == 17 and int(minor) >= 77)
    except ValueError:
        ok_ver = False
    if not ok_ver:
        fail(f"VERSION={v} (esperado >= 17.77)")
    else:
        ok(f"VERSION {v} (>=17.77)")


def main() -> None:
    print("=== VERIFY CATALOGO-SKIP-GERAL PATH ===")
    check_ast()
    check_fonte()
    check_path_slugs()
    check_arvore_loja()
    check_js_runtime()
    check_banana()
    check_version()
    print("---")
    if fails:
        print(f"VERIFY_FAIL ({fails})")
        sys.exit(1)
    print("VERIFY_OK")
    sys.exit(0)


if __name__ == "__main__":
    main()
