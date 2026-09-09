"""Prova path ETQ-COLAR-MV — colar códigos + mais vendidos etiquetas."""
from __future__ import annotations

import ast
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ok = 0
fail = 0


def check(cond: bool, msg: str) -> None:
    global ok, fail
    if cond:
        ok += 1
        print(f"OK  {msg}")
    else:
        fail += 1
        print(f"FAIL {msg}")


def main() -> int:
    util = (ROOT / "produtos" / "etiquetas_fila_util.py").read_text(encoding="utf-8")
    check("def extrair_tokens_codigos" in util, "util extrair_tokens_codigos")
    check("def resolver_produtos_por_codigos" in util, "util resolver_produtos_por_codigos")
    check("def produtos_mais_vendidos_para_etiquetas" in util, "util produtos_mais_vendidos")

    # parse tokens without Django
    tree = ast.parse(util)
    ns: dict = {}
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.Assign, ast.AnnAssign)):
            if isinstance(node, ast.FunctionDef) and node.name == "extrair_tokens_codigos":
                # exec only helpers needed
                pass
    # Import via exec of constants + extrair only — simpler: run inline copy test via import
    sys.path.insert(0, str(ROOT))
    # Avoid Django setup: reimplement mini extract from source by importing module after django?
    # Static: call function by compiling the free functions that don't need Django
    code_extract = """
import re
_RE_SEP_CELULA = re.compile(r"[\\t|;,]+")
_RE_WS = re.compile(r"\\s+")
_SKIP_TOKENS = frozenset({"codigo", "código", "cod", "gm", "produto", "nome", "pos", "#", "qtd", "total"})
MAX_CODIGOS = 400
""" + re.search(
        r"def extrair_tokens_codigos\(texto: str\) -> list\[str\]:.*?(?=\ndef )",
        util,
        re.S,
    ).group(
        0
    )
    loc: dict = {}
    exec(code_extract, loc, loc)
    extrair = loc["extrair_tokens_codigos"]
    amostra = "GM1140\tseringa\nGM1195 veneno\n4680\n#\nGM1140\n"
    toks = extrair(amostra)
    check(toks == ["GM1140", "GM1195", "4680"], f"extrair ordem/dedupe {toks}")

    urls = (ROOT / "produtos" / "urls.py").read_text(encoding="utf-8")
    check("api_etiquetas_resolver_codigos" in urls, "url resolver-codigos")
    check("api_etiquetas_mais_vendidos" in urls, "url mais-vendidos")

    views = (ROOT / "produtos" / "views.py").read_text(encoding="utf-8")
    check("def api_etiquetas_resolver_codigos" in views, "view resolver")
    check("def api_etiquetas_mais_vendidos" in views, "view mais vendidos")
    check("api_etq_resolver_url" in views, "contexto template resolver")
    check("api_etq_mais_vendidos_url" in views, "contexto template mv")

    html = (ROOT / "produtos" / "templates" / "produtos" / "produtos_etiquetas.html").read_text(
        encoding="utf-8"
    )
    check("etq-btn-colar-codigos" in html, "botão colar códigos")
    check("etq-mv-carregar" in html, "botão carregar ranking")
    check("etq-colar-texto" in html, "textarea colar")
    check("resolverUrl" in html, "CFG resolverUrl")
    check("maisVendidosUrl" in html, "CFG maisVendidosUrl")

    js = (ROOT / "produtos" / "static" / "produtos" / "js" / "produtos_etiquetas.js").read_text(
        encoding="utf-8"
    )
    check("confirmarColarCodigos" in js, "JS confirmarColarCodigos")
    check("carregarMaisVendidos" in js, "JS carregarMaisVendidos")
    check("URL_RESOLVER" in js, "JS URL_RESOLVER")
    check("URL_MAIS_VENDIDOS" in js, "JS URL_MAIS_VENDIDOS")
    check("limit: 200" in js or "limit:200" in js, "JS ranking até 200")

    print(f"\nVERIFY {'OK' if fail == 0 else 'FAIL'} {ok}/{ok + fail}")
    return 0 if fail == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
