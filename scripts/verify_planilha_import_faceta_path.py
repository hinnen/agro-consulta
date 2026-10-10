"""VERIFY PLANILHA-IMPORT-FACETA — Excel ↑ permitir novos (marca/cat/sub/unidade).

Run: python scripts/verify_planilha_import_faceta_path.py
"""
from __future__ import annotations

import ast
import inspect
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

fails = 0

PACOTE_FILES = (
    "produtos/cadastro_planilha_util.py",
    "produtos/views.py",
    "produtos/static/produtos/js/cadastro_erp_panel.js",
    "produtos/templates/produtos/produtos_cadastro_erp.html",
    "produtos/tests_cadastro_planilha_cols.py",
    "scripts/verify_planilha_import_faceta_path.py",
)


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
    for rel in PACOTE_FILES:
        if rel.endswith(".py"):
            try:
                ast.parse(read(rel))
                ok(f"AST {rel}")
            except SyntaxError as e:
                fail(f"AST {rel}: {e}")


def check_fonte() -> None:
    must_contain(
        "produtos/cadastro_planilha_util.py",
        [
            "def carregar_facetas_planilha",
            "def _resolver_facetas_no_patch",
            "def _resumir_eventos_faceta",
            "permitir_novos: bool = False",
            "n_bloqueadas_valor_novo",
            "n_valores_novos",
            "permitir_novos_padrao",
            "Marque «Permitir criar novos»",
        ],
        "util",
    )
    must_contain(
        "produtos/views.py",
        [
            'request.POST.get("permitir_novos")',
            "permitir_novos=permitir_novos",
            "def api_produtos_cadastro_import_aplicar",
        ],
        "views",
    )


def check_ui() -> None:
    must_contain(
        "produtos/templates/produtos/produtos_cadastro_erp.html",
        [
            'id="cadastro-import-permitir-novos"',
            "Permitir criar novos",
            'id="cadastro-import-novos"',
        ],
        "modal import",
    )
    must_contain(
        "produtos/static/produtos/js/cadastro_erp_panel.js",
        [
            "cadastro-import-permitir-novos",
            "permitirNovosMarcado",
            "podeConfirmarImportacao",
            "n_bloqueadas_valor_novo",
            "valores_novos",
            "fd.append('permitir_novos', '1')",
        ],
        "JS import",
    )


def check_escopo() -> None:
    for rel in ("produtos/caixa_util.py", "produtos/static/produtos/js/pdv_wizard.js"):
        txt = read(rel)
        if "permitir_novos" in txt or "_resolver_facetas_no_patch" in txt:
            fail(f"{rel} toca import faceta")
        else:
            ok(f"{rel} sem import faceta")
    util = read("produtos/cadastro_planilha_util.py")
    for k in (
        "COL_DEL_ATIVO",
        "DELIVERY_IMPORT_KEYS",
        "_aplicar_patch_delivery",
    ):
        if k not in util:
            fail(f"util perdeu delivery: {k}")
        else:
            ok(f"util mantém delivery {k}")


def check_logica() -> None:
    import django

    django.setup()
    from produtos.cadastro_planilha_util import (
        COL_CATEGORIA,
        COL_MARCA,
        _resolver_facetas_no_patch,
        aplicar_importacao_cadastro,
        preview_importacao_cadastro,
    )

    sig = inspect.signature(aplicar_importacao_cadastro)
    if "permitir_novos" not in sig.parameters:
        fail("aplicar_importacao_cadastro sem permitir_novos")
    else:
        ok("aplicar_importacao_cadastro aceita permitir_novos")

    facetas = {COL_CATEGORIA: ["Racoes", "Medicamentos"], COL_MARCA: ["AKILES"]}
    canonicos = {
        COL_CATEGORIA: {"racoes": "Racoes", "medicamentos": "Medicamentos"},
        COL_MARCA: {"akiles": "AKILES"},
    }
    patch = {COL_CATEGORIA: "Hortifruti"}
    out, evs, erros = _resolver_facetas_no_patch(
        patch, facetas, canonicos, permitir_novos=False, linha=3
    )
    if not erros:
        fail("deveria bloquear categoria nova sem flag")
    else:
        ok("bloqueia categoria nova sem flag")
    out2, evs2, erros2 = _resolver_facetas_no_patch(
        patch, facetas, canonicos, permitir_novos=True, linha=3
    )
    if erros2 or out2.get(COL_CATEGORIA) != "Hortifruti":
        fail(f"deveria aceitar com flag: erros={erros2} out={out2}")
    else:
        ok("aceita categoria nova com flag")

    # typo → corrige automaticamente
    patch_typo = {COL_MARCA: "akiles"}
    out3, evs3, erros3 = _resolver_facetas_no_patch(
        patch_typo, facetas, canonicos, permitir_novos=False, linha=4
    )
    if erros3 or out3.get(COL_MARCA) != "AKILES":
        fail(f"typo não corrigiu: {out3} erros={erros3}")
    else:
        ok("corrige typo de marca")

    prev_src = inspect.getsource(preview_importacao_cadastro)
    if "_resolver_facetas_no_patch" not in prev_src or "n_valores_novos" not in prev_src:
        fail("preview sem facetas/novos")
    else:
        ok("preview integra facetas")


def main() -> int:
    print("=== VERIFY PLANILHA-IMPORT-FACETA ===")
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
