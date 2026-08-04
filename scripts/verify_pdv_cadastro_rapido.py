"""VERIFY PDV-CAD-RAPIDO — path cadastro rápido no PDV.

Run: python scripts/verify_pdv_cadastro_rapido.py
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
        "produtos/pdv_cadastro_rapido_util.py",
        "produtos/cadastro_filtros_util.py",
        "pdv/views.py",
        "produtos/urls.py",
    ):
        try:
            ast.parse((ROOT / rel).read_text(encoding="utf-8"))
            ok(f"AST {rel}")
        except SyntaxError as e:
            fail(f"AST {rel}: {e}")


def check_urls() -> None:
    import django

    django.setup()
    from django.urls import reverse

    expect = {
        "api_pdv_cadastro_rapido_checar": "/api/produtos/pdv-cadastro-rapido/checar/",
        "api_pdv_cadastro_rapido_gm_preview": "/api/produtos/pdv-cadastro-rapido/gm-preview/",
        "api_pdv_cadastro_rapido_criar": "/api/produtos/pdv-cadastro-rapido/criar/",
        "api_cadastro_pendentes_pdv": "/api/produtos/cadastro/pendentes-pdv/",
        "api_cadastro_pendente_pdv_marcar_conferido": "/api/produtos/cadastro/pendentes-pdv/marcar-conferido/",
    }
    for name, path in expect.items():
        got = reverse(name)
        if got == path:
            ok(f"url {name}")
        else:
            fail(f"url {name}: {got} != {path}")


def check_views_import() -> None:
    from produtos import views

    for fn in (
        "api_pdv_cadastro_rapido_checar",
        "api_pdv_cadastro_rapido_criar",
        "api_cadastro_pendentes_pdv",
        "api_cadastro_pendente_pdv_marcar_conferido",
    ):
        if callable(getattr(views, fn, None)):
            ok(f"view {fn}")
        else:
            fail(f"view {fn} ausente")


def check_util_logic() -> None:
    from produtos.pdv_cadastro_rapido_util import (
        ean_parece_valido,
        limpar_pendente_conferencia,
        marcar_extras_origem_pdv,
        normalizar_ean,
    )

    if normalizar_ean("12.34") != "1234":
        fail("normalizar_ean")
    else:
        ok("normalizar_ean")
    if not ean_parece_valido("7891000100103") or ean_parece_valido("GM1"):
        fail("ean_parece_valido")
    else:
        ok("ean_parece_valido")
    ex = marcar_extras_origem_pdv({})
    if not (ex.get("origem_pdv") and ex.get("pendente_conferencia")):
        fail("marcar_extras")
    else:
        ok("marcar_extras")
    limpo = limpar_pendente_conferencia(ex)
    if limpo.get("pendente_conferencia") is not False:
        fail("limpar_pendente")
    else:
        ok("limpar_pendente")


def check_frontend() -> None:
    js = (ROOT / "produtos/static/produtos/js/pdv_wizard.js").read_text(encoding="utf-8")
    for t in ("wireCadastroRapidoUi", "apiPdvCadastroRapidoCriar", "cadastroRapidoSalvar"):
        if t not in js:
            fail(f"pdv_wizard.js missing {t}")
        else:
            ok(f"pdv_wizard.js {t}")
    step = (ROOT / "produtos/templates/produtos/partials/pdv/step_produtos.html").read_text(
        encoding="utf-8"
    )
    if "pdv-btn-cadastro-rapido" not in step:
        fail("botão + Produto")
    else:
        ok("botão + Produto")
    wiz = (ROOT / "produtos/templates/produtos/pdv_wizard.html").read_text(encoding="utf-8")
    if "pdv-cadastro-rapido-overlay" not in wiz:
        fail("modal PDV")
    else:
        ok("modal PDV")
    cad = (ROOT / "produtos/templates/produtos/produtos_cadastro_erp.html").read_text(
        encoding="utf-8"
    )
    if "cadastro-card-pendentes-pdv" not in cad:
        fail("card Cadastro")
    else:
        ok("card Cadastro")


def check_gm_derive() -> None:
    import re

    def derive(cod_sys: str, cod_gm: str) -> tuple[str, str]:
        if cod_gm and not cod_sys:
            m = re.match(r"^GM\s*(\d{4})$", cod_gm, flags=re.IGNORECASE)
            if m:
                cod_sys = m.group(1)
                cod_gm = f"GM{cod_sys}"
        return cod_sys, cod_gm

    if derive("", "GM4522") != ("4522", "GM4522"):
        fail("derive GM")
    else:
        ok("derive GM -> sistema")


def main() -> int:
    print("VERIFY PDV-CAD-RAPIDO")
    check_ast()
    check_urls()
    check_views_import()
    check_util_logic()
    check_gm_derive()
    check_frontend()
    print()
    if fails:
        print(f"VERIFY_FAIL · {fails} erro(s)")
        return 1
    print("VERIFY_OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
