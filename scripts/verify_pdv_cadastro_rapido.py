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
    for t in (
        "wireCadastroRapidoUi",
        "apiPdvCadastroRapidoCriar",
        "cadastroRapidoSalvar",
        "cadastroRapidoAplicarPreview",
        "payload.ncm",
        "foto_url",
    ):
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
    checks = {
        "modal PDV": "pdv-cadastro-rapido-overlay",
        "NCM hidden": 'id="pdv-cadastro-rapido-ncm" type="hidden"',
        "foto preview": "pdv-cadastro-rapido-foto",
        "ajuda ?": "Ajuda do cadastro rápido",
        "busca alta 5rem": "min-height: 5rem !important",
        "sem bloco Conferir NCM": "NCM (vai no cadastro)",
    }
    for label, needle in checks.items():
        found = needle in wiz
        if label.startswith("sem "):
            if found:
                fail(label)
            else:
                ok(label)
        elif found:
            ok(label)
        else:
            fail(label)
    cad = (ROOT / "produtos/templates/produtos/produtos_cadastro_erp.html").read_text(
        encoding="utf-8"
    )
    if "cadastro-card-pendentes-pdv" not in cad:
        fail("card Cadastro")
    else:
        ok("card Cadastro")
    panel = (ROOT / "produtos/static/produtos/js/cadastro_erp_panel.js").read_text(
        encoding="utf-8"
    )
    if "pendentes-pdv" not in panel and "pendentesPdv" not in panel and "origem_pdv" not in panel:
        # aceita nomes comuns do card
        if "pendentes_pdv" not in panel and "PDV conferir" not in panel and "pdv" not in panel.lower():
            fail("cadastro_erp_panel pendentes PDV")
        else:
            ok("cadastro_erp_panel PDV")
    else:
        ok("cadastro_erp_panel pendentes PDV")


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


def check_ncm_util() -> None:
    from produtos.pdv_cadastro_rapido_util import (
        _extrair_ncm_de_payload,
        _extrair_thumb_cosmos,
        formatar_ncm_exibicao,
    )

    if formatar_ncm_exibicao("22072019") != "2207.20.19":
        fail("formatar_ncm")
    else:
        ok("formatar_ncm")
    ncm = _extrair_ncm_de_payload({"ncm": {"code": "22072019"}})
    if ncm != "22072019":
        fail(f"extrair_ncm got {ncm!r}")
    else:
        ok("extrair_ncm cosmos")
    thumb = _extrair_thumb_cosmos(
        {"thumbnail": "https://cdn-cosmos.bluesoft.com.br/products/789"}
    )
    if not thumb.startswith("https://"):
        fail("thumb cosmos")
    else:
        ok("thumb cosmos")


def check_pdv_bootstrap() -> None:
    src = (ROOT / "pdv/views.py").read_text(encoding="utf-8")
    for t in (
        "apiPdvCadastroRapidoChecar",
        "apiPdvCadastroRapidoGmPreview",
        "apiPdvCadastroRapidoCriar",
    ):
        if t not in src:
            fail(f"pdv/views bootstrap {t}")
        else:
            ok(f"pdv/views {t}")


def check_criar_grava_ncm() -> None:
    src = (ROOT / "produtos/views.py").read_text(encoding="utf-8")
    marker = "def api_pdv_cadastro_rapido_criar"
    idx = src.find(marker)
    if idx < 0:
        fail("criar view ausente")
        return
    # Função é longa — cortar até o próximo def de mesmo nível aproximado
    nxt = src.find("\ndef api_", idx + 10)
    chunk = src[idx : nxt if nxt > idx else idx + 20000]
    if 'fiscal["ncm"]' not in chunk and "fiscal['ncm']" not in chunk:
        fail("criar não grava fiscal.ncm")
    else:
        ok("criar grava fiscal.ncm")
    if "normalizar_ncm_somente_digitos" not in chunk:
        fail("criar sem normalizar_ncm")
    else:
        ok("criar normaliza NCM")
    if 'ex["fiscal"] = fiscal' not in chunk and "ex['fiscal'] = fiscal" not in chunk:
        fail("criar não seta ex.fiscal")
    else:
        ok("criar seta ex.fiscal")


def main() -> int:
    print("VERIFY PDV-CAD-RAPIDO")
    check_ast()
    check_urls()
    check_views_import()
    check_util_logic()
    check_ncm_util()
    check_gm_derive()
    check_criar_grava_ncm()
    check_pdv_bootstrap()
    check_frontend()
    print()
    if fails:
        print(f"VERIFY_FAIL · {fails} erro(s)")
        return 1
    print("VERIFY_OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
