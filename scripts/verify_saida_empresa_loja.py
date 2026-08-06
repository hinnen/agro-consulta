#!/usr/bin/env python
"""Smoke: saída caixa usa empresa da loja (Centro × Vila). VERIFY_OK / VERIFY_FAIL."""
from __future__ import annotations

import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.chdir(ROOT)
sys.path.insert(0, ROOT)


def fail(msg: str) -> None:
    print(f"VERIFY_FAIL: {msg}")
    sys.exit(1)


def ok(msg: str) -> None:
    print(f"  OK {msg}")


def main() -> None:
    print("verify_saida_empresa_loja...")
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
    import django

    django.setup()

    from django.conf import settings

    from produtos.caixa_util import (
        empresa_nome_saida_caixa,
        deposito_operacional_sessao_caixa,
    )

    if not hasattr(settings, "AGRO_SAIDA_CAIXA_EMPRESA_VILA"):
        fail("settings sem AGRO_SAIDA_CAIXA_EMPRESA_VILA")
    ok("settings Vila")

    c = empresa_nome_saida_caixa("centro")
    v = empresa_nome_saida_caixa("vila")
    if "Centro" not in c:
        fail(f"centro inesperado: {c!r}")
    if "Vila" not in v:
        fail(f"vila inesperado: {v!r}")
    if c == v:
        fail("Centro e Vila iguais")
    ok(f"nomes: {c!r} / {v!r}")

    class _S:
        def __init__(self, ponto):
            self.ponto_caixa = ponto

    class _Req:
        session = {}
        COOKIES = {}

    assert deposito_operacional_sessao_caixa(_Req(), _S("vila")) == "vila"
    assert deposito_operacional_sessao_caixa(_Req(), _S("gaveta")) == "centro"
    ok("deposito por ponto")

    views = open(os.path.join(ROOT, "produtos", "views.py"), encoding="utf-8").read()
    if "empresa_nome_saida_caixa" not in views or "deposito_operacional_sessao_caixa" not in views:
        fail("views.py sem empresa por loja")
    if "Fonte da verdade = loja do caixa" not in views:
        fail("API saída sem forçar empresa da loja")
    ok("views painel + API")

    emb = open(
        os.path.join(ROOT, "produtos", "templates", "produtos", "includes", "caixa_saida_embed.html"),
        encoding="utf-8",
    ).read()
    if "readonly" not in emb or "cx-empresa" not in emb:
        fail("embed sem empresa readonly")
    if "empresa_nome=" not in emb:
        fail("embed não manda empresa_nome no quem leva")
    ok("template embed")

    print("VERIFY_OK 6/6")


if __name__ == "__main__":
    main()
