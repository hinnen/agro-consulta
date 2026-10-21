#!/usr/bin/env python3
"""Prova path — repasse manual Centro → Vila no PDV."""
from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

import django

django.setup()

failures: list[str] = []


def check(cond: bool, msg: str) -> None:
    if not cond:
        failures.append(msg)


def main() -> int:
    urls = (ROOT / "produtos/urls.py").read_text(encoding="utf-8")
    check("confirmar-centro-vila" in urls, "url confirmar-centro-vila")

    views = (ROOT / "produtos/views_repasse_vila.py").read_text(encoding="utf-8")
    check("api_repasse_centro_vila_confirmar" in views, "view api_repasse_centro_vila_confirmar")
    check("saldo_dinheiro_caixa_centro" in views, "meta caixa_centro")

    util = (ROOT / "produtos/repasse_vila_util.py").read_text(encoding="utf-8")
    for needle in (
        "def confirmar_repasse_centro_vila",
        "def aplicar_repasses_centro_vila_pendentes_vila",
        "RepasseCentroVilaAgro",
        "def saldo_dinheiro_caixa_centro",
    ):
        check(needle in util, f"util {needle}")

    models = (ROOT / "produtos/models.py").read_text(encoding="utf-8")
    check("class RepasseCentroVilaAgro" in models, "model RepasseCentroVilaAgro")

    views_py = (ROOT / "produtos/views.py").read_text(encoding="utf-8")
    check("aplicar_repasses_centro_vila_pendentes_vila" in views_py, "hook abertura Vila")

    overlay = (ROOT / "produtos/templates/produtos/partials/pdv/repasse_vila_overlay.html").read_text(
        encoding="utf-8"
    )
    check("pdv-rp-toggle-sentido" in overlay, "toggle sentido overlay")
    check("pdv-rp-corpo-centro-vila" in overlay, "corpo centro-vila")

    js = (ROOT / "produtos/static/produtos/js/pdv_repasse_vila.js").read_text(encoding="utf-8")
    check("confirmar-centro-vila" in js, "js fetch confirmar-centro-vila")
    check("setModoSentido" in js, "js setModoSentido")

    mig = ROOT / "produtos/migrations/0148_repasse_centro_vila.py"
    check(mig.is_file(), "migration 0148")

    from produtos.models import RepasseCentroVilaAgro

    check(
        RepasseCentroVilaAgro._meta.db_table,
        "model loaded",
    )

    if failures:
        print("VERIFY_REPASSE_CENTRO_VILA_PATH_FAIL")
        for f in failures:
            print(" -", f)
        return 1
    print("VERIFY_REPASSE_CENTRO_VILA_PATH_OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
