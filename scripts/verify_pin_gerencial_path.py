# -*- coding: utf-8 -*-
"""Prova path PIN gerencial + forçar liberar Point."""
from __future__ import annotations

import os
import sys
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parent.parent
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
sys.path.insert(0, str(ROOT))

import django

django.setup()

from produtos.pin_gerencial_util import (
    PIN_GERENCIAL_HINT,
    PIN_GERENCIAL_NOMES_UI,
    is_usuario_gerencial,
    rotulo_gerencial_do_user,
)

FAILS: list[str] = []
OKS = 0


def ok(msg: str) -> None:
    global OKS
    OKS += 1
    print("OK", msg)


def fail(msg: str) -> None:
    FAILS.append(msg)
    print("FAIL", msg)


def check(cond: bool, msg: str) -> None:
    if cond:
        ok(msg)
    else:
        fail(msg)


def main() -> int:
    views_mp = (ROOT / "produtos/views_mp_point.py").read_text(encoding="utf-8")
    views = (ROOT / "produtos/views.py").read_text(encoding="utf-8")
    urls = (ROOT / "produtos/urls.py").read_text(encoding="utf-8")
    pdv = (ROOT / "pdv/views.py").read_text(encoding="utf-8")
    wizard = (ROOT / "produtos/static/produtos/js/pdv_wizard.js").read_text(encoding="utf-8")
    util = (ROOT / "produtos/pin_gerencial_util.py").read_text(encoding="utf-8")

    check("Geraldo, Geraldinho ou Renan Hinnen" in PIN_GERENCIAL_NOMES_UI, "nomes UI")
    check("peça o PIN" in PIN_GERENCIAL_HINT.lower() or "peça o PIN" in PIN_GERENCIAL_HINT, "hint pede PIN")
    check("def validar_pin_gerencial" in util, "validar_pin_gerencial")
    check("def is_usuario_gerencial" in util, "is_usuario_gerencial reutilizável")

    u_renan = SimpleNamespace(username="admin", first_name="Renan", last_name="Hinnen")
    u_geraldo = SimpleNamespace(username="Geraldo", first_name="", last_name="")
    u_dinho = SimpleNamespace(username="Geraldinho", first_name="", last_name="")
    u_geraldo2 = SimpleNamespace(username="gmagromais", first_name="geraldo", last_name="hinnen")
    u_op = SimpleNamespace(username="caixa1", first_name="Maria", last_name="Silva")

    check(rotulo_gerencial_do_user(u_renan) == "Renan Hinnen", "match Renan")
    check(rotulo_gerencial_do_user(u_geraldo) == "Geraldo", "match Geraldo user")
    check(rotulo_gerencial_do_user(u_dinho) == "Geraldinho", "match Geraldinho")
    check(rotulo_gerencial_do_user(u_geraldo2) == "Geraldo", "match geraldo hinnen")
    check(rotulo_gerencial_do_user(u_op) is None, "operador comum fora")
    check(is_usuario_gerencial(u_dinho) is True, "is gerencial dinho")
    check(is_usuario_gerencial(u_op) is False, "is gerencial op false")
    # Geraldinho não classifica como Geraldo
    check(rotulo_gerencial_do_user(u_dinho) != "Geraldo", "dinho != geraldo")

    check("api_pdv_mp_point_forcar_liberar" in views_mp, "view forcar")
    check("api_pdv_mp_point_forcar_liberar" in urls, "url forcar")
    check("apiPdvMpPointForcarLiberar" in pdv, "bootstrap url")
    check("mp_point_forcar_bypass_ativo" in views_mp, "bloqueio respeita bypass")
    check("payload_hint_pin_gerencial" in views, "409 com hint")
    check("limpar_mp_point_forcar_bypass" in views, "limpa bypass pos venda")
    check("showPdvPinGerencial" in wizard, "overlay PIN")
    check("forcarLiberarMpPointComPin" in wizard, "JS forcar")
    check("Geraldo, Geraldinho ou Renan Hinnen" in wizard, "JS nomes")
    check("mpPointBloqueio" in wizard, "JS trata bloqueio")
    check("apiPdvMpPointForcarLiberar" in wizard, "JS usa URL")

    print("")
    print(f"OKS={OKS} FAILS={len(FAILS)}")
    if FAILS:
        for f in FAILS:
            print(" -", f)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
