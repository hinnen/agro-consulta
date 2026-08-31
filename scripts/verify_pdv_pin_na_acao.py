# -*- coding: utf-8 -*-
"""Prova path PDV-PIN-NA-ACAO — consulta livre; ação com PIN fresco ~45s (não mouse)."""
from __future__ import annotations

import os
import sys
import time
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parent.parent
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
sys.path.insert(0, str(ROOT))

import django

django.setup()

from produtos.caixa_util import (
    exigir_operador_pin_request,
    rotulo_usuario_registro_venda,
)
from produtos.pdv_transf_loja_util import (
    PDV_OPERADOR_FRESCO_KEY,
    PDV_OPERADOR_FRESCO_TTL_S,
    gravar_operador_sessao_pdv,
    marcar_operador_pdv_fresco,
    operador_pdv_esta_fresco,
    peek_operador_pdv,
    renovar_operador_pdv_fresco,
    resolver_operador_pdv,
)

FAILS: list[str] = []
OKS = 0


def ok(msg: str) -> None:
    global OKS
    OKS += 1
    print("OK", msg.encode("ascii", "replace").decode("ascii"))


def fail(msg: str) -> None:
    FAILS.append(msg)
    print("FAIL", msg.encode("ascii", "replace").decode("ascii"))


def check(cond: bool, msg: str) -> None:
    if cond:
        ok(msg)
    else:
        fail(msg)


class Sess(dict):
    modified = False


def _req() -> object:
    req = SimpleNamespace()
    req.session = Sess()
    req.user = SimpleNamespace(is_authenticated=True, get_full_name=lambda: "Chrome Fake")
    return req


def check_static() -> None:
    sspin = (ROOT / "produtos/templates/produtos/_screensaver_pin.html").read_text(
        encoding="utf-8"
    )
    wiz = (ROOT / "produtos/templates/produtos/pdv_wizard.html").read_text(encoding="utf-8")
    chat_js = (ROOT / "produtos/static/produtos/js/pdv_chat_loja.js").read_text(encoding="utf-8")
    wiz_js = (ROOT / "produtos/static/produtos/js/pdv_wizard.js").read_text(encoding="utf-8")
    pedir_js = (ROOT / "produtos/static/produtos/js/pdv_pedir_loja.js").read_text(
        encoding="utf-8"
    )
    transf = (ROOT / "produtos/pdv_transf_loja_util.py").read_text(encoding="utf-8")
    chat_py = (ROOT / "produtos/pdv_chat_loja_util.py").read_text(encoding="utf-8")

    check("gmSspinGarantirOperador" in sspin, "JS garantirOperador")
    check("renovar: true" in sspin or "renovar:true" in sspin.replace(" ", ""), "JS renova TTL")
    check("sspin_pedir_pin_ao_abrir" not in wiz, "wizard sem PIN ao abrir")
    check("gmSspinGarantirOperador" in wiz_js, "confirmSale pede garantir")
    check("PIN para confirmar a venda" in wiz_js, "titulo PIN venda")
    check("gmSspinGarantirOperador" in chat_js, "chat pede garantir")
    check("gmSspinGarantirOperador" in pedir_js, "Pedir loja pede garantir")
    check("PDV_OPERADOR_FRESCO_TTL_S = 45" in transf, "TTL 45s")
    check("def peek_operador_pdv" in transf, "peek sem renovar")
    check("Alguém" not in chat_py.split("def resolver_autor_chat")[1].split("def ")[0], "chat sem fallback Alguem")
    check("get_full_name" not in chat_py.split("def resolver_autor_chat")[1].split("def ")[0], "chat sem Chrome")


def check_runtime() -> None:
    check(PDV_OPERADOR_FRESCO_TTL_S == 45, "TTL constante 45")

    req = _req()
    ok_p, lab = peek_operador_pdv(req)
    check(not ok_p and lab == "", "peek vazio sem sessao")

    req.session["pdv_operador_nome"] = "Maria"
    check(not operador_pdv_esta_fresco(req), "nome sem timestamp = nao fresco")
    check(rotulo_usuario_registro_venda(req, {}) == "", "venda sem frescor bloqueia")
    lab_e, err_e = exigir_operador_pin_request(req)
    check(lab_e == "" and "PIN" in err_e, "exigir sem frescor")

    marcar_operador_pdv_fresco(req)
    check(operador_pdv_esta_fresco(req), "marcar fresco")
    ok_p2, lab2 = peek_operador_pdv(req)
    check(ok_p2 and lab2 == "Maria", "peek fresco")
    ts1 = float(req.session.get(PDV_OPERADOR_FRESCO_KEY) or 0)
    time.sleep(0.05)
    check(peek_operador_pdv(req)[0], "peek nao renova (ainda fresco)")
    ts2 = float(req.session.get(PDV_OPERADOR_FRESCO_KEY) or 0)
    check(ts1 == ts2, "peek nao altera timestamp")

    ok_r, lab_r, _u, err_r = resolver_operador_pdv(req, "")
    check(ok_r and lab_r == "Maria" and not err_r, "resolver renova")
    ts3 = float(req.session.get(PDV_OPERADOR_FRESCO_KEY) or 0)
    check(ts3 >= ts2, "resolver atualizou timestamp")

    check(renovar_operador_pdv_fresco(req), "renovar ok")

    # Expirado
    req.session[PDV_OPERADOR_FRESCO_KEY] = time.time() - 120
    check(not operador_pdv_esta_fresco(req), "expirado apos 120s")
    check(not renovar_operador_pdv_fresco(req), "renovar falha se expirado")
    ok_x, _lab_x, _u_x, err_x = resolver_operador_pdv(req, "")
    check(not ok_x and "PIN" in err_x, "resolver expirado pede PIN")
    check(rotulo_usuario_registro_venda(req, {}) == "", "venda expirada bloqueia")

    # Mouse nao existe no servidor — so prova que TTL nao depende de atividade fake
    req.session["pdv_operador_nome"] = "Maria"
    marcar_operador_pdv_fresco(req)
    req.session[PDV_OPERADOR_FRESCO_KEY] = time.time() - 46
    check(not operador_pdv_esta_fresco(req), "46s sem acao = expirado (mouse nao conta)")


def main() -> int:
    print("=== verify_pdv_pin_na_acao ===")
    check_static()
    check_runtime()
    print(f"OKS={OKS} FAILS={len(FAILS)}")
    for f in FAILS:
        print(" -", f)
    return 1 if FAILS else 0


if __name__ == "__main__":
    raise SystemExit(main())
