# -*- coding: utf-8 -*-
"""Prova path PDV-PIN-NA-ACAO — consulta livre; ação com PIN fresco ~45s (não mouse)."""
from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

ROOT = Path(__file__).resolve().parent.parent
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
sys.path.insert(0, str(ROOT))

import django

django.setup()

from django.test import RequestFactory

from produtos.caixa_util import (
    exigir_operador_pin_request,
    rotulo_usuario_registro_venda,
)
from produtos.pdv_chat_loja_util import criar_mensagem, resolver_autor_chat
from produtos.pdv_transf_loja_util import (
    PDV_OPERADOR_FRESCO_KEY,
    PDV_OPERADOR_FRESCO_TTL_S,
    limpar_operador_pdv_sessao,
    marcar_operador_pdv_fresco,
    operador_pdv_esta_fresco,
    operador_pdv_restante_fresco_s,
    peek_operador_pdv,
    renovar_operador_pdv_fresco,
    resolver_operador_pdv,
)
from produtos.views import api_pdv_registrar_operador
from produtos.views_pdv_transf_loja import api_pdv_transf_loja_resumo

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
    req.user = SimpleNamespace(
        is_authenticated=True,
        get_full_name=lambda: "Chrome Fake",
        get_username=lambda: "chrome.fake",
        pk=1,
    )
    return req


def _rf_get(path="/api/pdv/operador/", session=None):
    rf = RequestFactory()
    req = rf.get(path)
    req.session = session if session is not None else Sess()
    req.user = SimpleNamespace(is_authenticated=True, pk=1)
    return req


def _rf_post(path, body: dict, session=None):
    rf = RequestFactory()
    req = rf.post(path, data=json.dumps(body), content_type="application/json")
    req.session = session if session is not None else Sess()
    req.user = SimpleNamespace(is_authenticated=True, pk=1)
    return req


def check_static() -> None:
    print("--- static ---")
    sspin = (ROOT / "produtos/templates/produtos/_screensaver_pin.html").read_text(
        encoding="utf-8"
    )
    wiz = (ROOT / "produtos/templates/produtos/pdv_wizard.html").read_text(encoding="utf-8")
    consulta = (ROOT / "produtos/templates/produtos/consulta_produtos.html").read_text(
        encoding="utf-8"
    )
    chat_js = (ROOT / "produtos/static/produtos/js/pdv_chat_loja.js").read_text(encoding="utf-8")
    wiz_js = (ROOT / "produtos/static/produtos/js/pdv_wizard.js").read_text(encoding="utf-8")
    pedir_js = (ROOT / "produtos/static/produtos/js/pdv_pedir_loja.js").read_text(
        encoding="utf-8"
    )
    consulta_js = (ROOT / "produtos/static/produtos/js/consulta_produtos.js").read_text(
        encoding="utf-8"
    )
    transf = (ROOT / "produtos/pdv_transf_loja_util.py").read_text(encoding="utf-8")
    chat_py = (ROOT / "produtos/pdv_chat_loja_util.py").read_text(encoding="utf-8")
    views = (ROOT / "produtos/views.py").read_text(encoding="utf-8")
    views_transf = (ROOT / "produtos/views_pdv_transf_loja.py").read_text(encoding="utf-8")
    views_chat = (ROOT / "produtos/views_pdv_chat_loja.py").read_text(encoding="utf-8")
    caixa = (ROOT / "produtos/caixa_util.py").read_text(encoding="utf-8")

    check("gmSspinGarantirOperador" in sspin, "JS garantirOperador")
    check("renovar: true" in sspin or '"renovar": true' in sspin, "JS renova TTL")
    check("data-idle-min=\"3\"" in sspin, "descanso 3 min permanece")
    # Mouse só bumpIdle do screensaver — não toca API renovar
    check("wakeEvents" in sspin and "bumpIdle()" in sspin, "wakeEvents + bumpIdle")
    wake_idx = sspin.find("wakeEvents")
    wake_snip = sspin[wake_idx : wake_idx + 280] if wake_idx >= 0 else ""
    check("bumpIdle()" in wake_snip, "wakeEvents chama bumpIdle")
    check(
        "renovar" not in wake_snip and "API_OPERADOR" not in wake_snip,
        "wakeEvents nao renova identidade",
    )
    check("sspin_pedir_pin_ao_abrir" not in wiz, "wizard sem PIN ao abrir")
    check("sspin_pedir_pin_ao_abrir" not in consulta, "consulta sem PIN ao abrir")
    check("_screensaver_pin.html" in wiz and "_screensaver_pin.html" in consulta, "ambos incluem sspin")
    check("gmSspinGarantirOperador" in wiz_js, "confirmSale pede garantir")
    check("PIN para confirmar a venda" in wiz_js, "titulo PIN venda")
    check("gmSspinGarantirOperador" in chat_js, "chat pede garantir")
    check("precisa_pin" in chat_js, "chat trata precisa_pin")
    check("gmSspinGarantirOperador" in pedir_js, "Pedir loja pede garantir")
    check("enviarPedidoExec" in pedir_js, "Pedir split envio apos PIN")
    check("postAcaoExec" in pedir_js, "Pedir acao apos PIN")
    check("gmSspinGarantirOperador" in consulta_js, "consulta legado pede garantir")
    check("PDV_OPERADOR_FRESCO_TTL_S = 45" in transf, "TTL 45s")
    check("def peek_operador_pdv" in transf, "peek sem renovar")
    check("peek_operador_pdv" in views_transf, "resumo usa peek")
    check("operador_pdv_esta_fresco" in caixa, "venda exige fresco")
    check("limpar_operador_pdv_sessao" in views, "API limpa sessao")
    check('"renovar"' in views or "renovar" in views, "API renovar")
    check("precisa_pin" in views_chat, "chat API precisa_pin")
    autor_block = chat_py.split("def resolver_autor_chat")[1].split("def ")[0]
    check("Alguém" not in autor_block, "chat sem fallback Alguem")
    check("get_full_name" not in autor_block, "chat autor sem Chrome")
    check("resolver_operador_pdv" in chat_py.split("def criar_mensagem")[1], "criar_mensagem exige PIN")


def check_runtime() -> None:
    print("--- runtime frescor ---")
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
    check(operador_pdv_restante_fresco_s(req) > 0, "restante > 0")
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

    # 46s sem ação = expirado (mouse não conta no servidor)
    req.session["pdv_operador_nome"] = "Maria"
    marcar_operador_pdv_fresco(req)
    req.session[PDV_OPERADOR_FRESCO_KEY] = time.time() - 46
    check(not operador_pdv_esta_fresco(req), "46s sem acao = expirado")

    # limpar zera tudo
    marcar_operador_pdv_fresco(req)
    limpar_operador_pdv_sessao(req)
    check(not req.session.get("pdv_operador_nome"), "limpar remove nome")
    check(not operador_pdv_esta_fresco(req), "limpar remove fresco")


def check_dois_operadores() -> None:
    print("--- dois operadores ---")
    req = _req()
    req.session["pdv_operador_nome"] = "OperadorA"
    marcar_operador_pdv_fresco(req)
    check(rotulo_usuario_registro_venda(req, {}) == "OperadorA", "A fresco vende")

    # B «chega» sem PIN novo: simula só mexer (TTL passa)
    req.session[PDV_OPERADOR_FRESCO_KEY] = time.time() - 50
    check(rotulo_usuario_registro_venda(req, {}) == "", "B sem PIN novo nao herda A")
    check(resolver_autor_chat(req, {}) == "", "chat sem herdar A expirado")

    # A volta a ser fresco, B ainda não digitou — nome A só se fresco
    marcar_operador_pdv_fresco(req)
    check(resolver_autor_chat(req, {}) == "OperadorA", "chat com fresco = A")


def check_chat_bloqueio() -> None:
    print("--- chat bloqueio ---")
    req = _req()
    m, err = criar_mensagem(req, texto="oi sem pin")
    check(m is None and "PIN" in (err or ""), "criar_mensagem sem PIN bloqueia")
    # Chrome autenticado não basta
    check(resolver_autor_chat(req, {}) == "", "autor vazio sem PIN (ignora Chrome)")


def check_api_operador() -> None:
    print("--- API /api/pdv/operador/ ---")
    sess = Sess()
    req = _rf_get(session=sess)
    resp = api_pdv_registrar_operador(req)
    body = json.loads(resp.content)
    check(body.get("ok") is True and body.get("fresco") is False, "GET vazio fresco=false")
    check(body.get("operador") == "", "GET sem operador")

    sess["pdv_operador_nome"] = "Ana"
    marcar_operador_pdv_fresco(req)
    resp2 = api_pdv_registrar_operador(_rf_get(session=sess))
    body2 = json.loads(resp2.content)
    check(body2.get("fresco") is True and body2.get("operador") == "Ana", "GET fresco com nome")
    check(int(body2.get("restante_s") or 0) > 0, "GET restante_s")

    # renovar
    ts_before = float(sess.get(PDV_OPERADOR_FRESCO_KEY) or 0)
    time.sleep(0.02)
    resp3 = api_pdv_registrar_operador(_rf_post("/api/pdv/operador/", {"renovar": True}, session=sess))
    body3 = json.loads(resp3.content)
    check(resp3.status_code == 200 and body3.get("ok"), "POST renovar OK")
    check(float(sess.get(PDV_OPERADOR_FRESCO_KEY) or 0) >= ts_before, "renovar mexeu timestamp")

    # renovar expirado
    sess[PDV_OPERADOR_FRESCO_KEY] = time.time() - 99
    resp4 = api_pdv_registrar_operador(_rf_post("/api/pdv/operador/", {"renovar": True}, session=sess))
    body4 = json.loads(resp4.content)
    check(resp4.status_code == 403 and body4.get("precisa_pin"), "renovar expirado = 403 precisa_pin")

    # limpar
    sess["pdv_operador_nome"] = "Ana"
    marcar_operador_pdv_fresco(req)
    resp5 = api_pdv_registrar_operador(_rf_post("/api/pdv/operador/", {"operador": ""}, session=sess))
    body5 = json.loads(resp5.content)
    check(body5.get("operador") == "" and not sess.get("pdv_operador_nome"), "POST limpa operador")


def check_resumo_peek() -> None:
    print("--- resumo Pedir (peek) ---")
    sess = Sess()
    sess["pdv_operador_nome"] = "Maria"
    marcar_operador_pdv_fresco(SimpleNamespace(session=sess))
    ts1 = float(sess.get(PDV_OPERADOR_FRESCO_KEY) or 0)
    req = _rf_get("/api/pdv/transf-loja/resumo/", session=sess)
    with patch(
        "produtos.views_pdv_transf_loja.bootstrap_deposito",
        return_value={"deposito": "centro"},
    ), patch(
        "produtos.views_pdv_transf_loja.resumo_loja",
        return_value={
            "loja": "centro",
            "loja_label": "Centro",
            "recebidos_abertos": 0,
            "recebidos_pendentes": 0,
            "enviados_abertos": 0,
        },
    ):
        time.sleep(0.02)
        resp = api_pdv_transf_loja_resumo(req)
    body = json.loads(resp.content)
    check(body.get("precisa_pin") is False and body.get("operador") == "Maria", "resumo com fresco")
    ts2 = float(sess.get(PDV_OPERADOR_FRESCO_KEY) or 0)
    check(ts1 == ts2, "poll resumo NAO renova TTL")

    sess[PDV_OPERADOR_FRESCO_KEY] = time.time() - 80
    with patch(
        "produtos.views_pdv_transf_loja.bootstrap_deposito",
        return_value={"deposito": "centro"},
    ), patch(
        "produtos.views_pdv_transf_loja.resumo_loja",
        return_value={
            "loja": "centro",
            "loja_label": "Centro",
            "recebidos_abertos": 0,
            "recebidos_pendentes": 0,
            "enviados_abertos": 0,
        },
    ):
        resp2 = api_pdv_transf_loja_resumo(_rf_get("/api/pdv/transf-loja/resumo/", session=sess))
    body2 = json.loads(resp2.content)
    check(body2.get("precisa_pin") is True, "resumo expirado precisa_pin")


def check_unit_resolver() -> None:
    print("--- unit resolver ---")
    from produtos.tests_pdv_transf_loja import ResolverOperadorTests

    t = ResolverOperadorTests()
    try:
        t.test_sessao_sem_pin()
        ok("unit sessao fresco OK")
    except Exception as e:
        fail(f"unit sessao fresco: {e}")
    try:
        t.test_sessao_expirada_pede_pin()
        ok("unit sessao expirada OK")
    except Exception as e:
        fail(f"unit sessao expirada: {e}")
    try:
        t.test_sem_sessao_pede_pin()
        ok("unit sem sessao OK")
    except Exception as e:
        fail(f"unit sem sessao: {e}")


def main() -> int:
    print("=== verify_pdv_pin_na_acao (detalhado) ===")
    check_static()
    check_runtime()
    check_dois_operadores()
    check_chat_bloqueio()
    check_api_operador()
    check_resumo_peek()
    check_unit_resolver()
    print(f"OKS={OKS} FAILS={len(FAILS)}")
    for f in FAILS:
        print(" -", f)
    return 1 if FAILS else 0


if __name__ == "__main__":
    raise SystemExit(main())
