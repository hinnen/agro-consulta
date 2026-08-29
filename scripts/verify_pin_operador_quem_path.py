# -*- coding: utf-8 -*-
"""Prova path PIN-OPERADOR-QUEM — Quem = PIN; sem Chrome; sem PIN = exige PIN."""
from __future__ import annotations

import os
import re
import sys
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parent.parent
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
sys.path.insert(0, str(ROOT))

import django

django.setup()

from django.contrib.auth import get_user_model
from django.test import RequestFactory

from base.models import PerfilUsuario
from produtos.caixa_util import (
    MSG_PIN_OPERADOR_OBRIGATORIO,
    PinOperadorObrigatorioError,
    exigir_operador_pin_request,
    operador_label_request,
    rotulo_operador_pin,
    rotulo_usuario_registro_venda,
)
from produtos.pdv_transf_loja_util import gravar_operador_sessao_pdv

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


def _req_chrome_geraldo() -> object:
    rf = RequestFactory()
    req = rf.get("/")
    req.session = Sess()
    req.user = SimpleNamespace(
        is_authenticated=True,
        pk=999001,
        get_full_name=lambda: "Geraldo Hinnen",
        get_username=lambda: "geraldo.hinnen",
        first_name="Geraldo",
        last_name="Hinnen",
        email="geraldo.hinnen@loja.local",
    )
    return req


def check_static() -> None:
    caixa = (ROOT / "produtos/caixa_util.py").read_text(encoding="utf-8")
    views = (ROOT / "produtos/views.py").read_text(encoding="utf-8")
    bug_js = (ROOT / "produtos/static/produtos/js/agro_bug_report.js").read_text(encoding="utf-8")
    bug_py = (ROOT / "produtos/bug_report_views.py").read_text(encoding="utf-8")
    transf = (ROOT / "produtos/pdv_transf_loja_util.py").read_text(encoding="utf-8")
    ui = (ROOT / "produtos/templates/produtos/_agro_consulta_ui.html").read_text(encoding="utf-8")

    check("MSG_PIN_OPERADOR_OBRIGATORIO" in caixa, "constante MSG_PIN")
    check("def exigir_operador_pin_request" in caixa, "helper exigir_operador_pin")
    check("class PinOperadorObrigatorioError" in caixa, "PinOperadorObrigatorioError")

    # operador_label_request: não chama rotulo_usuario_django no final
    m = re.search(
        r"def operador_label_request\(request\).*?(?=\ndef )",
        caixa,
        re.S,
    )
    check(bool(m), "bloco operador_label_request")
    if m:
        body = m.group(0)
        check("rotulo_usuario_django" not in body, "operador_label_request sem Django")
        check('return ""' in body, "operador_label_request vazio sem PIN")

    m2 = re.search(
        r"def rotulo_usuario_registro_venda\(request.*?(?=\ndef )",
        caixa,
        re.S,
    )
    check(bool(m2), "bloco rotulo_usuario_registro_venda")
    if m2:
        body = m2.group(0)
        check("rotulo_usuario_django" not in body, "registro venda sem Django")
        check('("operador_pdv"' not in body and 'data.get("operador")' not in body, "ignora nome do browser")

    check("exigir_operador_pin_request(request, data)" in views, "API venda exige PIN")
    check("exigir_operador_pin_request(request)" in views, "devolução exige PIN")
    check("api_entrega_registrar" in views and "exigir_operador_pin_request(request, body" in views, "entrega exige PIN")
    check("usuario_op, err_pin_op = exigir_operador_pin_request" in views, "entrada NF exige PIN")
    check("MSG_PIN_OPERADOR_OBRIGATORIO" in views, "views usa MSG_PIN")

    # adotar caixa: sem fallback get_full_name
    m3 = re.search(
        r"operador = rotulo_operador_pin\(pin\) if pin else \"\".*?mensagem.*?navegador",
        views,
        re.S,
    )
    check(bool(m3), "bloco adotar caixa")
    if m3:
        check("get_full_name" not in m3.group(0), "adotar caixa sem get_full_name")
        check("MSG_PIN_OPERADOR_OBRIGATORIO" in m3.group(0), "adotar caixa pede PIN")

    check("pdv_caixa_gerido_operador" in transf and "pdv_operador_nome" in transf, "PIN grava gerido+nome")
    check("pdv_caixa_gerido_operador" in views and 'pop("pdv_caixa_gerido_operador"' in views, "limpa gerido ao sair")

    check("gm_sspin_operador" in bug_js, "bug JS lê PIN localStorage")
    check("agro-user-display" not in bug_js, "bug JS sem meta Chrome")
    check("preencherNomeDoPin" in bug_js, "bug JS preenche do PIN")
    check("operador_label_request" in bug_py, "bug API usa operador_label")
    check("get_full_name" not in bug_py.split("def _usuario_nome")[1].split("def ")[0], "bug _usuario_nome sem Chrome")
    check("bug15" in ui or "bug14" in ui, "cache bust bug report")


def check_runtime() -> None:
    req = _req_chrome_geraldo()
    check(operador_label_request(req) == "", "Chrome sozinho = vazio")
    check(rotulo_usuario_registro_venda(req, {"operador": "ChromeFake", "vendedor": "X"}) == "", "payload nome ignorado")
    lab, err = exigir_operador_pin_request(req)
    check(lab == "" and "PIN" in err, "exigir sem PIN = erro")
    check(err == MSG_PIN_OPERADOR_OBRIGATORIO, "mensagem padrao")

    req.session["pdv_caixa_gerido_operador"] = "Geraldo Hinnen"
    check(operador_label_request(req) == "Geraldo Hinnen", "gerido sozinho ainda conta (sessao)")
    req.session["pdv_operador_nome"] = "Renan"
    check(operador_label_request(req) == "Renan", "PIN descanso vence gerido grudado")
    check(rotulo_usuario_registro_venda(req, {}) == "Renan", "registro venda usa PIN sessao")

    req2 = _req_chrome_geraldo()
    req2.session["ajuste_mobile_operador"] = "Queila"
    check(operador_label_request(req2) == "Queila", "ajuste mobile conta")

    # PIN real do banco (Renan 9973 no local típico)
    pin_renan = None
    rot_esperado = None
    for p in (
        PerfilUsuario.objects.exclude(senha_rapida="")
        .exclude(senha_rapida__isnull=True)
        .exclude(senha_rapida="1234")
        .select_related("user")
        .order_by("pk")
    ):
        pin = (p.senha_rapida or "").strip()
        if not pin:
            continue
        rot = rotulo_operador_pin(pin)
        if rot:
            pin_renan = pin
            rot_esperado = rot
            break
    check(bool(pin_renan), "existe PIN válido no PG local")
    if pin_renan:
        req3 = _req_chrome_geraldo()
        ok_g, label, user, err_g = gravar_operador_sessao_pdv(req3, pin_renan)
        check(ok_g and not err_g, "gravar_operador_sessao_pdv OK")
        check(
            label == rot_esperado or (label or "").startswith((rot_esperado or "")[:3]),
            f"rotulo PIN={rot_esperado!r} got={label!r}",
        )
        check(req3.session.get("pdv_operador_nome") == (label or "")[:120], "sessao nome")
        check(req3.session.get("pdv_caixa_gerido_operador") == (label or "")[:120], "sessao gerido alinhado")
        check(operador_label_request(req3) == (label or "")[:120], "Quem = PIN nao Chrome")
        check(req3.user.get_full_name() == "Geraldo Hinnen", "login Chrome ainda Geraldo (ignorado)")

        lab2, err2 = exigir_operador_pin_request(req3)
        check(lab2 == (label or "")[:120] and err2 == "", "exigir com sessao OK")

        req4 = _req_chrome_geraldo()
        lab3, err3 = exigir_operador_pin_request(req4, {"pin": pin_renan})
        check(lab3 == rot_esperado and err3 == "", "exigir com pin no payload")

    try:
        raise PinOperadorObrigatorioError()
    except PinOperadorObrigatorioError as e:
        check(str(e) == MSG_PIN_OPERADOR_OBRIGATORIO, "exception mensagem")


def check_http_apis() -> None:
    """Smoke HTTP: endpoints que devem 403 sem PIN (quando autenticado)."""
    from django.conf import settings
    from django.test import Client, override_settings
    from django.urls import reverse

    User = get_user_model()
    user = User.objects.filter(is_superuser=True).first() or User.objects.first()
    if not user:
        fail("sem usuario Django para Client")
        return

    hosts = list(getattr(settings, "ALLOWED_HOSTS", []) or [])
    if "testserver" not in hosts:
        hosts = hosts + ["testserver", "localhost", "127.0.0.1"]

    with override_settings(ALLOWED_HOSTS=hosts):
        c = Client()
        c.force_login(user)

        url = reverse("api_entrega_registrar")
        r = c.post(url, data='{"cliente_nome":"Teste Path PIN"}', content_type="application/json")
        check(r.status_code == 403, f"entrega sem PIN = 403 (got {r.status_code})")
        try:
            j = r.json()
        except Exception:
            j = {}
        check("PIN" in str(j.get("erro") or ""), "entrega erro menciona PIN")

        # devolução exige PIN — precisa de venda existente; só checa helper via view source já feito
        # login_mobile / registrar operador GET
        url_op = reverse("api_pdv_registrar_operador")
        r3 = c.get(url_op)
        check(r3.status_code == 200, f"GET operador sessao ({r3.status_code})")
        try:
            j3 = r3.json()
        except Exception:
            j3 = {}
        check(j3.get("ok") is True and j3.get("operador") == "", "GET operador vazio sem PIN")

        # grava PIN e confere
        pin_ok = None
        for p in (
            PerfilUsuario.objects.exclude(senha_rapida="")
            .exclude(senha_rapida__isnull=True)
            .exclude(senha_rapida="1234")
            .order_by("pk")
        ):
            if (p.senha_rapida or "").strip():
                pin_ok = (p.senha_rapida or "").strip()
                break
        if pin_ok:
            r4 = c.post(
                url_op,
                data=f'{{"pin":"{pin_ok}"}}',
                content_type="application/json",
            )
            check(r4.status_code == 200, f"POST PIN operador ({r4.status_code})")
            try:
                j4 = r4.json()
            except Exception:
                j4 = {}
            check(bool(j4.get("ok") and j4.get("operador")), f"POST PIN devolve operador ({j4})")
            r5 = c.get(url_op)
            j5 = r5.json() if r5.status_code == 200 else {}
            check(
                (j5.get("operador") or "") == (j4.get("operador") or ""),
                "GET apos PIN = mesmo operador",
            )
            # limpa
            c.post(url_op, data='{"operador":""}', content_type="application/json")
        else:
            ok("skip POST PIN (sem PIN no PG)")


def main() -> int:
    print("=== PIN-OPERADOR-QUEM path ===")
    check_static()
    check_runtime()
    try:
        check_http_apis()
    except Exception as e:
        fail(f"HTTP smoke: {e}")

    print("---")
    print(f"OK={OKS} FAIL={len(FAILS)}")
    for f in FAILS:
        print(" ", f)
    if FAILS:
        print("VERIFY_FAIL")
        return 1
    print("VERIFY_OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
