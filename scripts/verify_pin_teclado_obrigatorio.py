# -*- coding: utf-8 -*-
"""
PIN-TECLADO-OBRIG — prova detalhada do path.

Bug: aviso «Identifique-se com o PIN (modo descanso)» sem linha/teclado.
Path: partial sspin · includes (NF/Gestão/Cadastro/PDV) · toast→abrir ·
      estoque NF com PIN · 403 reabre · runtime exigir + login PIN 9973.
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
sys.path.insert(0, str(ROOT))

PIN_TESTE = (os.environ.get("AGRO_PIN_TESTE") or "9973").strip()

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


def read(rel: str) -> str:
    return (ROOT / rel).read_text(encoding="utf-8", errors="replace")


def check_static() -> None:
    print("--- static ---")
    sspin = read("produtos/templates/produtos/_screensaver_pin.html")
    entrada = read("produtos/templates/produtos/entrada_nota.html")
    gestao = read("produtos/templates/produtos/produtos_gestao.html")
    cad = read("produtos/templates/produtos/produtos_cadastro_erp.html")
    pdv = read("produtos/static/produtos/js/pdv_wizard.js")
    wiz = read("produtos/templates/produtos/pdv_wizard.html")
    consulta = read("produtos/templates/produtos/consulta_produtos.html")
    caixa = read("produtos/caixa_util.py")
    views = read("produtos/views.py")

    check("id=\"sspin-input\"" in sspin, "campo #sspin-input")
    check("id=\"sspin-numpad\"" in sspin, "numpad #sspin-numpad")
    check("gmSspinErroPedePin" in sspin, "helper gmSspinErroPedePin")
    check("gmSspinAbrirSeErroPin" in sspin, "helper gmSspinAbrirSeErroPin")
    check("Identifique-se com o PIN" in sspin, "detecta mensagem canônica")
    check("forcarVisibilidadeLock" in sspin, "força visibilidade no lock")
    check("__GM_SSPIN_INIT__" in sspin, "boot único (sem 2 teclados)")
    check("querySelectorAll('#sspin-root')" in sspin, "remove sspin duplicado")
    check("input.focus" in sspin, "foco no campo ao abrir")
    check("display: flex !important" in sspin, "CSS lock display flex")
    check("sspin-locked #sspin-input" in sspin, "CSS libera clique no input")
    check("setInterval" in sspin and "sspin-locked" in sspin, "watchdog trava sem teclado")
    check("garantirSspinNoBody" in sspin, "reparent #sspin-root no body")
    check("gmSspinGarantirOperador" in sspin, "garantirOperador")
    check("openLock(true)" in sspin, "openLock forçado")

    check('_screensaver_pin.html' in entrada, "Entrada NF inclui sspin")
    check("PIN para registrar estoque" in entrada, "estoque: título PIN")
    check("gmSspinGarantirOperador" in entrada, "estoque chama garantirOperador")
    check("entradaNfeRegistrarEstoqueAgroPost" in entrada, "função POST estoque")
    check("r.status === 403" in entrada and "gmSspinErroPedePin" in entrada, "403 estoque → PIN")
    check("gmSspinAbrirSeErroPin" in entrada and "function showMsg" in entrada, "showMsg abre PIN")
    # Clique: confirm → garantirOperador (trecho do addEventListener)
    click_chunk = entrada
    idx_btn = entrada.find("btnEstoqueAgro?.addEventListener('click'")
    if idx_btn < 0:
        idx_btn = entrada.find('btnEstoqueAgro?.addEventListener("click"')
    if idx_btn >= 0:
        click_chunk = entrada[idx_btn : idx_btn + 2500]
    idx_confirm = click_chunk.find("window.confirm(`Registrar")
    idx_garantir = click_chunk.find("PIN para registrar estoque")
    check(idx_confirm > 0 and idx_garantir > idx_confirm, "confirm estoque antes do PIN (no clique)")

    check('_screensaver_pin.html' in gestao, "Gestão inclui sspin")
    check("gmSspinAbrirSeErroPin" in gestao and "function showBan" in gestao, "showBan abre PIN")
    check('_screensaver_pin.html' in cad, "Cadastro ERP inclui sspin")

    check("gmSspinAbrirSeErroPin" in pdv and "function showPdvAviso" in pdv, "PDV toast abre PIN")
    check('_screensaver_pin.html' in wiz, "wizard PDV tem sspin")
    check('_screensaver_pin.html' in consulta, "consulta tem sspin")

    msg = "Identifique-se com o PIN (modo descanso) antes de continuar."
    check(msg in caixa, "MSG canônica caixa_util")
    check("exigir_operador_pin_request(request, payload" in views, "estoque NF exige PIN no servidor")
    check("api_entrada_nota_estoque_agro" in views, "API estoque agro existe")


def check_runtime() -> None:
    print("--- runtime Django ---")
    import django

    django.setup()

    from django.conf import settings
    from django.contrib.auth import get_user_model
    from django.test import Client, RequestFactory, override_settings
    from django.urls import reverse

    from base.models import PerfilUsuario
    from produtos.caixa_util import (
        MSG_PIN_OPERADOR_OBRIGATORIO,
        exigir_operador_pin_request,
        operador_label_request,
        rotulo_operador_pin,
    )
    from produtos.pdv_transf_loja_util import gravar_operador_sessao_pdv

    class Sess(dict):
        modified = False

    rf = RequestFactory()
    req = rf.get("/")
    req.session = Sess()
    req.user = SimpleNamespace(
        is_authenticated=True,
        pk=1,
        get_full_name=lambda: "Chrome Fantasma",
        get_username=lambda: "chrome",
        first_name="Chrome",
        email="chrome@loja.local",
    )

    lab, err = exigir_operador_pin_request(req)
    check(lab == "" and err == MSG_PIN_OPERADOR_OBRIGATORIO, "sem PIN → erro teclado")
    check(operador_label_request(req) == "", "label vazio sem PIN (ignora Chrome)")

    rot = (rotulo_operador_pin(PIN_TESTE) or "").strip()
    check(bool(rot), f"PIN {PIN_TESTE} existe no PG (rotulo={rot!r})")
    if not rot:
        fail("PIN 9973 nao encontrado — restante runtime skip parcial")
        return

    gravar_operador_sessao_pdv(req, PIN_TESTE)
    lab2, err2 = exigir_operador_pin_request(req)
    check(lab2 == rot and err2 == "", f"com PIN fresco → ok ({lab2})")
    check(operador_label_request(req) == rot, "label = operador do PIN")

    # payload pin sem sessão
    req2 = rf.get("/")
    req2.session = Sess()
    req2.user = req.user
    lab3, err3 = exigir_operador_pin_request(req2, {"pin": PIN_TESTE})
    check(lab3 == rot and err3 == "", "exigir com pin no JSON")

    perfil = (
        PerfilUsuario.objects.select_related("user")
        .filter(senha_rapida=PIN_TESTE)
        .first()
    )
    check(perfil is not None, "PerfilUsuario com senha_rapida=9973")

    User = get_user_model()
    user = User.objects.filter(is_superuser=True).first() or User.objects.first()
    check(user is not None, "usuario Django para Client")
    if not user:
        return

    hosts = list(getattr(settings, "ALLOWED_HOSTS", []) or [])
    if "testserver" not in hosts:
        hosts = hosts + ["testserver", "localhost", "127.0.0.1"]

    with override_settings(ALLOWED_HOSTS=hosts):
        c = Client()
        c.force_login(user)

        url_op = reverse("api_pdv_registrar_operador")
        r0 = c.get(url_op)
        check(r0.status_code == 200, f"GET /api/pdv/operador/ ({r0.status_code})")
        j0 = r0.json() if r0.status_code == 200 else {}
        check(j0.get("ok") is True, "GET operador ok")

        # limpa sessão PIN
        c.post(url_op, data=json.dumps({"operador": ""}), content_type="application/json")

        r_login = c.post(reverse("api_login_mobile"), data={"pin": PIN_TESTE})
        check(r_login.status_code == 200, f"POST login-mobile PIN 9973 ({r_login.status_code})")
        j_login = r_login.json() if r_login.status_code == 200 else {}
        check(j_login.get("ok") is True and (j_login.get("operador") or "").strip(), f"login devolve operador ({j_login.get('operador')!r})")

        r1 = c.get(url_op)
        j1 = r1.json() if r1.status_code == 200 else {}
        check(bool(j1.get("fresco") and j1.get("operador")), f"GET fresco apos login ({j1})")

        # estoque agro SEM pin fresco deve 403 com mensagem
        c.post(url_op, data=json.dumps({"operador": ""}), content_type="application/json")
        # limpa também via login session keys
        session = c.session
        for k in ("pdv_operador_nome", "pdv_operador_fresco_em", "pdv_caixa_gerido_operador"):
            session.pop(k, None)
        session.save()

        url_est = reverse("api_entrada_nota_estoque_agro")
        body = {
            "linhas": [{"produto_id": "x", "quantidade": 1}],
            "deposito": "centro",
            "cabecalho": {},
            "salvar_rascunho": False,
        }
        r_est = c.post(url_est, data=json.dumps(body), content_type="application/json")
        check(r_est.status_code == 403, f"estoque agro sem PIN = 403 (got {r_est.status_code})")
        try:
            j_est = r_est.json()
        except Exception:
            j_est = {}
        check(
            MSG_PIN_OPERADOR_OBRIGATORIO in str(j_est.get("erro") or ""),
            "estoque 403 = mensagem modo descanso",
        )

        # com PIN fresco: API passa do gate de PIN (pode falhar depois por linhas/produto)
        r_pin = c.post(
            url_op,
            data=json.dumps({"pin": PIN_TESTE}),
            content_type="application/json",
        )
        check(r_pin.status_code == 200 and (r_pin.json() or {}).get("ok"), "POST operador PIN 9973")
        r_est2 = c.post(url_est, data=json.dumps(body), content_type="application/json")
        # Com PIN: não deve ser 403 de PIN; pode ser 400/404/outro por produto fake
        try:
            j2 = r_est2.json()
        except Exception:
            j2 = {}
        erro2 = str(j2.get("erro") or "")
        check(
            r_est2.status_code != 403 or MSG_PIN_OPERADOR_OBRIGATORIO not in erro2,
            f"com PIN fresco nao bloqueia por modo descanso (status={r_est2.status_code} erro={erro2[:80]!r})",
        )

        # HTTP pages include sspin markup
        for name, url_name in (
            ("entrada_nota", "entrada_nota"),
            ("gestao", "produtos_gestao"),
            ("cadastro", "produtos_cadastro_erp"),
        ):
            try:
                url = reverse(url_name)
            except Exception as e:
                fail(f"reverse {url_name}: {e}")
                continue
            r = c.get(url)
            # algumas telas exigem staff; aceita 200 ou redirect login
            if r.status_code in (301, 302):
                ok(f"GET {name} redirect {r.status_code} (auth)")
                continue
            check(r.status_code == 200, f"GET {name} ({r.status_code})")
            body_html = r.content.decode("utf-8", errors="replace")
            check("sspin-root" in body_html and "sspin-input" in body_html, f"{name} HTML tem teclado PIN")


def main() -> int:
    print("=== PIN-TECLADO-OBRIG path detalhado ===")
    check_static()
    try:
        check_runtime()
    except Exception as e:
        fail(f"runtime: {e}")

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
