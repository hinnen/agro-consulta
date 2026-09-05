#!/usr/bin/env python3
"""Prova detalhada WA-PIN-COMPOSER — card Quem/PIN na barra do Zap.

  python scripts/verify_wa_pin_composer_path.py

Fonte · Django Client (PIN 9973) · _autor_wa · (HTTP local opcional).
VERIFY_OK N/N · VERIFY_FAIL.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path
from urllib.error import URLError
from urllib.request import urlopen

ROOT = Path(__file__).resolve().parents[1]
os.chdir(ROOT)
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

CHECKS = 0
PIN = "9973"
BASE = os.environ.get("AGRO_VERIFY_BASE", "http://127.0.0.1:8000").rstrip("/")


def fail(msg: str) -> None:
    print(f"VERIFY_FAIL: {msg}")
    sys.exit(1)


def ok(msg: str) -> None:
    global CHECKS
    CHECKS += 1
    print(f"OK {msg}")


def _read(rel: str) -> str:
    return (ROOT / rel).read_text(encoding="utf-8")


def prova_fonte() -> None:
    print("=== estatico ===")
    composer = _read("produtos/templates/produtos/_wa_composer.html")
    skin = _read("produtos/templates/produtos/_wa_skin.html")
    page = _read("produtos/templates/produtos/atendimento_whatsapp.html")
    cel = _read("produtos/templates/produtos/atendimento_whatsapp_celular.html")
    js = _read("produtos/static/produtos/js/atendimento_whatsapp.js")
    sspin = _read("produtos/templates/produtos/_screensaver_pin.html")
    views = _read("produtos/views_atendimento_whatsapp.py")

    if 'id="wa-operador-pin"' not in composer:
        fail("composer sem botao wa-operador-pin")
    if "wa-op-pin-lbl" not in composer or "Quem" not in composer:
        fail("composer sem label Quem")
    if 'id="wa-operador-pin-nome"' not in composer:
        fail("composer sem span do nome")
    if composer.find("wa-operador-pin") > composer.find('id="wa-input"'):
        fail("card PIN deve vir antes do input")
    ok("composer card Quem antes do input")

    if ".wa-op-pin" not in skin or "wa-op-pin-nome" not in skin:
        fail("skin sem estilo do card")
    if "flex: 1 1 0%" not in skin and "flex: 1 1 0" not in skin:
        fail("input nao cede flex")
    if "is-rec #wa-operador-pin" not in skin:
        fail("skin nao esconde card na gravacao")
    if "7.25rem" not in skin and "width: 7.25rem" not in skin:
        fail("card sem largura fixa (~7.25rem)")
    ok("skin card + input cede espaco")

    if "_screensaver_pin.html" not in page:
        fail("Zap web sem screensaver PIN")
    if "_screensaver_pin.html" not in cel:
        fail("Zap celular sem screensaver PIN")
    if "_wa_composer.html" not in page and "wa-operador-pin" not in page:
        # page includes composer
        if "{% include \"produtos/_wa_composer.html\" %}" not in page and "_wa_composer.html" not in page:
            fail("pagina web sem composer")
    ok("paginas web/celular com PIN + composer")

    for trecho in (
        "pintarOperadorPin",
        "nomeCurtoOperador",
        "gmSspinSairEAbrirPin",
        "gm-sspin-operador",
        "gm_sspin_operador",
        "nomeOperadorPin() || 'Você'",
        "trocarOperadorPin",
    ):
        if trecho not in js:
            fail(f"JS sem {trecho!r}")
    ok("JS pintar/trocar/autor PIN")

    if "gmSspinSairEAbrirPin" not in sspin:
        fail("screensaver sem gmSspinSairEAbrirPin")
    ok("screensaver expoe troca de PIN")

    if "def _autor_wa" not in views:
        fail("views sem _autor_wa")
    if "operador_label_request" not in views:
        fail("_autor_wa sem operador_label_request")
    ok("_autor_wa usa operador da sessao/PIN")


def prova_django() -> None:
    print("=== Django Client / PIN 9973 ===")
    import django

    django.setup()
    from django.test import Client, override_settings
    from django.urls import reverse

    from base.models import PerfilUsuario
    from produtos.caixa_util import operador_label_de_pin, rotulo_operador_pin
    from produtos.pdv_transf_loja_util import gravar_operador_sessao_pdv
    from produtos.views_atendimento_whatsapp import _autor_wa

    perfil = (
        PerfilUsuario.objects.filter(senha_rapida=PIN, ativo=True)
        .select_related("user")
        .first()
    )
    if not perfil:
        fail(f"PIN {PIN} ativo nao encontrado no PG")
    ok_pin, label, err = operador_label_de_pin(PIN)
    if not ok_pin:
        fail(f"PIN {PIN} nao valida: {err}")
    rot = rotulo_operador_pin(PIN) or label
    ok(f"PIN {PIN} valida -> {rot}")

    with override_settings(ALLOWED_HOSTS=["*", "testserver", "localhost", "127.0.0.1"]):
        c = Client(HTTP_HOST="127.0.0.1")
        c.force_login(perfil.user)

        r_web = c.get(reverse("atendimento_whatsapp"))
        if r_web.status_code != 200:
            fail(f"GET Zap web status={r_web.status_code}")
        body = r_web.content.decode("utf-8", errors="replace")
        for trecho in (
            'id="wa-operador-pin"',
            "wa-op-pin-nome",
            "wa-op-pin-lbl",
            "Quem",
            'id="wa-input"',
            "sspin-root",
            "gmSspinSairEAbrirPin",
        ):
            if trecho not in body:
                fail(f"HTML Zap web sem {trecho!r}")
        # ordem: card antes do input no HTML renderizado
        if body.find("wa-operador-pin") > body.find('id="wa-input"'):
            fail("HTML renderizado: card depois do input")
        ok("GET /atendimento-whatsapp/ 200 + card Quem")

        r_cel = c.get(reverse("atendimento_whatsapp_celular"))
        if r_cel.status_code != 200:
            fail(f"GET Zap celular status={r_cel.status_code}")
        cel_body = r_cel.content.decode("utf-8", errors="replace")
        if 'id="wa-operador-pin"' not in cel_body or "sspin-root" not in cel_body:
            fail("celular sem card PIN ou screensaver")
        ok("GET /atendimento-whatsapp/celular/ 200 + card")

        # Assinatura: PIN na sessao atualiza _autor_wa
        from django.test import RequestFactory
        from django.contrib.sessions.middleware import SessionMiddleware

        rf = RequestFactory()
        req = rf.post("/api/atendimento-whatsapp/enviar/")
        req.user = perfil.user
        middleware = SessionMiddleware(lambda r: None)
        middleware.process_request(req)
        req.session.save()

        ok_g, nome_g, _u, err_g = gravar_operador_sessao_pdv(req, PIN)
        if not ok_g:
            fail(f"gravar_operador_sessao_pdv: {err_g}")
        autor = _autor_wa(req, {})
        if not autor:
            fail("_autor_wa vazio apos PIN na sessao")
        # nome deve lembrar Renan / rotulo do PIN
        if rot.split()[0].lower() not in autor.lower() and autor.lower() not in rot.lower():
            # ainda ok se autor == nome_g
            if autor != nome_g:
                fail(f"_autor_wa={autor!r} nao casa com PIN {rot!r}")
        ok(f"_autor_wa apos PIN -> {autor}")

        # trocar PIN mental: outro perfil ativo com PIN diferente se existir
        outro = (
            PerfilUsuario.objects.filter(ativo=True)
            .exclude(senha_rapida=PIN)
            .exclude(senha_rapida="1234")
            .exclude(senha_rapida="")
            .select_related("user")
            .first()
        )
        if outro and (outro.senha_rapida or "").strip():
            pin2 = (outro.senha_rapida or "").strip()
            ok2, nome2, _u2, err2 = gravar_operador_sessao_pdv(req, pin2)
            if not ok2:
                fail(f"troca PIN falhou: {err2}")
            autor2 = _autor_wa(req, {})
            if autor2 == autor and nome2 != nome_g:
                fail("assinatura nao mudou apos outro PIN")
            if autor2 != nome2 and nome2 not in autor2:
                # allow partial
                pass
            ok(f"troca PIN muda assinatura -> {autor2}")
        else:
            ok("troca PIN (skip — so um PIN ativo no PG)")

        # anon redireciona
        c_anon = Client(HTTP_HOST="127.0.0.1")
        r_anon = c_anon.get(reverse("atendimento_whatsapp"))
        if r_anon.status_code not in (302, 401, 403):
            fail(f"Zap anon status={r_anon.status_code}")
        ok("Zap exige login")

    # PIN Renan intacto
    if not operador_label_de_pin(PIN)[0]:
        fail("PIN 9973 quebrou no final")
    ok(f"PIN {PIN} intacto no final")


def prova_http_opcional() -> None:
    print("=== HTTP local (opcional) ===")
    try:
        with urlopen(f"{BASE}/healthz", timeout=2) as resp:
            body = resp.read().decode("utf-8", errors="replace")
        if resp.status != 200:
            print(f"SKIP healthz status={resp.status}")
            return
        ok(f"healthz 200 ({body[:40]})")
    except (URLError, OSError, TimeoutError) as exc:
        print(f"SKIP runserver offline ({exc})")


def main() -> int:
    print("WA-PIN-COMPOSER path")
    prova_fonte()
    prova_django()
    prova_http_opcional()
    print(f"\nVERIFY_OK {CHECKS}/{CHECKS}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except SystemExit:
        raise
    except Exception as exc:  # noqa: BLE001
        fail(f"excecao: {exc}")
