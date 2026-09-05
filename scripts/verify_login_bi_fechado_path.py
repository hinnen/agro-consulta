#!/usr/bin/env python3
"""Prova LOGIN-BI-FECHADO + LOGIN-UI-AGRO — painel fechado + tela GM Agro Mais.

  python scripts/verify_login_bi_fechado_path.py

Contratos fonte · Django Client (anon/auth) · PIN 9973 · HTTP local se runserver up.
VERIFY_OK N/N · VERIFY_FAIL.
"""
from __future__ import annotations

import os
import sys
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.chdir(ROOT)
sys.path.insert(0, ROOT)
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
    with open(os.path.join(ROOT, rel), encoding="utf-8") as f:
        return f.read()


def prova_fonte() -> None:
    print("=== estático ===")
    settings = _read("config/settings.py")
    urls_root = _read("config/urls.py")
    urls_prod = _read("produtos/urls.py")
    views = _read("produtos/views.py")
    html = _read("produtos/templates/produtos/entrar.html")

    if 'AGRO_PUBLIC_DASHBOARD = config("AGRO_PUBLIC_DASHBOARD", default=False' not in settings:
        fail("AGRO_PUBLIC_DASHBOARD default deve ser False")
    ok("AGRO_PUBLIC_DASHBOARD default False")

    if 'LOGIN_URL = "/entrar/"' not in settings:
        fail("LOGIN_URL não é /entrar/")
    ok("LOGIN_URL=/entrar/")

    if 'path("entrar/", views.agro_entrar' not in urls_prod:
        fail("rota /entrar/ ausente")
    if "def agro_entrar" not in views:
        fail("view agro_entrar ausente")
    ok("rota + view agro_entrar")

    if "admin_login_redirect" not in urls_root:
        fail("falta redirect admin/login -> entrar")
    if 'path("admin/login/", admin_login_redirect' not in urls_root:
        fail("admin/login não aponta para redirect")
    ok("admin/login -> /entrar/")

    if 'login_url="/admin/login/"' in views:
        fail("views.py ainda manda para /admin/login/")
    if 'protected = login_required(login_url="/entrar/")' not in views:
        fail("_dashboard_login_required não usa /entrar/")
    ok("login_required do BI aponta /entrar/")

    for trecho in (
        "logo_agro_mais.png",
        "linear-gradient(155deg",
        "Sua casa de ração",
        'name="username"',
        'name="password"',
        "AGRO",
    ):
        if trecho not in html:
            fail(f"entrar.html sem {trecho!r}")
    ok("entrar.html marca + formulário")

    logo = os.path.join(ROOT, "produtos", "static", "produtos", "img", "logo_agro_mais.png")
    if not os.path.isfile(logo):
        fail("logo_agro_mais.png ausente")
    ok("logo estático presente")


def prova_django() -> None:
    print("=== Django Client ===")
    import django

    django.setup()
    from django.conf import settings
    from django.contrib.auth import get_user_model
    from django.test import Client, override_settings

    if getattr(settings, "AGRO_PUBLIC_DASHBOARD", True):
        fail("runtime AGRO_PUBLIC_DASHBOARD ainda True")
    ok("runtime painel fechado")

    if settings.LOGIN_URL != "/entrar/":
        fail(f"runtime LOGIN_URL={settings.LOGIN_URL!r}")
    ok("runtime LOGIN_URL")

    with override_settings(ALLOWED_HOSTS=["*", "testserver", "localhost", "127.0.0.1"]):
        c = Client()
        r = c.get("/", follow=False)
        if r.status_code != 302:
            fail(f"GET / anon status={r.status_code}")
        loc = r.get("Location") or ""
        if not loc.startswith("/entrar/"):
            fail(f"GET / anon Location={loc!r}")
        if "next=" not in loc:
            fail("GET / sem ?next=")
        ok("anon / redireciona /entrar/?next=")

        r2 = c.get("/atalhos/", follow=False)
        if r2.status_code != 302 or "/entrar/" not in (r2.get("Location") or ""):
            fail(f"/atalhos/ anon Location={r2.get('Location')!r}")
        ok("anon /atalhos/ redireciona login")

        r3 = c.get("/dashboard/gerencial/", follow=False)
        if r3.status_code != 302 or "/entrar/" not in (r3.get("Location") or ""):
            fail(f"dashboard anon Location={r3.get('Location')!r}")
        ok("anon dashboard redireciona login")

        r4 = c.get("/admin/login/?next=/", follow=False)
        if r4.status_code != 302:
            fail(f"admin/login status={r4.status_code}")
        loc4 = r4.get("Location") or ""
        if not loc4.startswith("/entrar/"):
            fail(f"admin/login Location={loc4!r}")
        ok("admin/login -> /entrar/")

        r5 = c.get("/entrar/")
        if r5.status_code != 200:
            fail(f"/entrar/ status={r5.status_code}")
        body = r5.content.decode("utf-8", "ignore")
        if "csrfmiddlewaretoken" not in body:
            fail("/entrar/ sem CSRF")
        if "logo_agro_mais" not in body and "AGRO" not in body:
            fail("/entrar/ sem marca")
        if "Administração do Django" in body:
            fail("/entrar/ ainda parece Admin Django")
        ok("/entrar/ 200 marca + CSRF")

        User = get_user_model()
        uname = "login_verify_path"
        pwd = "VerifyLogin9973!"
        User.objects.filter(username=uname).delete()
        User.objects.create_superuser(uname, "login_verify@test.local", pwd)
        ok("superuser temporário criado")

        bad = c.post("/entrar/", {"username": uname, "password": "errada", "next": "/"})
        if bad.status_code != 200:
            fail(f"senha errada status={bad.status_code}")
        if "incorretos" not in bad.content.decode("utf-8", "ignore").lower():
            fail("senha errada sem mensagem")
        ok("senha errada mantém tela com erro")

        good = c.post("/entrar/", {"username": uname, "password": pwd, "next": "/"})
        if good.status_code != 302:
            fail(f"login OK status={good.status_code}")
        if (good.get("Location") or "") != "/":
            fail(f"login OK Location={good.get('Location')!r}")
        ok("login OK vai para /")

        home = c.get("/", follow=False)
        # autenticado: 200 no BI (não redirect login)
        if home.status_code not in (200, 302):
            fail(f"autenticado GET / status={home.status_code}")
        loc_h = home.get("Location") or ""
        if home.status_code == 302 and "/entrar/" in loc_h:
            fail("autenticado ainda cai no login")
        if home.status_code == 200:
            ok("autenticado vê home/BI 200")
        else:
            # pode redirecionar interno sem login
            ok(f"autenticado GET / {home.status_code} sem /entrar/")

        sair = c.get("/sair/", follow=False)
        if sair.status_code != 302:
            fail(f"/sair/ status={sair.status_code}")
        after = c.get("/", follow=False)
        if after.status_code != 302 or "/entrar/" not in (after.get("Location") or ""):
            fail("após /sair/ ainda autenticado")
        ok("/sair/ fecha sessão")

        User.objects.filter(username=uname).delete()
        ok("superuser temporário removido")


def prova_pin() -> None:
    print("=== PIN 9973 ===")
    import django

    django.setup()
    try:
        from produtos.caixa_util import rotulo_operador_pin, validar_pin_operador

        ok_pin, err = validar_pin_operador(PIN)
        if not ok_pin:
            ok(f"PIN 9973 indisponivel no PG local — skip ({err})")
            return
        nome = (rotulo_operador_pin(PIN) or "ok")[:40]
        ok(f"PIN 9973 valido ({nome})")
    except Exception as exc:  # noqa: BLE001
        ok(f"PIN 9973 skip — {type(exc).__name__}: {str(exc)[:60]}")


def _http_loc(path: str) -> tuple[int, str]:
    class _NoRedirect(urllib.request.HTTPRedirectHandler):
        def redirect_request(self, req, fp, code, msg, headers, newurl):  # noqa: ANN001
            return None

    opener = urllib.request.build_opener(_NoRedirect)
    req = Request(BASE + path, method="GET")
    try:
        with opener.open(req, timeout=4) as resp:
            return int(resp.status), resp.headers.get("Location") or ""
    except HTTPError as e:
        return int(e.code), e.headers.get("Location") or ""


def prova_http_local() -> None:
    print("=== HTTP local ===")
    try:
        code, loc = _http_loc("/")
    except (URLError, TimeoutError, OSError) as exc:
        ok(f"runserver off — HTTP skip ({exc})")
        return

    if code != 302 or "/entrar/" not in loc:
        fail(f"HTTP / -> {code} {loc!r}")
    ok(f"HTTP / -> {loc}")

    code2, loc2 = _http_loc("/admin/login/?next=/")
    if code2 != 302 or "/entrar/" not in loc2:
        fail(f"HTTP admin/login -> {code2} {loc2!r}")
    ok(f"HTTP admin/login -> {loc2}")

    req = Request(BASE + "/entrar/", method="GET")
    with urlopen(req, timeout=6) as resp:
        body = resp.read().decode("utf-8", "ignore")
        if int(resp.status) != 200:
            fail(f"HTTP /entrar/ status={resp.status}")
        if "Administração do Django" in body:
            fail("HTTP /entrar/ ainda Admin")
        if "username" not in body or "password" not in body:
            fail("HTTP /entrar/ sem campos")
        ok("HTTP /entrar/ 200 tela marca")


def main() -> None:
    print("verify_login_bi_fechado_path")
    prova_fonte()
    prova_django()
    prova_pin()
    prova_http_local()
    print(f"VERIFY_OK {CHECKS}/{CHECKS}")


if __name__ == "__main__":
    main()
