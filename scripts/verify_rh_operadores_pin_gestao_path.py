#!/usr/bin/env python3
"""Prova RH-PIN-GESTAO — operadores PIN com vínculo RH / 1234 / desativar.

  python scripts/verify_rh_operadores_pin_gestao_path.py

Fonte · migrate · util · Client HTTP · PIN 9973 · (HTTP runserver opcional).
VERIFY_OK N/N · VERIFY_FAIL.
"""
from __future__ import annotations

import os
import sys
from urllib.error import URLError
from urllib.request import urlopen

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.chdir(ROOT)
sys.path.insert(0, ROOT)
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

CHECKS = 0
PIN = "9973"
BASE = os.environ.get("AGRO_VERIFY_BASE", "http://127.0.0.1:8000").rstrip("/")
TAG = "rh-pin-gestao-verify"


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
    models = _read("base/models.py")
    util = _read("base/operador_util.py")
    mig = _read("base/migrations/0011_perfilusuario_ativo_funcionario.py")
    urls = _read("rh/urls.py")
    views = _read("rh/views.py")
    tpl = _read("rh/templates/rh/rh_operadores_pins.html")
    caixa = _read("produtos/caixa_util.py")
    estoque = _read("estoque/views.py")
    sspin = _read("produtos/templates/produtos/_screensaver_pin.html")

    for trecho in (
        'ativo = models.BooleanField',
        'funcionario = models.ForeignKey',
        '"rh.Funcionario"',
    ):
        if trecho not in models:
            fail(f"PerfilUsuario sem {trecho!r}")
    ok("modelo PerfilUsuario ativo + funcionario")

    if "0011_perfilusuario_ativo_funcionario" not in mig and "name=\"ativo\"" not in mig:
        fail("migration 0011 incompleta")
    if "rh.funcionario" not in mig and "to=\"rh.funcionario\"" not in mig:
        fail("migration 0011 sem FK funcionario")
    ok("migration base.0011")

    for nome in (
        "criar_operador",
        "vincular_funcionario",
        "desativar_operador",
        "reativar_operador",
        "resetar_pin_bootstrap",
        "buscar_funcionarios_rh",
        "PIN_BOOTSTRAP = \"1234\"",
    ):
        if nome not in util:
            fail(f"operador_util sem {nome}")
    ok("operador_util APIs")

    for rota in (
        "api_rh_operadores_lista",
        "api_rh_operadores_buscar_rh",
        "api_rh_operador_criar",
        "api_rh_operador_vincular",
        "api_rh_operador_desativar",
        "api_rh_operador_reativar",
        "api_rh_operador_reset_1234",
    ):
        if rota not in urls or rota not in views:
            fail(f"rota/view ausente: {rota}")
    ok("rotas + views RH operadores")

    for trecho in (
        "api_rh_operadores_lista",
        "api_rh_operador_criar",
        "Cadastrar PIN",
        "Mostrar saídos",
        "rh-btn-avulso",
        "1234",
    ):
        if trecho not in tpl:
            fail(f"template sem {trecho!r}")
    ok("template gestão PIN")

    if "ativo=True" not in caixa or "primeiro_acesso = False" not in caixa:
        fail("caixa_util sem filtro ativo / primeiro_acesso no bootstrap")
    if 'bootstrap) or "").strip() != "1234"' not in caixa and '!= "1234"' not in caixa:
        fail("bootstrap 1234 ausente no cadastrar_pin")
    ok("caixa_util 1234 + ativo")

    if "listar_operadores" not in estoque:
        fail("api_listar_usuarios não usa listar_operadores")
    if "ativo=True" not in estoque:
        fail("estoque PIN sem ativo=True")
    ok("estoque listar + PIN ativo")

    for trecho in ("BOOTSTRAP_PIN = '1234'", "api_cadastrar_pin_bootstrap", "pin_personalizado"):
        if trecho not in sspin:
            fail(f"screensaver sem {trecho}")
    ok("screensaver bootstrap 1234")


def _limpar_lixo() -> None:
    from django.contrib.auth.models import User

    from base.models import PerfilUsuario
    from produtos.models import ClienteAgro
    from rh.models import Funcionario

    for p in PerfilUsuario.objects.filter(user__username__startswith=f"{TAG}"):
        uid = p.user_id
        p.delete()
        User.objects.filter(pk=uid).delete()
    Funcionario.objects.filter(nome_cache__startswith=f"{TAG}").delete()
    ClienteAgro.objects.filter(nome__startswith=f"{TAG}").delete()
    User.objects.filter(username__startswith=f"{TAG}").delete()


def prova_django() -> None:
    print("=== Django / PG / Client ===")
    import django

    django.setup()
    from django.contrib.auth.models import User
    from django.test import Client, override_settings
    from django.urls import reverse

    from base.models import Empresa, PerfilUsuario
    from base.operador_util import (
        PIN_BOOTSTRAP,
        buscar_funcionarios_rh,
        criar_operador,
        desativar_operador,
        listar_operadores,
        reativar_operador,
        resetar_pin_bootstrap,
        vincular_funcionario,
    )
    from produtos.caixa_util import (
        cadastrar_pin_operador_primeira_vez,
        operador_label_de_pin,
        validar_pin_operador,
    )
    from produtos.models import ClienteAgro
    from rh.models import Funcionario

    # migrate aplicada
    campos = {f.name for f in PerfilUsuario._meta.get_fields()}
    if "ativo" not in campos or "funcionario" not in campos:
        fail("ORM sem campos ativo/funcionario — rode migrate")
    ok("ORM PerfilUsuario com ativo/funcionario")

    # PIN Renan 9973 vivo
    perfil_renan = PerfilUsuario.objects.filter(senha_rapida=PIN, ativo=True).select_related("user").first()
    if not perfil_renan:
        fail(f"PIN {PIN} não encontrado (ativo) no PG")
    ok_pin, label, err_pin = operador_label_de_pin(PIN)
    if not ok_pin:
        fail(f"PIN {PIN} não valida: {err_pin}")
    ok(f"PIN {PIN} valida -> {label}")

    _limpar_lixo()

    emp = Empresa.objects.filter(ativo=True).order_by("id").first()
    if not emp:
        fail("sem Empresa ativa no PG")
    cli = ClienteAgro.objects.create(nome=f"{TAG} Cliente RH", ativo=True)
    fun = Funcionario.objects.create(
        cliente_agro=cli,
        empresa=emp,
        nome_cache=f"{TAG} Funcionario Novo",
        ativo=True,
    )
    ok("fixture Funcionario RH criada")

    # criar a partir do RH
    ok_c, data, err_c = criar_operador(funcionario_id=fun.pk)
    if not ok_c or not data:
        fail(f"criar_operador RH: {err_c}")
    pid = int(data["id"])
    if data.get("pin_personalizado"):
        fail("novo operador já tem PIN personalizado")
    p = PerfilUsuario.objects.get(pk=pid)
    if p.senha_rapida != PIN_BOOTSTRAP or not p.primeiro_acesso or not p.ativo:
        fail("estado inicial esperado: 1234 + primeiro_acesso + ativo")
    if p.funcionario_id != fun.pk:
        fail("vínculo RH não gravou")
    ok("criar do RH -> 1234 + vínculo")

    # duplicata
    ok_dup, _, err_dup = criar_operador(funcionario_id=fun.pk)
    if ok_dup:
        fail("permitiu segundo PIN no mesmo funcionário")
    ok(f"bloqueia duplicata RH ({err_dup[:40]})")

    # busca RH marca já_tem_pin
    hits = buscar_funcionarios_rh(TAG)
    hit = next((h for h in hits if h["id"] == fun.pk), None)
    if not hit or not hit.get("ja_tem_pin"):
        fail("busca RH não marca ja_tem_pin")
    ok("busca RH ja_tem_pin")

    # 1234 não entra no caixa
    if validar_pin_operador(PIN_BOOTSTRAP)[0]:
        fail("1234 não deveria validar no caixa")
    ok("1234 bloqueado na validação")

    # bootstrap troca
    ok_b, rot, err_b = cadastrar_pin_operador_primeira_vez(pid, "4815", bootstrap="1234")
    if not ok_b:
        fail(f"bootstrap: {err_b}")
    if not validar_pin_operador("4815")[0]:
        fail("PIN novo não valida")
    p.refresh_from_db()
    if p.primeiro_acesso or p.senha_rapida == PIN_BOOTSTRAP:
        fail("após bootstrap ainda primeiro_acesso/1234")
    ok(f"bootstrap 1234->4815 ({rot})")

    # reset 1234
    ok_r, err_r = resetar_pin_bootstrap(pid)
    if not ok_r:
        fail(err_r)
    p.refresh_from_db()
    if p.senha_rapida != PIN_BOOTSTRAP or not p.primeiro_acesso:
        fail("reset não voltou 1234")
    if validar_pin_operador("4815")[0]:
        fail("PIN antigo ainda valida após reset")
    ok("reset -> 1234 e invalida PIN antigo")

    # desativar
    cadastrar_pin_operador_primeira_vez(pid, "4816", bootstrap="1234")
    ok_off, err_off = desativar_operador(pid)
    if not ok_off:
        fail(err_off)
    if any(x["id"] == pid for x in listar_operadores(incluir_inativos=False)):
        fail("ainda na lista ativa")
    if validar_pin_operador("4816")[0]:
        fail("PIN de inativo ainda valida")
    # Renan intacto
    if not validar_pin_operador(PIN)[0]:
        fail("PIN 9973 quebrou após desativar outro")
    ok("desativar some da lista e bloqueia PIN (9973 ok)")

    # reativar -> 1234
    ok_on, err_on = reativar_operador(pid)
    if not ok_on:
        fail(err_on)
    p.refresh_from_db()
    if not p.ativo or p.senha_rapida != PIN_BOOTSTRAP:
        fail("reativar não voltou 1234/ativo")
    ok("reativar -> 1234")

    # avulso + vincular
    ok_a, data_a, err_a = criar_operador(nome=f"{TAG} Avulso")
    if not ok_a or not data_a:
        fail(f"criar avulso: {err_a}")
    pid2 = int(data_a["id"])
    # outro func livre
    cli2 = ClienteAgro.objects.create(nome=f"{TAG} Cliente 2", ativo=True)
    fun2 = Funcionario.objects.create(
        cliente_agro=cli2,
        empresa=emp,
        nome_cache=f"{TAG} Func Vincular",
        ativo=True,
    )
    ok_v, data_v, err_v = vincular_funcionario(pid2, fun2.pk)
    if not ok_v:
        fail(f"vincular: {err_v}")
    if not data_v or data_v.get("funcionario_id") != fun2.pk:
        fail("vínculo avulso falhou")
    ok("criar avulso + vincular RH")

    # HTTP Client
    with override_settings(ALLOWED_HOSTS=["*", "testserver", "localhost", "127.0.0.1"]):
        user = perfil_renan.user
        c = Client(HTTP_HOST="127.0.0.1")
        c.force_login(user)

        r_page = c.get(reverse("rh_operadores_pins"))
        if r_page.status_code != 200:
            fail(f"GET operadores status={r_page.status_code}")
        body = r_page.content.decode("utf-8", errors="replace")
        if "api_rh_operadores_lista" not in body and "operadores/api/lista" not in body:
            fail("página sem API lista")
        ok("GET /rh/operadores/ 200")

        r_lista = c.get(reverse("api_rh_operadores_lista"))
        j = r_lista.json()
        if not j.get("ok") or not any(u.get("id") == pid for u in j.get("usuarios") or []):
            fail("API lista sem operador de teste")
        ok("API lista")

        r_busca = c.get(reverse("api_rh_operadores_buscar_rh"), {"q": TAG})
        jb = r_busca.json()
        if not jb.get("ok") or not jb.get("funcionarios"):
            fail("API busca RH vazia")
        ok("API busca RH")

        # criar via API (nome avulso) + desativar
        r_criar = c.post(reverse("api_rh_operador_criar"), {"nome": f"{TAG} Http"})
        jc = r_criar.json()
        if not jc.get("ok"):
            fail(f"API criar: {jc}")
        pid_http = int(jc["usuario"]["id"])
        ok("API criar")

        r_rst = c.post(reverse("api_rh_operador_reset_1234"), {"perfil_id": str(pid_http)})
        if not r_rst.json().get("ok"):
            fail(f"API reset: {r_rst.json()}")
        ok("API reset 1234")

        # definir PIN via RH (!= 1234)
        r_pin = c.post(
            reverse("api_definir_pin_rh"),
            {"perfil_id": str(pid_http), "novo_pin": "7391"},
        )
        if not r_pin.json().get("ok"):
            fail(f"api_definir_pin_rh: {r_pin.json()}")
        if not validar_pin_operador("7391")[0]:
            fail("PIN definido pelo RH não valida")
        ok("api_definir_pin_rh")

        r_off = c.post(reverse("api_rh_operador_desativar"), {"perfil_id": str(pid_http)})
        if not r_off.json().get("ok"):
            fail(f"API desativar: {r_off.json()}")
        if validar_pin_operador("7391")[0]:
            fail("após API desativar PIN ainda valida")
        ok("API desativar bloqueia PIN")

        r_on = c.post(reverse("api_rh_operador_reativar"), {"perfil_id": str(pid_http)})
        if not r_on.json().get("ok"):
            fail(f"API reativar: {r_on.json()}")
        p_http = PerfilUsuario.objects.get(pk=pid_http)
        if p_http.senha_rapida != PIN_BOOTSTRAP:
            fail("API reativar sem 1234")
        ok("API reativar -> 1234")

        # listar usuarios (screensaver) só ativos
        r_usu = c.get(reverse("api_listar_usuarios"))
        ju = r_usu.json()
        if not ju.get("ok"):
            fail(f"api_listar_usuarios: {ju}")
        ids_ativos = {u["id"] for u in ju.get("usuarios") or []}
        if pid_http not in ids_ativos:
            # reativado deve aparecer
            fail("reativado não está em api_listar_usuarios")
        ok("api_listar_usuarios inclui ativos")

        # anon bloqueado nas APIs RH (login_required)
        c_anon = Client(HTTP_HOST="127.0.0.1")
        r_anon = c_anon.get(reverse("api_rh_operadores_lista"))
        if r_anon.status_code not in (302, 401, 403):
            # Django login_required -> 302
            fail(f"API lista anon status={r_anon.status_code}")
        ok("API lista exige login")

    # 9973 intacto no fim
    if not validar_pin_operador(PIN)[0]:
        fail("PIN 9973 quebrado no final")
    ok(f"PIN {PIN} intacto no final")

    _limpar_lixo()
    ok("cleanup fixtures")


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
        return


def main() -> int:
    print("RH-PIN-GESTAO path")
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
        fail(f"exceção: {exc}")
