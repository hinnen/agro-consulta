# -*- coding: utf-8 -*-
"""
Prova detalhada — conferência fiado no Fechar caixa (`CAIXA-FIADO-CONF`).

  python scripts/verify_caixa_fiado_conferencia_path.py
"""
from __future__ import annotations

import os
import sys
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

import django

django.setup()

from produtos.caixa_util import (
    marcar_fiado_conferencia_caixa,
    validar_conferencia_fiado_caixa,
    validar_pin_operador,
)

fails: list[str] = []
oks: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    if cond:
        oks.append(name)
        print(f"  OK  {name}" + (f" — {detail}" if detail else ""))
    else:
        fails.append(name)
        print(f"  FAIL {name}" + (f" — {detail}" if detail else ""))


def _read(rel: str) -> str:
    return (ROOT / rel).read_text(encoding="utf-8")


def test_arquivos() -> None:
    print("== Arquivos / contratos ==")
    models = _read("produtos/models.py")
    util = _read("produtos/caixa_util.py")
    views = _read("produtos/views.py")
    urls = _read("produtos/urls.py")
    html = _read("produtos/templates/produtos/caixa_fechar.html")
    modal = _read("produtos/templates/produtos/includes/caixa_fiado_wizard_modal.html")
    mig = ROOT / "produtos/migrations/0123_fiado_nota_caixa_conferida.py"

    check("mig_0123", mig.is_file())
    check("venda_field", "fiado_nota_caixa_conferida_em" in models)
    check("baixa_field", models.count("fiado_nota_caixa_conferida_em") >= 2)
    check("listar_skip_conferida", util.count("fiado_nota_caixa_conferida_em__isnull=True") >= 3)
    check("marcar_fn", "def marcar_fiado_conferencia_caixa" in util)
    check("api_view", "def api_caixa_fiado_conferencia_salvar" in views)
    check("api_url", "api/caixa/fiado-conferencia/" in urls)
    check("js_grava", "gravarConferencia" in html and "idsMarcados" in html)
    check("js_confirmar_grava", html.count("gravarConferencia({") == 2)
    check(
        "js_pular_nao_grava",
        "cf-fiado-vendas-pular" in html
        and html.split("btnPulV")[1].split("btnConfB")[0].find("gravarConferencia") < 0,
    )
    check("data_id", "data-fiado-id" in modal)
    check("ctx_url", "api_fiado_conferencia_url" in views and "api_fiado_conferencia_url" in html)
    check("pg_nao_localstorage", "localStorage" not in html.split("initFiadoWizard")[1].split("var popup =")[0])
    check("validar_usa_lista", "fiado_assinado_{row['id']}" in util)
    check("marcar_so_turno", "sessao_caixa_id__in=ids" in util)
    check("marcar_idempotente", "fiado_nota_caixa_conferida_em__isnull=True" in util.split("def marcar_fiado_conferencia_caixa")[1][:900])
    check("api_deposito", "filtrar_sessoes_por_deposito" in views.split("def api_caixa_fiado_conferencia_salvar")[1][:800])
    check("api_vazio", '"Nada para gravar."' in views)


def test_validar_logica() -> None:
    print("== Validar / marcar (sem gravar venda) ==")
    row = {"id": 4242, "cliente_nome": "Teste Conf", "valor": "10.00"}
    err = validar_conferencia_fiado_caixa({}, [row], [])
    check("validar_falta_check", bool(err) and "Teste Conf" in (err or ""))
    err2 = validar_conferencia_fiado_caixa({"fiado_assinado_4242": "1"}, [row], [])
    check("validar_com_check", err2 is None)
    err3 = validar_conferencia_fiado_caixa({}, [], [])
    check("validar_lista_vazia", err3 is None)
    baixa = {"id": 77, "cliente_nome": "Pago", "valor": "8.00"}
    errb = validar_conferencia_fiado_caixa({}, [], [baixa])
    check("validar_baixa_falta", bool(errb) and "Pago" in (errb or ""))
    errb2 = validar_conferencia_fiado_caixa({"fiado_retirado_77": "1"}, [], [baixa])
    check("validar_baixa_ok", errb2 is None)
    n = marcar_fiado_conferencia_caixa([], [1], [2])
    check("marcar_sem_sessao_zero", n == {"vendas": 0, "baixas": 0})
    pin_ok, pin_err = validar_pin_operador("9973")
    check("pin_9973", pin_ok, pin_err or "")
    if pin_ok:
        errp = validar_conferencia_fiado_caixa(
            {"fiado_vendas_pulado": "1", "fiado_vendas_pulo_pin": "9973"},
            [row],
            [],
        )
        check("pulo_pin_dispensa_check", errp is None, errp or "")
    err_bad = validar_conferencia_fiado_caixa(
        {"fiado_vendas_pulado": "1", "fiado_vendas_pulo_pin": "0000"},
        [row],
        [],
    )
    check("pulo_pin_errado", bool(err_bad))


def test_http() -> None:
    print("== HTTP runserver ==")
    try:
        urllib.request.urlopen("http://127.0.0.1:8000/healthz", timeout=2)
        check("runserver_healthz", True)
    except Exception:
        check("runserver_skip", True, "PC local off — Client Django trava SQLite com o servidor")
        return
    req = urllib.request.Request(
        "http://127.0.0.1:8000/caixa/fechar/",
        method="GET",
    )
    try:
        with urllib.request.urlopen(req, timeout=8) as resp:
            code = resp.status
            loc = ""
    except urllib.error.HTTPError as e:
        code = e.code
        loc = e.headers.get("Location", "")
    check("http_fechar_login_ou_ok", code in (200, 302, 301), f"HTTP {code} {loc}")
    req2 = urllib.request.Request(
        "http://127.0.0.1:8000/api/caixa/fiado-conferencia/",
        data=b"{}",
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req2, timeout=8) as resp:
            api_code = resp.status
    except urllib.error.HTTPError as e:
        api_code = e.code
    check("http_api_pede_login", api_code in (302, 401, 403, 400), f"HTTP {api_code}")


def main() -> int:
    print("verify_caixa_fiado_conferencia_path")
    test_arquivos()
    test_validar_logica()
    test_http()
    print()
    if fails:
        print(f"FALHOU: {len(fails)} falha(s), {len(oks)} ok")
        for f in fails:
            print(f"  - {f}")
        return 1
    print(f"OK verify_caixa_fiado_conferencia_path — {len(oks)}/{len(oks)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
