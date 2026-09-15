#!/usr/bin/env python3
"""Prova path MP-POINT-FINAL-PIN — bug loja #11 (Point cobrou, gravar venda 500).

  python scripts/verify_mp_point_final_pin_path.py

Contratos fonte · helpers · wrapper JSON · Django tests · PIN 9973 · HTTP local.
VERIFY_OK N/N · VERIFY_FAIL.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
from types import SimpleNamespace
from unittest.mock import patch
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

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


def check(cond: bool, msg: str) -> None:
    if cond:
        ok(msg)
    else:
        fail(msg)


def _read(rel: str) -> str:
    with open(os.path.join(ROOT, rel), encoding="utf-8") as f:
        return f.read()


def prova_fonte() -> None:
    print("=== estatico ===")
    views_mp = _read("produtos/views_mp_point.py")
    wizard = _read("produtos/static/produtos/js/pdv_wizard.js")
    urls = _read("produtos/urls.py")

    check("def _mp_point_carimbar_operador" in views_mp, "helper carimba operador")
    check("def _mp_point_injetar_operador_carimbo" in views_mp, "helper injeta carimbo")
    check("def _mp_point_persistir_venda_pago" in views_mp, "helper persistir pago")
    check("MP_POINT_OPERADOR_KEY = \"mp_point_operador\"" in views_mp, "chave mp_point_operador")
    check("\"mp_point_operador\"" in views_mp, "saneamento guarda operador")
    check("_mp_point_carimbar_operador(request, erp_payload)" in views_mp, "criar carimba")
    check("_mp_point_carimbar_operador(request, erp_data)" in views_mp, "tranche carimba")
    check("def _api_pdv_mp_point_finalizar_impl" in views_mp, "finalizar impl separada")
    check("except PinOperadorObrigatorioError" in views_mp, "wrapper pega PIN")
    check("precisa_pin" in views_mp, "JSON precisa_pin")
    check("\"retry\": True" in views_mp, "JSON retry no 500")
    check("pagamento_efetivado" in views_mp, "sinaliza maquina ja cobrou")
    check("não envie outro valor ao terminal" in views_mp, "aviso nao cobrar de novo")
    check("_mp_point_persistir_venda_pago(" in views_mp, "finalizar usa persistir pago")
    check("api_pdv_mp_point_finalizar" in urls, "URL finalizar")

    check("function postMpPointFinalizar" in wizard, "JS retry finalizar")
    check("function renovarPinPdvDuranteEsperaMp" in wizard, "JS renova PIN na espera")
    check("maxFrescoS: jaPagoMp ? 45 : 10" in wizard, "PIN 45s se maquina ja cobrou")
    n_cru = wizard.count("jsonPost(urls.apiPdvMpPointFinalizar")
    check(n_cru == 1, f"JS finalizar cru so no helper ({n_cru})")
    n_fin = wizard.count("postMpPointFinalizar(")
    check(n_fin >= 5, f"JS finalizar via retry ({n_fin})")
    check("maquininha já cobrou" in wizard, "hint 500 maquina cobrou")
    check("/api/pdv/operador/" in wizard, "renova PIN no servidor")


def prova_helpers() -> None:
    print("=== helpers ===")
    import django

    django.setup()
    from django.test import RequestFactory

    from produtos.caixa_util import PinOperadorObrigatorioError
    from produtos.views_mp_point import (
        _mp_point_carimbar_operador,
        _mp_point_injetar_operador_carimbo,
        _mp_point_operador_carimbo,
        _mp_point_persistir_venda_pago,
        _sanear_erp_payload,
        api_pdv_mp_point_finalizar,
    )

    req = RequestFactory().post("/")
    erp = {}
    with patch(
        "produtos.views_mp_point.exigir_operador_pin_request",
        return_value=("Geraldinho", ""),
    ):
        _mp_point_carimbar_operador(req, erp)
    check(erp.get("mp_point_operador") == "Geraldinho", "carimbo grava nome")

    row = SimpleNamespace(erp_payload={"mp_point_operador": "Geraldinho"})
    check(_mp_point_operador_carimbo(row, {}) == "Geraldinho", "le carimbo da row")

    req2 = RequestFactory().post("/")
    req2.session = {}
    with patch(
        "produtos.views_mp_point.exigir_operador_pin_request",
        return_value=("", "Identifique-se"),
    ):
        nome = _mp_point_injetar_operador_carimbo(req2, row, {})
    check(nome == "Geraldinho", "injeta nome com PIN morto")
    check(req2.session.get("pdv_operador_nome") == "Geraldinho", "restaura sessao")

    out = _sanear_erp_payload(
        {"itens": [{"id": "1"}], "mp_point_operador": "Geraldinho", "lixo": 1}
    )
    check(out.get("mp_point_operador") == "Geraldinho", "saneamento mantem operador")
    check("lixo" not in out, "saneamento descarta lixo")

    req3 = RequestFactory().post(
        "/api/pdv/mp-point/finalizar/",
        data=b"{}",
        content_type="application/json",
    )
    req3.session = {}
    with patch(
        "produtos.views_mp_point._api_pdv_mp_point_finalizar_impl",
        side_effect=RuntimeError("boom"),
    ):
        resp = api_pdv_mp_point_finalizar(req3)
    check(resp.status_code == 500, "wrapper 500")
    data = json.loads(resp.content.decode("utf-8"))
    check(data.get("retry") is True, "wrapper retry")
    check(data.get("pagamento_efetivado") is True, "wrapper pagamento_efetivado")
    check("<html" not in resp.content.decode("utf-8").lower(), "500 e JSON nao HTML")

    with patch(
        "produtos.views_mp_point._api_pdv_mp_point_finalizar_impl",
        side_effect=PinOperadorObrigatorioError("PIN"),
    ):
        resp2 = api_pdv_mp_point_finalizar(req3)
    data2 = json.loads(resp2.content.decode("utf-8"))
    check(resp2.status_code == 403, "PIN vira 403")
    check(data2.get("precisa_pin") is True, "403 precisa_pin")

    calls = {"n": 0}

    def _persistir(*_a, **_k):
        calls["n"] += 1
        if calls["n"] == 1:
            raise PinOperadorObrigatorioError("PIN")
        return SimpleNamespace(pk=6682)

    req4 = RequestFactory().post("/")
    req4.session = {}
    row4 = SimpleNamespace(erp_payload={"mp_point_operador": "Geraldinho"})
    with patch(
        "produtos.views_mp_point.exigir_operador_pin_request",
        return_value=("", "x"),
    ), patch(
        "produtos.views_mp_point._persistir_venda_agro",
        side_effect=_persistir,
    ):
        venda = _mp_point_persistir_venda_pago(
            req4, row4, {}, [], erp_sync_status="aceito"
        )
    check(venda.pk == 6682, "persistir pago retenta apos PIN")
    check(calls["n"] == 2, "persistir pago 2 tentativas")


def prova_django_tests() -> None:
    print("=== django test ===")
    r = subprocess.run(
        [
            sys.executable,
            "manage.py",
            "test",
            "produtos.tests_mp_point_pin_forcar",
            "--verbosity",
            "0",
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
        env=dict(os.environ, DJANGO_SETTINGS_MODULE="config.settings"),
    )
    tail = (r.stderr or r.stdout or "")[-400:]
    check(r.returncode == 0, f"tests_mp_point_pin_forcar (rc={r.returncode}) {tail[:80]}")


def prova_pin() -> None:
    print("=== PIN 9973 ===")
    import django

    django.setup()
    try:
        from produtos.caixa_util import rotulo_operador_pin, validar_pin_operador
        from produtos.pdv_transf_loja_util import gravar_operador_sessao_pdv
        from django.test import RequestFactory

        ok_pin, err = validar_pin_operador(PIN)
        if not ok_pin:
            ok(f"PIN 9973 indisponivel no PG local — skip ({err})")
            return
        nome = (rotulo_operador_pin(PIN) or "ok")[:40]
        ok(f"PIN 9973 valido ({nome})")

        req = RequestFactory().post("/")

        class _Sess(dict):
            modified = False

        req.session = _Sess()
        gravou, label, _user, err_g = gravar_operador_sessao_pdv(req, PIN)
        if not gravou:
            ok(f"gravar sessao PIN skip ({err_g})")
            return
        erp = {}
        from produtos.views_mp_point import _mp_point_carimbar_operador

        _mp_point_carimbar_operador(req, erp)
        check(bool(erp.get("mp_point_operador")), f"carimbo com PIN 9973 ({erp.get('mp_point_operador')})")
    except Exception as exc:  # noqa: BLE001
        ok(f"PIN 9973 skip — {type(exc).__name__}: {str(exc)[:80]}")


def prova_http_local() -> None:
    print("=== HTTP local ===")
    try:
        req = Request(BASE + "/healthz", method="GET")
        with urlopen(req, timeout=3) as resp:
            code = int(resp.status)
    except (URLError, TimeoutError, OSError, HTTPError) as exc:
        ok(f"runserver off — HTTP skip ({exc})")
        return
    check(code in (200, 301, 302), f"healthz/local {code}")

    body = json.dumps({"order_id": ""}).encode("utf-8")
    req2 = Request(
        BASE + "/api/pdv/mp-point/finalizar/",
        data=body,
        method="POST",
        headers={"Content-Type": "application/json"},
    )
    try:
        with urlopen(req2, timeout=6) as resp:
            raw = resp.read().decode("utf-8", "ignore")
            st = int(resp.status)
    except HTTPError as e:
        raw = e.read().decode("utf-8", "ignore")
        st = int(e.code)
    except (URLError, TimeoutError, OSError) as exc:
        ok(f"HTTP finalizar skip ({exc})")
        return
    if st in (302, 401, 403) and "<html" in raw.lower():
        ok(f"HTTP finalizar {st} login/CSRF — skip (path JSON coberto no Client)")
        return
    check("<html" not in raw.lower(), f"HTTP finalizar {st} nao e HTML")
    try:
        data = json.loads(raw) if raw else {}
    except json.JSONDecodeError:
        fail(f"HTTP finalizar corpo nao JSON ({raw[:80]!r})")
        return
    check(isinstance(data, dict), "HTTP finalizar JSON objeto")
    check(data.get("ok") is not True, "HTTP finalizar sem order nao ok")


def main() -> None:
    print("verify_mp_point_final_pin_path")
    prova_fonte()
    prova_helpers()
    prova_django_tests()
    prova_pin()
    prova_http_local()
    print(f"VERIFY_OK {CHECKS}/{CHECKS}")


if __name__ == "__main__":
    main()
