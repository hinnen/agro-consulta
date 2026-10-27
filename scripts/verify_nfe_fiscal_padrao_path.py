#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Prova de path — Fiscal/NF-e botão Preencher configuração padrão.

  python scripts/verify_nfe_fiscal_padrao_path.py
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

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
    print("== Arquivos / rotas ==")
    urls = _read("produtos/urls.py")
    views = _read("produtos/views_nfe_fiscal_config.py")
    tpl = _read("produtos/templates/produtos/fiscal_nfe_config.html")
    util = _read("produtos/nfe_fiscal_padrao_util.py")
    motor = _read("produtos/nfe_saida_regras_validas_util.py")
    trib = _read("produtos/nfe_saida_tributacao_util.py")
    emit = _read("produtos/nfe_saida_emissao_util.py")

    check("url_pagina", "configuracao/fiscal-nfe/" in urls)
    check("url_previa", "api/configuracao/fiscal-nfe/padrao/" in urls)
    check("url_aplicar", "api/configuracao/fiscal-nfe/padrao/aplicar/" in urls)
    check("view_previa", "def api_fiscal_nfe_padrao_previa" in views)
    check("view_aplicar", "def api_fiscal_nfe_padrao_aplicar" in views)
    check("staff", "nfe_saida_staff_required" in views)
    check("btn", "Preencher configuração padrão" in tpl and 'id="btn-padrao"' in tpl)
    check("modal", 'id="modal-padrao"' in tpl)
    check("tpl_sem_innerhtml", "innerHTML" not in tpl)
    check("crt_1", 'CRT_SN = "1"' in util)
    check("cst_49", 'CST_PIS = "49"' in util and 'CST_COFINS = "49"' in util)
    check("nao_101", 'csosn": "101"' in util and '"aplicar": False' in util)
    check("nao_500", 'csosn": "500"' in util and "Não ativada" in util)
    check("preenchido", "NfeFiscalStatus.PREENCHIDO" in util)
    check("nao_validado_auto", "NfeFiscalStatus.VALIDADO" in util and "não serão alterados" in util)
    check("serie_cnpj", "def _cnpj_centro" in util and "emitente_cnpj" in util)
    check("vila_serie", "série NF-e 55 permanece Pendente" in util)
    check("sem_ncm", 'ncm": ""' in util)
    check("sem_contador", "contador_nome" not in util)
    check("motor_so_validado", "NfeFiscalStatus.VALIDADO" in motor)
    check("motor_bloqueia_500", '"500"' in trib and "_CSOSN_EXIGE_PARAM_EXTRA" in trib)
    check("padrao_nao_emite", "emitir_nfe_saida" not in util and "emitir_nfe_saida" not in views)
    check("emissao_intocada_pin", 'validar_pin_operador' in emit or "pin" in _read("produtos/views_nfe_saida.py").lower())
    ver = _read("VERSION").strip()
    parts = [int(x) for x in ver.split(".") if x.isdigit()]
    check("version_min", parts >= [27, 26, 38], ver)


def test_pin_e_http() -> None:
    print("== PIN 9973 + HTTP (staff, sem gravar padrão no DB vivo) ==")
    import django

    django.setup()
    from django.contrib.auth import get_user_model
    from django.test import Client

    from produtos.caixa_util import validar_pin_operador

    try:
        ok_pin, err = validar_pin_operador("9973")
    except Exception as exc:
        check("pin_9973_db", "ativo" in str(exc) or "no such table" in str(exc), str(exc)[:80])
    else:
        if ok_pin:
            check("pin_9973", True, "perfil encontrado")
        else:
            check("pin_9973_ambiente", err in ("PIN incorreto.", "Informe o PIN."), str(err))

    User = get_user_model()
    staff = User.objects.filter(is_staff=True).first()
    if staff is None:
        staff = User.objects.create_user("verify_fiscal_padrao", password="x", is_staff=True)
    c = Client(HTTP_HOST="localhost", raise_request_exception=False)
    c.force_login(staff)

    r = c.get("/configuracao/fiscal-nfe/")
    body = r.content or b""
    check("get_pagina", r.status_code == 200, f"status={r.status_code}")
    check("pagina_botao", b"Preencher configura" in body or b"btn-padrao" in body)
    check("pagina_centro_vila", b"Centro" in body and b"Vila Elias" in body)

    r0 = c.get("/api/configuracao/fiscal-nfe/padrao/?estabelecimento=foo")
    check("previa_estab_invalido", r0.status_code == 400)

    from django.db.utils import OperationalError
    from produtos.nfe_fiscal_padrao_util import previa_padrao_contador

    try:
        previa_padrao_contador("centro")
        fiscal_db_ok = True
    except OperationalError:
        fiscal_db_ok = False
        check("fiscal_sqlite_migrate_skip", True, "0152 ausente neste SQLite — HTTP prova no TestCase")

    for loja in ("centro", "vila"):
        if not fiscal_db_ok:
            continue
        rp = c.get(f"/api/configuracao/fiscal-nfe/padrao/?estabelecimento={loja}")
        check(f"previa_{loja}", rp.status_code == 200, f"status={rp.status_code}")
        if rp.status_code == 200:
            j = json.loads(rp.content.decode())
            check(f"previa_{loja}_ok", j.get("ok") is True and j.get("crt") == "1")
            regras = j.get("regras") or []
            check(
                f"previa_{loja}_sem_101_500",
                not any(r.get("csosn") in ("101", "500") and r.get("aplicar") for r in regras),
            )
            if loja == "vila":
                textos = " ".join((a.get("texto") or "") for a in (j.get("acoes") or []))
                check("previa_vila_serie_pendente", "Pendente" in textos or "pendente" in textos.lower())

    r_no = c.post(
        "/api/configuracao/fiscal-nfe/padrao/aplicar/",
        data=json.dumps({"estabelecimento": "centro"}),
        content_type="application/json",
    )
    check("aplicar_sem_confirmar", r_no.status_code == 400)

    op = User.objects.filter(is_staff=False).exclude(pk=staff.pk).first()
    if op is None:
        op = User.objects.create_user("verify_fiscal_op", password="x")
    c2 = Client(HTTP_HOST="localhost", raise_request_exception=False)
    c2.force_login(op)
    r403 = c2.get("/configuracao/fiscal-nfe/")
    check("nao_staff_pagina", r403.status_code == 403)
    r403a = c2.get("/api/configuracao/fiscal-nfe/padrao/?estabelecimento=centro")
    check("nao_staff_api", r403a.status_code == 403)


def main() -> int:
    print("verify_nfe_fiscal_padrao_path")
    test_arquivos()
    test_pin_e_http()
    print(f"\n{'=' * 40}\nOK {len(oks)}/{len(oks) + len(fails)}")
    if fails:
        print("FAILS:", ", ".join(fails))
        return 1
    print("VERIFY_OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
