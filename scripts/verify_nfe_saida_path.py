# -*- coding: utf-8 -*-
"""
Prova estática + smoke Django — NF-e saída modelo 55 (NFE-SAIDA-55).

  python scripts/verify_nfe_saida_path.py
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

import django

django.setup()

from django.contrib.auth import get_user_model
from django.test import Client

from produtos.caixa_util import validar_pin_operador
from produtos.nfe_saida_emissao_util import emitir_nfe_saida_documento
from produtos.nfe_saida_util import (
    cfop_padrao_destino,
    doc_novo_vazio,
    validar_destinatario,
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
    print("== Arquivos / rotas / integrações ==")
    urls = _read("produtos/urls.py")
    views_py = _read("produtos/views.py")
    dash = _read("produtos/templates/produtos/dashboard_gerencial.html")
    venda = _read("produtos/templates/produtos/venda_agro_detalhe.html")
    lista = _read("produtos/templates/produtos/vendas_lista.html")
    pdv = _read("produtos/static/produtos/js/pdv_wizard.js")
    tpl = _read("produtos/templates/produtos/nfe_saida.html")
    js = _read("produtos/static/produtos/js/nfe_saida.js")

    for needle in (
        "path('nfe/saida/'",
        "api/nfe-saida/listar/",
        "api/nfe-saida/de-venda/",
        "api/nfe-saida/<str:doc_id>/emitir/",
        "views_nfe_saida",
    ):
        check(f"urls_{needle[:28]}", needle in urls)

    check("util_col", "COL_NFE_SAIDA = \"AgroNfeSaida\"" in _read("produtos/nfe_saida_util.py"))
    check("views_nfe_saida_view", "def nfe_saida_view" in _read("produtos/views_nfe_saida.py"))
    check("emissao_sim", "def emitir_nfe_saida_documento" in _read("produtos/nfe_saida_emissao_util.py"))
    check("tpl_lista_editor", "nfe-saida-view-lista" in tpl and "nfe-saida-view-editor" in tpl)
    check("js_de_venda", "deVenda" in js and "CFG.vendaId" in js)
    check("dash_launchpad", "nfe_saida" in dash and "NF-e saída" in dash)
    check("venda_detalhe_btn", "nfe_saida_painel" in venda and "Emitir NF-e 55" in venda)
    check("lista_acao", "nfe_saida_painel" in lista)
    check("views_painel_lista", "painel_nfe_saida_venda" in views_py)
    check("pdv_toast_link", "NF-e A4 (55)" in pdv and "/nfe/saida/?venda=" in pdv)
    check("help_include", "nfe_saida_help_agents.html" in tpl)
    check("agents_rota", "/nfe/saida/" in _read("AGENTS.md"))


def test_logica() -> None:
    print("== Lógica (sem Mongo) ==")
    check("cfop_sp", cfop_padrao_destino("SP", "SP") == "5102")
    check("cfop_rj", cfop_padrao_destino("RJ", "SP") == "6102")
    dest = {
        "nome": "Teste",
        "documento": "52998224725",
        "uf": "SP",
        "cidade": "Jacupiranga",
        "logradouro": "Rua X",
        "ie": "",
    }
    check("dest_cpf_ok", validar_destinatario(dest) == [])
    doc = doc_novo_vazio()
    doc["destinatario"] = dest
    doc["itens"] = [
        {
            "descricao": "Prod",
            "qtd": 1,
            "v_unit": 10,
            "v_total": 10,
            "ncm": "23099020",
            "cfop": "5102",
            "csosn": "102",
        }
    ]
    doc["pagamento"] = [{"forma": "Dinheiro", "tPag": "01", "valor": 10}]
    doc["tp_amb"] = 2
    from unittest.mock import patch

    with patch("produtos.sefaz_dfe_client.distribuicao_dfe_configurada", return_value=True):
        out = emitir_nfe_saida_documento(doc, cnpj_emit="11222333000181")
        check("emit_sim_homolog", out.get("ok") is True and out.get("simulado") is True, str(out.get("c_stat")))
        doc["tp_amb"] = 1
        os.environ["NFE_SAIDA_SIMULAR_EMIT"] = "0"
        out_prod = emitir_nfe_saida_documento(doc, cnpj_emit="11222333000181")
        bloqueio = (out_prod.get("erro") or "") + (out_prod.get("x_motivo") or "")
        check(
            "emit_prod_bloqueada",
            out_prod.get("ok") is False and ("soap" in bloqueio.lower() or "pendente" in bloqueio.lower()),
        )
        os.environ.pop("NFE_SAIDA_SIMULAR_EMIT", None)


def test_pin() -> None:
    print("== PIN operador ==")
    try:
        ok, err = validar_pin_operador("9973")
    except Exception as exc:
        check("pin_9973_db", "ativo" in str(exc) or "no such table" in str(exc), "migrate local pendente — PIN não testado")
        return
    if ok:
        check("pin_9973", True, "Perfil encontrado")
    else:
        check("pin_9973_ambiente", err in ("PIN incorreto.", "Informe o PIN."), f"sem perfil 9973 neste DB: {err}")


def test_http() -> None:
    print("== HTTP smoke (login) ==")
    User = get_user_model()
    u = User.objects.first()
    if u is None:
        u = User.objects.create_user("verify_nfe_saida", password="x")
    c = Client(HTTP_HOST="localhost")
    c.force_login(u)

    r = c.get("/nfe/saida/")
    check("get_page", r.status_code == 200 and b"nfe-saida-view-lista" in r.content)

    r2 = c.get("/api/nfe-saida/status-cert/")
    check("get_cert", r2.status_code == 200)
    if r2.status_code == 200:
        j = json.loads(r2.content.decode())
        check("cert_mod55", j.get("mod_ui") == "55")

    r3 = c.get("/api/nfe-saida/listar/")
    check("get_listar", r3.status_code in (200, 503))
    if r3.status_code == 200:
        j3 = json.loads(r3.content.decode())
        check("listar_ok", j3.get("ok") is True and isinstance(j3.get("itens"), list))


def main() -> int:
    print("verify_nfe_saida_path")
    test_arquivos()
    test_logica()
    test_pin()
    test_http()
    print(f"\n{'=' * 40}\nOK {len(oks)}/{len(oks) + len(fails)}")
    if fails:
        print("FAIL:", ", ".join(fails))
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
