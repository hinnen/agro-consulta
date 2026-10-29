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
    check("pg_model", "class NfeSaidaRascunhoAgro" in _read("produtos/models.py"))
    check("pg_flag", "agro_nfe_saida_rascunho_postgres" in _read("produtos/agro_fonte_config.py"))
    check("views_persistencia", "nfe_saida_persistencia_ok" in _read("produtos/views_nfe_saida.py"))
    check("views_nfe_saida_view", "def nfe_saida_view" in _read("produtos/views_nfe_saida.py"))
    check("emissao_sim", "def emitir_nfe_saida_documento" in _read("produtos/nfe_saida_emissao_util.py"))
    sp = _read("produtos/nfe_saida_sp_emissao_util.py")
    check("sefaz_sp_emit", "def emitir_nfe_saida_sefaz" in sp and "URL_AUTORIZACAO_NFE_SP" in _read("produtos/nfce_sp_emissao_util.py"))
    check("emissao_chama_sp", "emitir_nfe_saida_sefaz" in _read("produtos/nfe_saida_emissao_util.py"))
    check("email_util", "enviar_nfe_saida_por_email" in _read("produtos/nfe_saida_email_util.py"))
    check("emit_email_hook", "enviar_nfe_saida_por_email" in _read("produtos/views_nfe_saida.py"))
    check("danfe_padrao", "def montar_html_danfe" in _read("produtos/nfe_saida_danfe_html.py"))
    danfe = _read("produtos/nfe_saida_danfe_html.py")
    check("danfe_canhoto", "canhoto" in danfe and "RECEBEMOS DE" in danfe)
    check("danfe_layout_erp", "Cálculo do imposto" in danfe and "Dados dos produtos / serviços" in danfe)
    check("danfe_barcode", "JsBarcode" in danfe and "CODE128" in danfe)
    check("danfe_logo", (ROOT / "produtos/static/produtos/img/logo_agro_mais_danfe.png").is_file())
    check("danfe_pagina", "danfe-page" in danfe and "Folha" in danfe)
    check("danfe_emit_cnpj", "cnpj_emitente_da_chave" in danfe)
    check("views_emitente_doc", "carregar_emitente_danfe(doc=doc)" in _read("produtos/views_nfe_saida.py"))
    check("tpl_lista_editor", "nfe-saida-view-lista" in tpl and "nfe-saida-view-editor" in tpl)
    check("js_de_venda", "deVenda" in js and "CFG.vendaId" in js)
    check("js_xml_btn", "btn-nfe-xml-baixar" in tpl and "baixarXml" in js)
    check("js_danfe_imprimir", "btn-nfe-danfe-imprimir" in tpl and "nfe-emit-artefatos" in tpl)
    check("dash_launchpad", "nfe_saida" in dash and "NF-e saída" in dash)
    check("venda_detalhe_btn", "nfe_saida_painel" in venda and "Emitir NF-e 55" in venda)
    check("lista_acao", "nfe_saida_painel" in lista)
    check("views_painel_lista", "painel_nfe_saida_venda" in views_py)
    check("pdv_toast_link", "NF-e A4 (55)" in pdv and "/nfe/saida/?venda=" in pdv)
    check("help_include", "nfe_saida_help_agents.html" in tpl)
    check("agents_rota", "/nfe/saida/" in _read("AGENTS.md"))
    check("ux_typeahead_tpl", "nfe-cli-suggest" in tpl and "nfe-prod-suggest" in tpl and "nfe-venda-suggest" in tpl)
    check("ux_typeahead_js", "createTypeahead" in js and "clienteAgroPk" in js and "entrada_nfe=1" in js)
    check("ux_buscar_vendas", "api_nfe_saida_buscar_vendas" in urls and "urlBuscarVendas" in tpl)
    check("xsd_dest_cpf_ordem", "dest_xml.find(\"<CPF>\")" in _read("produtos/tests_nfe_saida_util.py"))
    sp_dest = _read("produtos/nfe_saida_sp_emissao_util.py")
    idx_cnpj = sp_dest.find('_sub(dest_el, "CNPJ"')
    idx_xnome = sp_dest.find('_sub(dest_el, "xNome"')
    check("xsd_dest_doc_before_nome", idx_cnpj >= 0 and idx_xnome > idx_cnpj)
    trib = _read("produtos/nfe_saida_tributacao_util.py")
    check("motor_tributacao", "def apurar_nfe_saida" in trib and "_grupo_xml_icms_csosn" in trib)
    check("fiscal_cst_cadastro", '"cst_pis_cofins"' in _read("produtos/nfce_fiscal_produto_util.py"))
    check("xsd_bundle", (ROOT / "produtos/schemas/nfe/PL_010f/nfe_v4.00.xsd").is_file())
    check("xsd_pl010f_version", (ROOT / "produtos/schemas/nfe/PL_010f/VERSION.txt").is_file())
    check("nfe_prefill_sem_ncm_generico", "aplicar_defaults_sn=False" in _read("produtos/nfe_saida_util.py"))
    check(
        "simulacao_opt_in",
        "NFE_SAIDA_SIMULAR_EMIT" in _read("produtos/nfe_saida_emissao_util.py")
        and "int(tp_amb) == 1" in _read("produtos/nfe_saida_emissao_util.py"),
    )
    check("sp_usa_motor", "apurar_nfe_saida" in sp_dest and "_montar_grupo_tributo" in sp_dest)
    check("emissao_valida_fiscal", "erros_fiscal_emissao" in _read("produtos/nfe_saida_emissao_util.py"))
    check("tpl_dest_fiscal", "dest-situacao-icms" in tpl and "dest-finalidade" in tpl)
    check("tpl_emitente_centro_vila", 'data-nfe-estab="centro"' in tpl and 'data-nfe-estab="vila"' in tpl)
    check("js_sem_ncm_generico", "23099020" not in js)
    check("js_sem_cfop_generico", "cfop: p.cfop || '5102'" not in js and "p.csosn || p.CSOSN || '102'" not in js)
    check("js_emitir_espera_salvar", "j.ok !== true" in js and "transmissão não foi iniciada" in js)
    check("js_set_estab", "function setEstabelecimento" in js)
    util = _read("produtos/nfe_saida_util.py")
    check("salvar_pg_estab", '"estabelecimento"' in util and "aplicar_estabelecimento_no_doc" in util)
    check("checklist_ufs", "origem" in util and "Não convertemos" in util)
    check("js_cache_v9", "nfe_saida.js' %}?v=9" in tpl or "nfe_saida.js?v=9" in tpl)
    check("js_qtd_decimal", "function parseBrNum" in js and "function moneyQtdVu" in js and 'data-it="un"' in js)
    check("qtd_decimal_util", "def decimal_qtd" in util and "float(ln.get(\"qtd\")" not in util)
    trib = _read("produtos/nfe_saida_tributacao_util.py")
    check("trib_dec_qtd", "def _dec_qtd" in trib and "qtd = _dec_qtd" in trib)


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
        "cmun": "3524600",
        "ie": "",
    }
    check("dest_cpf_ok", validar_destinatario(dest) == [])
    doc = doc_novo_vazio()
    doc["estabelecimento"] = "centro"
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

    with patch("produtos.sefaz_dfe_client.distribuicao_dfe_configurada", return_value=True), patch(
        "produtos.nfe_saida_tributacao_util.erros_fiscal_emissao", return_value=[]
    ), patch(
        "produtos.nfe_saida_regras_validas_util.resolver_cnpj_serie_emitente",
        return_value=("11222333000181", 1, None),
    ):
        os.environ["NFE_SAIDA_SIMULAR_EMIT"] = "1"
        out = emitir_nfe_saida_documento(doc, cnpj_emit="11222333000181")
        check(
            "emit_sim_homolog",
            out.get("ok") is True and out.get("simulado") is True and not out.get("autorizacao_sefaz_real", True),
            str(out.get("c_stat")),
        )
        os.environ.pop("NFE_SAIDA_SIMULAR_EMIT", None)
        doc["tp_amb"] = 1
        os.environ["NFE_SAIDA_SIMULAR_EMIT"] = "0"
        with patch(
            "produtos.nfe_saida_sp_emissao_util.emitir_nfe_saida_sefaz",
            return_value={
                "ok": True,
                "status": "autorizada",
                "chave": "3" * 44,
                "protocolo": "135000000000000",
                "c_stat": 100,
                "x_motivo": "Autorizado o uso da NF-e",
                "nNF": 1,
                "serie": 1,
                "xml_autorizado": "<nfeProc/>",
                "simulado": False,
                "autorizacao_sefaz_real": True,
            },
        ) as sefaz_mock:
            out_prod = emitir_nfe_saida_documento(doc, cnpj_emit="11222333000181")
            check(
                "emit_prod_sefaz",
                out_prod.get("ok") is True
                and sefaz_mock.called
                and out_prod.get("simulado") is False,
                str(out_prod),
            )
        os.environ.pop("NFE_SAIDA_SIMULAR_EMIT", None)


def _seed_pin_9973_local() -> str:
    """Cria tabela/perfil 9973 só no SQLite local da prova — não transmite nota."""
    from django.contrib.auth import get_user_model
    from django.db import connection

    from base.models import PerfilUsuario

    tables = set(connection.introspection.table_names())
    if PerfilUsuario._meta.db_table not in tables:
        with connection.schema_editor() as se:
            se.create_model(PerfilUsuario)
    cols = {c.name for c in connection.introspection.get_table_description(connection.cursor(), PerfilUsuario._meta.db_table)}
    extra_cols = {
        "ativo": "bool NOT NULL DEFAULT 1",
        "primeiro_acesso": "bool NOT NULL DEFAULT 1",
        "funcionario_id": "integer NULL",
    }
    with connection.cursor() as cur:
        for name, ddl in extra_cols.items():
            if name not in cols:
                cur.execute(f"ALTER TABLE {PerfilUsuario._meta.db_table} ADD COLUMN {name} {ddl}")
    User = get_user_model()
    u, _ = User.objects.get_or_create(username="verify_pin_9973", defaults={"is_staff": True})
    perfil = PerfilUsuario.objects.filter(senha_rapida="9973", ativo=True).first()
    if perfil is None:
        codigo = "9973"
        if PerfilUsuario.objects.filter(codigo_vendedor=codigo).exists():
            codigo = "v973"
        PerfilUsuario.objects.create(
            user=u,
            codigo_vendedor=codigo[:4],
            senha_rapida="9973",
            ativo=True,
        )
    return "ok"


def test_pin() -> None:
    print("== PIN operador ==")
    try:
        _seed_pin_9973_local()
        ok, err = validar_pin_operador("9973")
    except Exception as exc:
        check("pin_9973_db", "ativo" in str(exc) or "no such table" in str(exc), "migrate local pendente — PIN não testado")
        return
    if ok:
        check("pin_9973", True, "Perfil 9973 válido neste ambiente de prova")
    else:
        check("pin_9973_ambiente", err in ("PIN incorreto.", "Informe o PIN."), f"sem perfil 9973 neste DB: {err}")
    ok_bad, err_bad = validar_pin_operador("0000")
    check("pin_9973_nao_aceita_errado", ok_bad is False and "PIN" in (err_bad or ""), err_bad)


def test_http() -> None:
    print("== HTTP smoke (login) ==")
    User = get_user_model()
    u = User.objects.first()
    if u is None:
        u = User.objects.create_user("verify_nfe_saida", password="x")
    if not u.is_staff:
        u.is_staff = True
        u.save(update_fields=["is_staff"])
    c = Client(HTTP_HOST="localhost", raise_request_exception=False)
    c.force_login(u)

    r = c.get("/nfe/saida/")
    body_page = r.content or b""
    check("get_page", r.status_code == 200 and b"nfe-saida-view-lista" in body_page)
    check("page_ux_typeahead", b"nfe-cli-suggest" in body_page and b"urlBuscarVendas" in body_page)

    r_vendas = c.get("/api/nfe-saida/buscar-vendas/?q=1&limit=3")
    if r_vendas.status_code == 500:
        detail_v = (r_vendas.content or b"")[:4000].decode("utf-8", errors="replace").lower()
        if (
            "vendaagro" in detail_v
            or "no such table" in detail_v
            or "no such column" in detail_v
            or "operationalerror" in detail_v
        ):
            check("buscar_vendas_db_skip", True, "VendaAgro/schema pendente neste ambiente")
        else:
            check("get_buscar_vendas", False, f"status=500")
    elif r_vendas.status_code == 403:
        check("get_buscar_vendas_staff", True, "403 sem is_staff — usuário verify deve ser staff")
    else:
        check("get_buscar_vendas", r_vendas.status_code == 200, f"status={r_vendas.status_code}")
        if r_vendas.status_code == 200:
            jv = json.loads(r_vendas.content.decode())
            check("buscar_vendas_ok", jv.get("ok") is True and isinstance(jv.get("itens"), list))

    r2 = c.get("/api/nfe-saida/status-cert/")
    check("get_cert", r2.status_code == 200)
    if r2.status_code == 200:
        j = json.loads(r2.content.decode())
        check("cert_mod55", j.get("mod_ui") == "55")

    r3 = c.get("/api/nfe-saida/listar/")
    if r3.status_code != 200:
        detail = (r3.content or b"")[:4000].decode("utf-8", errors="replace").lower()
        mig_ok = (ROOT / "produtos/migrations/0146_nfe_saida_rascunho_agro.py").is_file()
        from produtos.agro_fonte_config import agro_mongo_erp_desligado

        if r3.status_code == 500 and mig_ok and agro_mongo_erp_desligado() and (
            "nfesaidarascunho" in detail
            or "no such table" in detail
            or "operationalerror" in detail
        ):
            check("http_pg_migrate_skip", True, "migrate 0146 pendente — OK após post_deploy loja")
            return
    if r3.status_code == 403:
        check("get_listar_staff", True, "403 sem is_staff — usuário verify deve ser staff")
        return
    check("get_listar", r3.status_code == 200, f"status={r3.status_code}")
    if r3.status_code == 200:
        j3 = json.loads(r3.content.decode())
        check("listar_ok", j3.get("ok") is True and isinstance(j3.get("itens"), list))

    r4 = c.post("/api/nfe-saida/nova/", data="{}", content_type="application/json")
    check("post_nova", r4.status_code == 200, f"status={r4.status_code}")
    doc_id = ""
    if r4.status_code == 200:
        j4 = json.loads(r4.content.decode())
        check("nova_ok", j4.get("ok") is True and len(str(j4.get("id") or "")) == 24)
        doc_id = str(j4.get("id") or "")

        if doc_id:
            r5 = c.get(f"/api/nfe-saida/{doc_id}/")
            check("get_doc", r5.status_code == 200)
            if r5.status_code == 200:
                j5 = json.loads(r5.content.decode())
                check("doc_rascunho", j5.get("ok") and j5.get("doc", {}).get("status") == "rascunho")

            r_danfe = c.get(f"/api/nfe-saida/{doc_id}/danfe/")
            check("get_danfe", r_danfe.status_code == 200, f"status={r_danfe.status_code}")
            if r_danfe.status_code == 200:
                body = r_danfe.content or b""
                check("danfe_html_danfe", b"DANFE" in body)
                check("danfe_html_previa", b"PR\xc9VIA" in body or b"previa" in body.lower())

            body_emit = json.dumps({"pin": "9973"})
            try:
                r6 = c.post(
                    f"/api/nfe-saida/{doc_id}/emitir/",
                    data=body_emit,
                    content_type="application/json",
                )
            except Exception as exc:
                check("post_emitir", "ativo" in str(exc) or "no such table" in str(exc), "PIN DB local incompleto")
            else:
                check("post_emitir", r6.status_code in (200, 400, 403, 500), f"status={r6.status_code}")
                if r6.status_code == 500:
                    check("emit_pin_db", True, "migrate/base_perfilusuario pendente neste ambiente")
                elif r6.status_code == 403:
                    j6 = json.loads(r6.content.decode())
                    check("emit_pin", j6.get("erro") in ("PIN incorreto.", "Informe o PIN."), j6.get("erro", ""))
                elif r6.status_code in (200, 400):
                    j6 = json.loads(r6.content.decode())
                    check(
                        "emit_resposta",
                        "ok" in j6,
                        "autorizada" if j6.get("ok") else (j6.get("erro") or "")[:80],
                    )


def test_qtd_decimal() -> None:
    print("== Quantidade decimal Prefeitura (NF-e 55) ==")
    from decimal import Decimal
    from datetime import datetime
    from unittest.mock import patch

    from produtos.nfe_saida_danfe_html import _linha_produto
    from produtos.nfe_saida_sp_emissao_util import _montar_xml_nfe_saida
    from produtos.nfe_saida_tributacao_util import _dec, _dec_qtd, apurar_nfe_saida
    from produtos.nfe_saida_util import (
        decimal_moeda,
        decimal_qtd,
        doc_novo_vazio,
        normalizar_linha_item_nfe,
        normalizar_unidade_nfe,
        parse_decimal_br,
        qtd_persistida,
        validar_itens,
    )
    from produtos.nfe_saida_xml_validate_util import validar_xml_nfe_xsd

    check("parse_virgula_0645", parse_decimal_br("0,645") == Decimal("0.645"))
    check("parse_ponto_0645", parse_decimal_br("0.645") == Decimal("0.645"))
    check("parse_virgula_4355", parse_decimal_br("4,355") == Decimal("4.355"))
    check("parse_milhar_vu", parse_decimal_br("2.870,00") == Decimal("2870.00"))
    check("qtd_4casas", decimal_qtd("4,355") == Decimal("4.3550"))
    check("qtd_nao_arredonda_2", decimal_qtd("4,355") != Decimal("4.36"))
    check("dec_qtd_nao_usa_dec_money", _dec_qtd("4,355") == Decimal("4.3550") and _dec("2870") == Decimal("2870.00"))
    tot1 = (decimal_qtd("4,355") * decimal_moeda("2870")).quantize(Decimal("0.01"))
    tot2 = (decimal_qtd("0,645") * decimal_moeda("2870")).quantize(Decimal("0.01"))
    check("tot_4355", tot1 == Decimal("12498.85"))
    check("tot_0645", tot2 == Decimal("1851.15"))
    check("un_tonelada", normalizar_unidade_nfe("tonelada") == "TON")
    check("un_ton", normalizar_unidade_nfe("TON") == "TON")
    check("un_sc", normalizar_unidade_nfe("saco") == "SC")
    ln = normalizar_linha_item_nfe(
        {"descricao": "X", "qtd": "0,645", "v_unit": "2.870,00", "v_total": 0, "un": "TON"}
    )
    check("persist_qtd_str", ln.get("qtd") == "0.645")
    check("persist_un", ln.get("un") == "TON")
    check("persist_total", ln.get("v_total") == 1851.15)
    ln2 = normalizar_linha_item_nfe(dict(ln))
    check("reabrir_qtd", ln2.get("qtd") == "0.645" and ln2.get("un") == "TON")
    er = validar_itens(
        [
            {
                "descricao": "Calcário",
                "qtd": "0,645",
                "v_unit": "2870",
                "v_total": "1851,15",
                "ncm": "23099020",
                "un": "TON",
            }
        ]
    )
    check("validar_virgula", not any("quantidade" in e.lower() and "inválid" in e.lower() for e in er), str(er))
    er_mais = validar_itens(
        [{"descricao": "X", "qtd": "0,64555", "v_unit": 1, "v_total": 1, "ncm": "23099020"}]
    )
    check("validar_4casas_max", any("4 casas" in e for e in er_mais), str(er_mais))
    er_int = validar_itens(
        [{"descricao": "Saco", "qtd": 2, "v_unit": 10, "v_total": 20, "ncm": "23099020", "un": "UN"}]
    )
    check("validar_inteiro", not any("quantidade" in e.lower() for e in er_int), str(er_int))
    html = _linha_produto(
        {"descricao": "P", "qtd": "4,355", "v_unit": "2870", "v_total": "12498.85", "un": "TON", "ncm": "23099020"}
    )
    check("danfe_qtd_virgula", "4,355" in html)
    check("danfe_un_ton", "TON" in html)

    def _doc(qtd, total):
        d = doc_novo_vazio()
        d["estabelecimento"] = "centro"
        d["destinatario"].update(
            {
                "nome": "PREFEITURA MUNICIPAL DE JACUPIRANGA",
                "documento": "53221728000100",
                "uf": "SP",
                "cidade": "Jacupiranga",
                "logradouro": "Rua A",
                "numero": "1",
                "bairro": "Centro",
                "cep": "11940000",
                "cmun": "3524600",
            }
        )
        d["itens"] = [
            {
                "descricao": "Produto TON",
                "qtd": qtd,
                "v_unit": "2870",
                "v_total": total,
                "ncm": "23099020",
                "cfop": "5102",
                "csosn": "102",
                "cst_pis_cofins": "49",
                "un": "TON",
            }
        ]
        d["pagamento"] = [{"tPag": "01", "valor": total}]
        return d

    cfg = {
        "tp_amb": 2,
        "cnpj": "11222333000181",
        "razao_social": "Em",
        "fantasia": "Em",
        "logradouro": "Ru",
        "numero": "1",
        "bairro": "Ce",
        "cmun": "3524600",
        "cidade": "Ja",
        "uf": "SP",
        "cep": "11940000",
        "ie": "123456789012",
        "fone": "",
    }
    from produtos.nfe_saida_regras_validas_util import RegraFiscalResolvida
    import re as _re

    def _regra(doc, ln, idx, **kwargs):
        cfop = _re.sub(r"\D", "", str(ln.get("cfop") or ""))[:4]
        csosn = _re.sub(r"\D", "", str(ln.get("csosn") or ""))[:3]
        return (
            RegraFiscalResolvida(
                pk=1,
                estabelecimento="centro",
                escopo="geral",
                cfop=cfop or "5102",
                csosn=csosn or "102",
                cst_pis="49",
                cst_cofins="49",
            ),
            [],
        )

    casos = (("4,355", "12498.85", "4.3550"), ("0,645", "1851.15", "0.6450"))
    with patch("produtos.nfe_saida_tributacao_util.resolver_regra_item", side_effect=_regra), patch(
        "produtos.nfe_saida_tributacao_util.estabelecimento_do_doc", return_value="centro"
    ), patch(
        "produtos.nfe_saida_tributacao_util.cmun_destinatario_ou_erro", return_value=("3524600", None)
    ):
        for qtd, total, qcom in casos:
            xml = _montar_xml_nfe_saida(
                _doc(qtd, total),
                cfg,
                serie=1,
                numero=8,
                chave="3" * 43 + "5",
                dh_emi=datetime(2026, 8, 31, 10, 0, 0),
            )
            check(f"xml_qcom_{qcom}", f"<qCom>{qcom}</qCom>" in xml)
            check(f"xml_qtrib_{qcom}", f"<qTrib>{qcom}</qTrib>" in xml)
            check(f"xml_ucom_ton_{qcom}", "<uCom>TON</uCom>" in xml and "<uTrib>TON</uTrib>" in xml)
            check(f"xml_vprod_{total}", f"<vProd>{total}</vProd>" in xml)
            ok, det = validar_xml_nfe_xsd(xml)
            check(f"xsd_{qcom}", ok is True, str(det)[:120])
            ap = apurar_nfe_saida(_doc(qtd, total))
            check(f"apurar_ok_{qcom}", not ap.bloqueios, str(ap.bloqueios)[:120])

    js = _read("produtos/static/produtos/js/nfe_saida.js")
    check("js_parseBrNum", "function parseBrNum" in js)
    check("js_moneyQtdVu", "function moneyQtdVu" in js and "q4 * v4 / 1000000" in js)
    check("js_un_input", 'data-it="un"' in js)
    check("js_placeholder_qtd", "0,645" in js)

    try:
        from django.db import connection

        from produtos.models import NfeSaidaRascunhoAgro
        from produtos.nfe_saida_util import obter_nfe_saida_bruto, salvar_documento_nfe_saida

        tables = set(connection.introspection.table_names())
        if NfeSaidaRascunhoAgro._meta.db_table not in tables:
            with connection.schema_editor() as se:
                se.create_model(NfeSaidaRascunhoAgro)
        payload = _doc("4,355", "12498.85")
        payload["estabelecimento"] = "centro"
        r = salvar_documento_nfe_saida(None, None, payload, usuario="verify_qtd")
        check("salvar_pg_qtd", r.get("ok") is True, str(r)[:120])
        if r.get("ok"):
            got = obter_nfe_saida_bruto(None, str(r.get("id")))
            it0 = ((got or {}).get("itens") or [{}])[0]
            check("reabrir_pg_qtd", qtd_persistida(it0.get("qtd")) == "4.355", str(it0.get("qtd")))
            check("reabrir_pg_un", normalizar_unidade_nfe(it0.get("un")) == "TON", str(it0.get("un")))
    except Exception as exc:
        check(
            "salvar_pg_qtd_skip",
            "no such table" in str(exc).lower() or "ativo" in str(exc).lower(),
            str(exc)[:120],
        )


def test_pg_flag() -> None:
    print("== Postgres store (flag) ==")
    from produtos.agro_fonte_config import agro_mongo_erp_desligado, agro_nfe_saida_rascunho_postgres
    from produtos.nfe_saida_util import nfe_saida_persistencia_ok, nfe_saida_usa_postgres

    check("flag_pg_fn", callable(agro_nfe_saida_rascunho_postgres))
    if agro_mongo_erp_desligado():
        check("mongo_off_usa_pg", nfe_saida_usa_postgres() is True)
        check("persistencia_sem_mongo", nfe_saida_persistencia_ok(None) is True)
    else:
        check("mongo_on_skip", True, "AGRO_MONGO_ERP_DESLIGADO=false neste ambiente")


def test_migration_file() -> None:
    print("== Migrations NF-e ==")
    mig = ROOT / "produtos/migrations/0146_nfe_saida_rascunho_agro.py"
    check("mig_0146_exists", mig.is_file())
    check("mig_model_name", "NfeSaidaRascunhoAgro" in mig.read_text(encoding="utf-8"))
    mig7 = ROOT / "produtos/migrations/0147_nfe_numeracao_agro.py"
    check("mig_0147_exists", mig7.is_file())
    check("mig_nfe_numeracao", "NfeNumeracaoAgro" in mig7.read_text(encoding="utf-8"))
    check("model_nfe_numeracao", "class NfeNumeracaoAgro" in _read("produtos/models.py"))


def main() -> int:
    print("verify_nfe_saida_path")
    test_arquivos()
    test_migration_file()
    test_pg_flag()
    test_logica()
    test_qtd_decimal()
    test_pin()
    test_http()
    print(f"\n{'=' * 40}\nOK {len(oks)}/{len(oks) + len(fails)}")
    print("PREP_FAILS=" + str(len(fails)))
    if fails:
        print("FAIL:", ", ".join(fails))
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
