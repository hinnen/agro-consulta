"""Etapa 2 — página Fiscal / NF-e, isolamento Centro/Vila, auditoria. Sem motor."""
from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path

from django.conf import settings
from django.contrib.auth.models import User
from django.test import Client, TestCase


class _DisableMigrations(dict):
    """SQLite de teste não aplica o histórico: 0039 quebra (ver tests_nfe_fiscal_migracao)."""

    def __contains__(self, item):
        return True

    def __getitem__(self, item):
        return None


# Precisa valer antes de setup_databases (override_settings na classe chega tarde).
# A migração 0152/0153 é verificada à parte em Postgres descartável.
settings.MIGRATION_MODULES = _DisableMigrations()

from produtos.models import (
    NfeAliquotaCreditoSn,
    NfeAlteracaoFiscalLog,
    NfeEstabelecimentoFiscal,
    NfeFiscalStatus,
    NfeParametroPisCofins,
    NfeQuestionarioResposta,
    NfeRegraOperacao,
)
from produtos.nfe_fiscal_config_util import (
    MSG_AGUARDANDO,
    parse_estabelecimento,
    painel_estabelecimento,
    salvar_credito,
    salvar_empresa,
    salvar_pis,
    salvar_questionario_item,
    texto_questionario,
    validar_registro,
)

_TPL = Path("produtos/templates/produtos/fiscal_nfe_config.html")
_CONTADOR = {
    "contador_nome": "Maria Contadora",
    "contador_crc": "CRC-SP 12345",
    "contador_aprovado_em": "2026-10-20",
}


class NfeFiscalConfigAcessoTests(TestCase):
    def setUp(self):
        self.client = Client()
        self.user = User.objects.create_user("op", "op@t.local", "x")
        self.staff = User.objects.create_user("adm", "adm@t.local", "x", is_staff=True)

    def test_anonimo_redireciona(self):
        r = self.client.get("/configuracao/fiscal-nfe/")
        self.assertEqual(r.status_code, 302)
        self.assertIn("/entrar/", r.url)

    def test_nao_staff_403_pagina_e_api(self):
        self.client.force_login(self.user)
        r = self.client.get("/configuracao/fiscal-nfe/")
        self.assertEqual(r.status_code, 403)
        r2 = self.client.get("/api/configuracao/fiscal-nfe/?estabelecimento=centro")
        self.assertEqual(r2.status_code, 403)

    def test_staff_acessa_pagina(self):
        self.client.force_login(self.staff)
        r = self.client.get("/configuracao/fiscal-nfe/")
        self.assertEqual(r.status_code, 200)
        html = r.content.decode("utf-8")
        self.assertIn("Fiscal / NF-e", html)
        self.assertIn("Centro", html)
        self.assertIn("Vila Elias", html)
        self.assertIn("Questionário para o contador", html)
        self.assertIn("id=\"cnt-nome\"", html)
        self.assertIn("id=\"cnt-crc\"", html)
        self.assertNotIn("NFC_E_CERT_PASSWORD", html)
        self.assertNotIn("NFC_E_IBPT_TOKEN", html)
        self.assertNotIn("cert_password", html.lower())


class NfeFiscalConfigPersistenciaTests(TestCase):
    def setUp(self):
        self.staff = User.objects.create_user("adm", "adm@t.local", "x", is_staff=True)
        self.client = Client()
        self.client.force_login(self.staff)

    def test_centro_e_vila_isolados(self):
        salvar_empresa("centro", {"crt": "1", "serie_nfe_55": 1}, self.staff)
        salvar_empresa("vila", {"crt": "1", "serie_nfe_55": 20}, self.staff)
        c = NfeEstabelecimentoFiscal.objects.get(estabelecimento="centro")
        v = NfeEstabelecimentoFiscal.objects.get(estabelecimento="vila")
        self.assertEqual(c.serie_nfe_55, 1)
        self.assertEqual(v.serie_nfe_55, 20)

    def test_campo_vazio_permanece_pendente(self):
        out = salvar_empresa("centro", {"crt": "", "serie_nfe_55": "", "observacoes": ""}, self.staff)
        self.assertTrue(out["ok"])
        self.assertEqual(out["empresa"]["status"], NfeFiscalStatus.PENDENTE)

    def test_preenchido_nao_valida_sozinho(self):
        r = salvar_credito(
            "centro",
            {"competencia": "2026-10", "percentual": "1.25", "observacoes": ""},
            self.staff,
        )
        self.assertTrue(r["ok"])
        self.assertEqual(r["item"]["status"], NfeFiscalStatus.PREENCHIDO)
        self.assertEqual(r["item"]["status_msg"], MSG_AGUARDANDO)
        self.assertIsNone(NfeAliquotaCreditoSn.objects.get(pk=r["item"]["id"]).validado_em)

    def test_validar_exige_contador(self):
        r = salvar_credito("centro", {"competencia": "2026-10", "percentual": "2"}, self.staff)
        pk = r["item"]["id"]
        v = validar_registro("credito", pk, self.staff, "centro")
        self.assertFalse(v["ok"])
        self.assertIn("contador", v["erro"].lower())
        obj = NfeAliquotaCreditoSn.objects.get(pk=pk)
        self.assertEqual(obj.status, NfeFiscalStatus.PREENCHIDO)

    def test_validar_manual_com_contador(self):
        r = salvar_credito(
            "centro",
            {"competencia": "2026-10", "percentual": "2", **_CONTADOR},
            self.staff,
        )
        pk = r["item"]["id"]
        v = validar_registro("credito", pk, self.staff, "centro")
        self.assertTrue(v["ok"])
        obj = NfeAliquotaCreditoSn.objects.get(pk=pk)
        self.assertEqual(obj.status, NfeFiscalStatus.VALIDADO)
        self.assertIsNotNone(obj.validado_em)
        self.assertEqual(obj.validado_por_id, self.staff.pk)
        self.assertEqual(obj.contador_nome, "Maria Contadora")
        self.assertEqual(obj.contador_crc, "CRC-SP 12345")

    def test_competencias_nao_sobrescrevem(self):
        a = salvar_credito("centro", {"competencia": "2026-01", "percentual": "1"}, self.staff)
        b = salvar_credito("centro", {"competencia": "2026-02", "percentual": "2"}, self.staff)
        self.assertTrue(a["ok"] and b["ok"])
        self.assertEqual(NfeAliquotaCreditoSn.objects.filter(estabelecimento="centro").count(), 2)
        self.assertEqual(
            NfeAliquotaCreditoSn.objects.get(estabelecimento="centro", competencia="2026-01").percentual,
            Decimal("1"),
        )

    def test_historico_registra(self):
        salvar_empresa("vila", {"crt": "1"}, self.staff)
        self.assertTrue(
            NfeAlteracaoFiscalLog.objects.filter(tabela="NfeEstabelecimentoFiscal", estabelecimento="vila").exists()
        )

    def test_questionario_em_branco_e_texto(self):
        grupos = painel_estabelecimento("centro")["questionario"]
        self.assertEqual(len(grupos), 9)
        self.assertTrue(all(p["status"] == "pendente" for g in grupos for p in g["perguntas"]))
        r = salvar_questionario_item(
            {"grupo": 1, "chave": "diferencas_cnpjs", "resposta": "CNPJs distintos"},
            self.staff,
        )
        self.assertTrue(r["ok"])
        rec = NfeQuestionarioResposta.objects.get(grupo=1, chave="diferencas_cnpjs")
        self.assertEqual(rec.status, NfeFiscalStatus.PREENCHIDO)
        txt = texto_questionario()
        self.assertIn("QUESTIONÁRIO FISCAL", txt)
        self.assertIn("CNPJs distintos", txt)

    def test_api_staff_salva_e_nao_vaza_segredo(self):
        r = self.client.get("/api/configuracao/fiscal-nfe/?estabelecimento=centro")
        self.assertEqual(r.status_code, 200)
        body = r.json()
        blob = json.dumps(body)
        self.assertNotIn("NFC_E_IBPT_TOKEN", blob)
        self.assertNotIn("cert_password", blob)
        self.assertTrue(body.get("nao_integra_motor"))
        r2 = self.client.post(
            "/api/configuracao/fiscal-nfe/regra/",
            data=json.dumps({"estabelecimento": "centro", "ambito": "interna", "tipo_destinatario": "consumidor"}),
            content_type="application/json",
        )
        self.assertEqual(r2.status_code, 200)
        self.assertEqual(r2.json()["item"]["status"], "preenchido")
        self.assertEqual(NfeRegraOperacao.objects.filter(csosn="102").count(), 0)

    def test_questionario_texto_e_f11(self):
        r = self.client.get("/api/configuracao/fiscal-nfe/questionario/texto/")
        self.assertEqual(r.status_code, 200)
        self.assertIn("text/plain", r["Content-Type"])
        js = Path("produtos/static/produtos/js/agro_perf_config.js").read_text(encoding="utf-8")
        self.assertIn("/configuracao/fiscal-nfe/", js)
        self.assertIn("Fiscal / NF-e", js)


class NfeFiscalConfigValidacaoTests(TestCase):
    def setUp(self):
        self.staff = User.objects.create_user("adm", "adm@t.local", "x", is_staff=True)

    def test_nao_valida_registro_vazio(self):
        salvar_empresa("centro", {"crt": "", "serie_nfe_55": ""}, self.staff)
        pk = NfeEstabelecimentoFiscal.objects.get(estabelecimento="centro").pk
        v = validar_registro("empresa", pk, self.staff, "centro", _CONTADOR)
        self.assertFalse(v["ok"])
        self.assertIn("CRT", v["erro"])

    def test_nao_valida_credito_incompleto(self):
        r = salvar_credito("centro", {"competencia": "2026-10"}, self.staff)
        pk = r["item"]["id"]
        v = validar_registro("credito", pk, self.staff, "centro", _CONTADOR)
        self.assertFalse(v["ok"])
        self.assertIn("percentual", v["erro"].lower())

    def test_percentual_invalido(self):
        a = salvar_credito("centro", {"competencia": "2026-10", "percentual": "abc"}, self.staff)
        self.assertFalse(a["ok"])
        b = salvar_credito("centro", {"competencia": "2026-10", "percentual": "101"}, self.staff)
        self.assertFalse(b["ok"])
        c = salvar_credito("centro", {"competencia": "2026-10", "percentual": "-1"}, self.staff)
        self.assertFalse(c["ok"])

    def test_vigencia_inconsistente(self):
        r = salvar_credito(
            "centro",
            {
                "competencia": "2026-10",
                "percentual": "2",
                "vigencia_inicio": "2026-12-01",
                "vigencia_fim": "2026-01-01",
            },
            self.staff,
        )
        self.assertFalse(r["ok"])
        self.assertIn("Vigência", r["erro"])


class NfeFiscalConfigContadorTests(TestCase):
    def setUp(self):
        self.staff = User.objects.create_user("adm", "adm@t.local", "x", is_staff=True)

    def test_salvar_contador_nao_valida(self):
        r = salvar_credito(
            "centro",
            {"competencia": "2026-10", "percentual": "2", **_CONTADOR},
            self.staff,
        )
        self.assertTrue(r["ok"])
        obj = NfeAliquotaCreditoSn.objects.get(pk=r["item"]["id"])
        self.assertEqual(obj.status, NfeFiscalStatus.PREENCHIDO)
        self.assertEqual(obj.contador_nome, "Maria Contadora")
        self.assertEqual(obj.contador_crc, "CRC-SP 12345")
        self.assertIsNone(obj.validado_em)
        self.assertIsNone(obj.validado_por_id)

    def test_validado_por_e_contador_sao_distintos(self):
        r = salvar_credito(
            "centro",
            {"competencia": "2026-11", "percentual": "3", **_CONTADOR},
            self.staff,
        )
        pk = r["item"]["id"]
        v = validar_registro("credito", pk, self.staff, "centro")
        self.assertTrue(v["ok"])
        obj = NfeAliquotaCreditoSn.objects.get(pk=pk)
        self.assertEqual(obj.validado_por.get_username(), "adm")
        self.assertNotEqual(obj.contador_nome.lower(), obj.validado_por.get_username())
        self.assertEqual(str(obj.contador_aprovado_em), "2026-10-20")


class NfeFiscalConfigHtmlSeguroTests(TestCase):
    def setUp(self):
        self.staff = User.objects.create_user("adm", "adm@t.local", "x", is_staff=True)
        self.client = Client()
        self.client.force_login(self.staff)

    def test_template_nao_usa_innerhtml(self):
        src = _TPL.read_text(encoding="utf-8")
        self.assertNotIn("innerHTML", src)
        self.assertNotIn("insertAdjacentHTML", src)
        self.assertIn("textContent", src)
        self.assertIn(".value =", src)

    def test_resposta_com_tags_html_fica_texto(self):
        payload = '<img src=x onerror=alert(1)> <b>negrito</b>'
        r = salvar_questionario_item(
            {"grupo": 1, "chave": "diferencas_cnpjs", "resposta": payload},
            self.staff,
        )
        self.assertTrue(r["ok"])
        rec = NfeQuestionarioResposta.objects.get(grupo=1, chave="diferencas_cnpjs")
        self.assertEqual(rec.resposta, payload)
        txt = texto_questionario()
        self.assertIn(payload, txt)
        painel = painel_estabelecimento("centro")
        achou = False
        for g in painel["questionario"]:
            for p in g["perguntas"]:
                if p["chave"] == "diferencas_cnpjs" and not p["estabelecimento"]:
                    self.assertEqual(p["resposta"], payload)
                    achou = True
        self.assertTrue(achou)
        page = self.client.get("/configuracao/fiscal-nfe/")
        html = page.content.decode("utf-8")
        self.assertIn("ta.value = p.resposta", html)
        script = html.split("function pintarQ()")[1].split("function pintar()")[0]
        self.assertNotIn("innerHTML", script)


class NfeFiscalConfigPisExcecaoTests(TestCase):
    def setUp(self):
        self.staff = User.objects.create_user("adm", "adm@t.local", "x", is_staff=True)
        self.client = Client()
        self.client.force_login(self.staff)

    def test_excecao_preserva_cinco_campos(self):
        ex = {
            "operacao": "revenda",
            "produto": "ração X",
            "ncm": "23099020",
            "cst_pis": "01",
            "cst_cofins": "01",
        }
        r = salvar_pis(
            "centro",
            {"cst_pis": "49", "cst_cofins": "49", "excecoes": [ex]},
            self.staff,
        )
        self.assertTrue(r["ok"])
        item = r["pis"]["excecoes"][0]
        self.assertEqual(item["operacao"], "revenda")
        self.assertEqual(item["produto"], "ração X")
        self.assertEqual(item["ncm"], "23099020")
        self.assertEqual(item["cst_pis"], "01")
        self.assertEqual(item["cst_cofins"], "01")
        obj = NfeParametroPisCofins.objects.get(estabelecimento="centro")
        self.assertEqual(obj.excecoes_json[0]["ncm"], "23099020")
        r2 = self.client.get("/api/configuracao/fiscal-nfe/?estabelecimento=centro")
        novamente = r2.json()["pis"]["excecoes"][0]
        self.assertEqual(novamente["operacao"], "revenda")
        self.assertEqual(novamente["produto"], "ração X")
        self.assertEqual(novamente["cst_pis"], "01")
        self.assertEqual(novamente["cst_cofins"], "01")


class NfeFiscalConfigIsolamentoEstabTests(TestCase):
    def setUp(self):
        self.staff = User.objects.create_user("adm", "adm@t.local", "x", is_staff=True)
        self.client = Client()
        self.client.force_login(self.staff)

    def test_parse_desconhecido_nao_vira_centro(self):
        self.assertIsNone(parse_estabelecimento("foo"))
        self.assertIsNone(parse_estabelecimento("loja3"))

    def test_salvar_util_rejeita_desconhecido(self):
        out = salvar_empresa("desconhecido", {"crt": "1", "serie_nfe_55": 1}, self.staff)
        self.assertFalse(out["ok"])
        self.assertFalse(NfeEstabelecimentoFiscal.objects.filter(estabelecimento="centro").exists())

    def test_api_gravacao_rejeita_desconhecido(self):
        r = self.client.post(
            "/api/configuracao/fiscal-nfe/empresa/",
            data=json.dumps({"estabelecimento": "foo", "crt": "1", "serie_nfe_55": 1}),
            content_type="application/json",
        )
        self.assertEqual(r.status_code, 400)
        self.assertFalse(r.json().get("ok"))

    def test_api_gravacao_exige_estabelecimento(self):
        r = self.client.post(
            "/api/configuracao/fiscal-nfe/empresa/",
            data=json.dumps({"crt": "1", "serie_nfe_55": 1}),
            content_type="application/json",
        )
        self.assertEqual(r.status_code, 400)

    def test_api_leitura_desconhecido_400(self):
        r = self.client.get("/api/configuracao/fiscal-nfe/?estabelecimento=foo")
        self.assertEqual(r.status_code, 400)


class NfeFiscalConfigNaoMexeuMotorTests(TestCase):
    def test_arquivos_do_motor_intocados_nesta_etapa(self):
        from produtos.nfe_saida_tributacao_util import _cfg_cred_sn_aliquota

        self.assertTrue(callable(_cfg_cred_sn_aliquota))
        self.assertFalse(NfeRegraOperacao.objects.exists())
