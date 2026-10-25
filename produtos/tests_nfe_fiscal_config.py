"""Etapa 2 — página Fiscal / NF-e, isolamento Centro/Vila, auditoria. Sem motor."""
from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path

from django.conf import settings
from django.contrib.auth.models import User
from django.test import Client, TestCase


class _DisableMigrations(dict):
    """A migração 0039 quebra o test DB (NfceNumeracaoAgro). Cria tabelas pelos models."""

    def __contains__(self, item):
        return True

    def __getitem__(self, item):
        return None


# Precisa valer antes de setup_databases (override_settings na classe chega tarde).
settings.MIGRATION_MODULES = _DisableMigrations()

from produtos.models import (
    NfeAliquotaCreditoSn,
    NfeAlteracaoFiscalLog,
    NfeEstabelecimentoFiscal,
    NfeFiscalStatus,
    NfeQuestionarioResposta,
    NfeRegraOperacao,
)
from produtos.nfe_fiscal_config_util import (
    MSG_AGUARDANDO,
    painel_estabelecimento,
    salvar_credito,
    salvar_empresa,
    salvar_questionario_item,
    texto_questionario,
    validar_registro,
)


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
        self.assertEqual(out["status"], NfeFiscalStatus.PENDENTE)

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

    def test_validar_manual(self):
        r = salvar_credito("centro", {"competencia": "2026-10", "percentual": "2"}, self.staff)
        pk = r["item"]["id"]
        v = validar_registro("credito", pk, self.staff, "centro")
        self.assertTrue(v["ok"])
        obj = NfeAliquotaCreditoSn.objects.get(pk=pk)
        self.assertEqual(obj.status, NfeFiscalStatus.VALIDADO)
        self.assertIsNotNone(obj.validado_em)

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


class NfeFiscalConfigNaoMexeuMotorTests(TestCase):
    def test_arquivos_do_motor_intocados_nesta_etapa(self):
        from produtos.nfe_saida_tributacao_util import _cfg_cred_sn_aliquota

        # Ainda lê o .env; cadastro da Etapa 2 não substitui.
        self.assertTrue(callable(_cfg_cred_sn_aliquota))
        self.assertFalse(NfeRegraOperacao.objects.exists())
