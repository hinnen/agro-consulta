"""Padrão fiscal confirmado pela contadora — Preenchido, sem Validado, sem transmissão."""
from __future__ import annotations

import json

from django.conf import settings
from django.contrib.auth.models import User
from django.test import Client, TestCase


class _DisableMigrations(dict):
    def __contains__(self, item):
        return True

    def __getitem__(self, item):
        return None


settings.MIGRATION_MODULES = _DisableMigrations()

from produtos.models import (
    NfeAliquotaCreditoSn,
    NfeAlteracaoFiscalLog,
    NfeEstabelecimentoFiscal,
    NfeFiscalStatus,
    NfeNumeracaoAgro,
    NfeParametroPisCofins,
    NfeRegraOperacao,
)
from produtos.nfe_fiscal_padrao_util import aplicar_padrao_contador, previa_padrao_contador
from produtos.nfe_saida_regras_validas_util import resolver_regra_item
from produtos.nfe_saida_tributacao_util import _CSOSN_EXIGE_PARAM_EXTRA


class NfeFiscalPadraoTests(TestCase):
    def setUp(self):
        self.staff = User.objects.create_user("adm", "adm@t.local", "x", is_staff=True)
        self.op = User.objects.create_user("op", "op@t.local", "x")
        self.client = Client()
        self.client.force_login(self.staff)

    def test_api_staff_e_nao_staff(self):
        r = self.client.get("/api/configuracao/fiscal-nfe/padrao/?estabelecimento=centro")
        self.assertEqual(r.status_code, 200)
        self.assertTrue(r.json()["ok"])
        c2 = Client()
        c2.force_login(self.op)
        r2 = c2.get("/api/configuracao/fiscal-nfe/padrao/?estabelecimento=centro")
        self.assertEqual(r2.status_code, 403)
        r3 = c2.post(
            "/api/configuracao/fiscal-nfe/padrao/aplicar/",
            data=json.dumps({"estabelecimento": "centro", "confirmar": True}),
            content_type="application/json",
        )
        self.assertEqual(r3.status_code, 403)

    def test_aplicar_exige_confirmar(self):
        r = self.client.post(
            "/api/configuracao/fiscal-nfe/padrao/aplicar/",
            data=json.dumps({"estabelecimento": "centro"}),
            content_type="application/json",
        )
        self.assertEqual(r.status_code, 400)

    def test_centro_preenche_crt_pis_serie_e_regras_102(self):
        out = aplicar_padrao_contador("centro", self.staff)
        self.assertTrue(out["ok"])
        emp = NfeEstabelecimentoFiscal.objects.get(estabelecimento="centro")
        pis = NfeParametroPisCofins.objects.get(estabelecimento="centro")
        self.assertEqual(emp.crt, "1")
        self.assertEqual(emp.serie_nfe_55, 1)
        self.assertEqual(emp.status, NfeFiscalStatus.PREENCHIDO)
        self.assertEqual(emp.contador_nome, "")
        self.assertEqual(emp.contador_crc, "")
        self.assertIsNone(emp.contador_aprovado_em)
        self.assertIsNone(emp.validado_em)
        self.assertEqual(pis.cst_pis, "49")
        self.assertEqual(pis.cst_cofins, "49")
        self.assertEqual(pis.status, NfeFiscalStatus.PREENCHIDO)
        regras = list(NfeRegraOperacao.objects.filter(estabelecimento="centro"))
        csosn = {r.csosn for r in regras}
        self.assertEqual(csosn, {"102"})
        tipos = {r.tipo_destinatario for r in regras}
        self.assertEqual(tipos, {"consumidor", "orgao_publico"})
        for r in regras:
            self.assertEqual(r.status, NfeFiscalStatus.PREENCHIDO)
            self.assertEqual(r.cfop, "5102")
            self.assertEqual(r.ncm, "")
            self.assertFalse(r.validado_por_id)
        self.assertFalse(NfeAliquotaCreditoSn.objects.exists())
        self.assertTrue(NfeAlteracaoFiscalLog.objects.filter(estabelecimento="centro").exists())
        self.assertIn("500", _CSOSN_EXIGE_PARAM_EXTRA)

    def test_vila_nao_preenche_serie(self):
        aplicar_padrao_contador("vila", self.staff)
        v = NfeEstabelecimentoFiscal.objects.get(estabelecimento="vila")
        self.assertEqual(v.crt, "1")
        self.assertIsNone(v.serie_nfe_55)
        self.assertFalse(NfeEstabelecimentoFiscal.objects.filter(estabelecimento="centro").exists())

    def test_nao_sobrescreve_nem_duplica(self):
        NfeEstabelecimentoFiscal.objects.create(
            estabelecimento="centro", crt="3", serie_nfe_55=9, status=NfeFiscalStatus.PREENCHIDO
        )
        NfeParametroPisCofins.objects.create(
            estabelecimento="centro", cst_pis="01", cst_cofins="01", status=NfeFiscalStatus.PREENCHIDO
        )
        aplicar_padrao_contador("centro", self.staff)
        aplicar_padrao_contador("centro", self.staff)
        emp = NfeEstabelecimentoFiscal.objects.get(estabelecimento="centro")
        pis = NfeParametroPisCofins.objects.get(estabelecimento="centro")
        self.assertEqual(emp.crt, "3")
        self.assertEqual(emp.serie_nfe_55, 9)
        self.assertEqual(pis.cst_pis, "01")
        self.assertEqual(
            NfeRegraOperacao.objects.filter(estabelecimento="centro", csosn="102", tipo_destinatario="consumidor").count(),
            1,
        )

    def test_validado_nao_e_rebaixado(self):
        NfeEstabelecimentoFiscal.objects.create(
            estabelecimento="centro", crt="1", status=NfeFiscalStatus.VALIDADO
        )
        aplicar_padrao_contador("centro", self.staff)
        emp = NfeEstabelecimentoFiscal.objects.get(estabelecimento="centro")
        self.assertEqual(emp.status, NfeFiscalStatus.VALIDADO)
        self.assertIsNone(emp.serie_nfe_55)

    def test_regra_preenchida_nao_entra_no_motor(self):
        aplicar_padrao_contador("centro", self.staff)
        regra, erros = resolver_regra_item(
            {
                "estabelecimento": "centro",
                "destinatario": {"tipo_destinatario": "orgao_publico", "ind_final": 1},
            },
            {"ncm": "01012100"},
            1,
            uf_emit="SP",
            uf_dest="SP",
        )
        self.assertIsNone(regra)
        self.assertTrue(erros)

    def test_previa_lista_101_e_500_sem_criar(self):
        p = previa_padrao_contador("centro")
        titulos = " ".join(r["titulo"] + r["detalhe"] for r in p["regras"])
        self.assertIn("101", titulos)
        self.assertIn("5405", titulos)
        self.assertFalse(any(r["csosn"] == "101" and r["aplicar"] for r in p["regras"]))
        self.assertFalse(any(r["csosn"] == "500" and r["aplicar"] for r in p["regras"]))

    def test_serie_centro_segue_numeracao_existente(self):
        NfeNumeracaoAgro.objects.create(emitente_cnpj="12345678000199", serie=7, proximo_numero=10)
        p = previa_padrao_contador("centro")
        self.assertEqual(p["serie_sugerida"], 7)
        aplicar_padrao_contador("centro", self.staff)
        self.assertEqual(NfeEstabelecimentoFiscal.objects.get(estabelecimento="centro").serie_nfe_55, 7)
