"""Pedido/empenho prefeitura — XML, XSD, rascunho e DANFE (sem SEFAZ)."""
from __future__ import annotations

import re
from datetime import datetime
from decimal import Decimal
from unittest.mock import patch

from django.conf import settings
from django.test import SimpleTestCase, TestCase


class _DisableMigrations(dict):
    def __contains__(self, item):
        return True

    def __getitem__(self, item):
        return None


settings.MIGRATION_MODULES = _DisableMigrations()

from produtos.tests_nfe_saida_util import xml_com_regra

_CFG = {
    "tp_amb": 2,
    "cnpj": "48900774000103",
    "razao_social": "GM Agropecuaria Ltda ME",
    "fantasia": "GM Agro",
    "logradouro": "Rua B",
    "numero": "10",
    "bairro": "Centro",
    "cmun": "3524600",
    "cidade": "Jacupiranga",
    "uf": "SP",
    "cep": "11940000",
    "ie": "123456789012",
    "fone": "",
}

_DEST = {
    "nome": "Prefeitura Municipal de Jacupiranga",
    "documento": "46582185000190",
    "ie": "",
    "uf": "SP",
    "cidade": "Jacupiranga",
    "cmun": "3524600",
    "logradouro": "Praca",
    "numero": "1",
    "bairro": "Centro",
    "cep": "11940000",
    "orgao_publico": True,
    "finalidade_mercadoria": "orgao_publico",
}


def _doc_prefeitura(*, pedido: str, empenho: str, qtd, total):
    from produtos.nfe_saida_util import doc_novo_vazio

    doc = doc_novo_vazio()
    doc["destinatario"].update(_DEST)
    doc["fiscal"]["x_ped"] = pedido
    doc["fiscal"]["x_nemp"] = empenho
    doc["fiscal"]["x_cont"] = "206/2026"
    doc["fiscal"]["natureza"] = "VENDA"
    doc["itens"] = [
        {
            "descricao": "Adubo fosfato",
            "un": "TON",
            "qtd": qtd,
            "v_unit": "2870.00",
            "v_total": str(total),
            "ncm": "23099020",
            "cfop": "5102",
            "csosn": "102",
            "cst_pis_cofins": "49",
        }
    ]
    doc["pagamento"] = [{"tPag": "01", "valor": float(total)}]
    return doc


class NfeSaidaCompraPrefeituraTests(SimpleTestCase):
    def _xml(self, doc):
        from produtos.nfe_saida_sp_emissao_util import _montar_xml_nfe_saida

        with xml_com_regra():
            return _montar_xml_nfe_saida(
                doc,
                _CFG,
                serie=1,
                numero=101,
                chave="3" * 43 + "5",
                dh_emi=datetime(2026, 10, 27, 12, 0, 0),
            )

    def test_nfe1_pedido_2185_empenho_6134(self):
        from produtos.nfe_saida_xml_validate_util import validar_xml_nfe_xsd

        doc = _doc_prefeitura(pedido="2185/2026", empenho="6134", qtd="4.355", total="12498.85")
        xml = self._xml(doc)
        ok, det = validar_xml_nfe_xsd(xml)
        self.assertTrue(ok, det)
        self.assertIn("<xNEmp>6134</xNEmp>", xml)
        self.assertIn("<xPed>2185/2026</xPed>", xml)
        self.assertIn("<xCont>206/2026</xCont>", xml)
        self.assertIn("<nItemPed>1</nItemPed>", xml)
        self.assertIn("<uCom>TON</uCom>", xml)
        self.assertIn("<vNF>12498.85</vNF>", xml)
        self.assertLess(xml.find("</infAdic>"), xml.find("<compra>"))
        compra_blk = re.search(r"<compra>.*?</compra>", xml)
        self.assertIsNotNone(compra_blk)
        cx = compra_blk.group(0)
        self.assertLess(cx.find("<xNEmp>"), cx.find("<xPed>"))
        self.assertLess(cx.find("<xPed>"), cx.find("<xCont>"))
        self.assertIn("Pedido 2185/2026", xml)
        self.assertIn("Empenho 6134", xml)

    def test_nfe2_pedido_2186_empenho_6133(self):
        from produtos.nfe_saida_xml_validate_util import validar_xml_nfe_xsd

        doc = _doc_prefeitura(pedido="2186/2026", empenho="6133", qtd="0.645", total="1851.15")
        xml = self._xml(doc)
        ok, det = validar_xml_nfe_xsd(xml)
        self.assertTrue(ok, det)
        self.assertIn("<xPed>2186/2026</xPed>", xml)
        self.assertIn("<xNEmp>6133</xNEmp>", xml)
        self.assertIn("<vNF>1851.15</vNF>", xml)
        self.assertEqual(Decimal("0.645") * Decimal("2870"), Decimal("1851.15"))

    def test_sem_compra_nao_gera_grupo(self):
        doc = _doc_prefeitura(pedido="2185/2026", empenho="6134", qtd="4.355", total="12498.85")
        doc["fiscal"]["x_ped"] = ""
        doc["fiscal"]["x_nemp"] = ""
        doc["fiscal"]["x_cont"] = ""
        xml = self._xml(doc)
        self.assertNotIn("<compra>", xml)

    def test_danfe_mostra_pedido_empenho(self):
        from produtos.nfe_saida_danfe_html import montar_html_danfe

        doc = _doc_prefeitura(pedido="2185/2026", empenho="6134", qtd="4.355", total="12498.85")
        html = montar_html_danfe(doc, emitente={"razao": "GM", "cnpj": "48900774000103", "uf": "SP"})
        self.assertIn("Pedido 2185/2026", html)
        self.assertIn("Empenho 6134", html)
        self.assertIn("Processo 206/2026", html)


class NfeSaidaCompraRascunhoTests(TestCase):
    def test_salvar_e_reabrir_pedido_empenho(self):
        from produtos.nfe_saida_util import (
            criar_nfe_saida_vazia,
            obter_nfe_saida_bruto,
            salvar_documento_nfe_saida,
        )

        with patch("produtos.nfe_saida_util.nfe_saida_usa_postgres", return_value=True):
            r = criar_nfe_saida_vazia(None, usuario="pytest")
            self.assertTrue(r.get("ok"))
            doc_id = r["id"]
            bruto = obter_nfe_saida_bruto(None, doc_id)
            bruto["fiscal"]["x_ped"] = "2185/2026"
            bruto["fiscal"]["x_nemp"] = "6134"
            bruto["fiscal"]["x_cont"] = "206/2026"
            bruto["inf_complementar"] = "Prefeitura Jacupiranga"
            out = salvar_documento_nfe_saida(None, doc_id, bruto, usuario="pytest")
            self.assertTrue(out.get("ok"))
            again = obter_nfe_saida_bruto(None, doc_id)
            self.assertEqual(again["fiscal"]["x_ped"], "2185/2026")
            self.assertEqual(again["fiscal"]["x_nemp"], "6134")
            self.assertEqual(again["fiscal"]["x_cont"], "206/2026")
            self.assertEqual(again["inf_complementar"], "Prefeitura Jacupiranga")
