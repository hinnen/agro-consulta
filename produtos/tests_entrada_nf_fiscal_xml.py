"""Entrada NF — fiscal do XML (NCM, CFOP, CSOSN) → catálogo."""
from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock, patch

from django.test import SimpleTestCase, TestCase

from produtos.agro_produto_fiscal_defaults import cfop_venda_de_cfop_nf_entrada
from produtos.models import Produto, ProdutoGestaoOverlayAgro
from produtos.nfe_entrada_util import parse_nfe_xml_bytes, propagar_fiscal_nf_catalogo_entrada_nota


def _xml_com_imposto() -> bytes:
    return b"""<?xml version="1.0" encoding="UTF-8"?>
<nfeProc xmlns="http://www.portalfiscal.inf.br/nfe">
  <NFe>
    <infNFe Id="NFe35260911111111000191550010002692631000000010" versao="4.00">
      <ide><nNF>1</nNF><serie>1</serie></ide>
      <emit><CNPJ>11111111000191</CNPJ></emit>
      <dest><CNPJ>48900774000103</CNPJ></dest>
      <det nItem="1">
        <prod>
          <cProd>A1</cProd>
          <xProd>Prod teste fiscal</xProd>
          <NCM>3004.90.99</NCM>
          <CFOP>1102</CFOP>
          <uCom>UN</uCom>
          <qCom>1</qCom>
          <vUnCom>10</vUnCom>
          <vProd>10</vProd>
        </prod>
        <imposto>
          <ICMS>
            <ICMSSN102>
              <orig>2</orig>
              <CSOSN>102</CSOSN>
            </ICMSSN102>
          </ICMS>
        </imposto>
      </det>
    </infNFe>
  </NFe>
</nfeProc>
"""


class CfopEntradaParaVendaTests(SimpleTestCase):
    def test_compra_estadual_para_venda(self):
        self.assertEqual(cfop_venda_de_cfop_nf_entrada("1102"), "5102")

    def test_compra_interestadual(self):
        self.assertEqual(cfop_venda_de_cfop_nf_entrada("2102"), "6102")

    def test_cfop_saida_mantem(self):
        self.assertEqual(cfop_venda_de_cfop_nf_entrada("5405"), "5405")


class ParseNfeFiscalXmlTests(SimpleTestCase):
    def test_extrai_ncm_cfop_csosn_origem(self):
        parsed = parse_nfe_xml_bytes(_xml_com_imposto())
        self.assertTrue(parsed.get("ok"), parsed.get("erro"))
        it = parsed["itens"][0]
        self.assertEqual(it["ncm"], "30049099")
        self.assertEqual(it["cfop"], "1102")
        self.assertEqual(it["csosn"], "102")
        self.assertEqual(it["origem"], "2")


class PropagarFiscalEntradaNfTests(TestCase):
    def test_linha_xml_atualiza_overlay_e_produto(self):
        p = Produto.objects.create(
            produto_externo_id="erp-fiscal-1",
            nome="X",
            ncm="23099020",
            preco_venda=Decimal("1"),
        )
        linhas = [
            {
                "produto_id": str(p.pk),
                "ncm": "30049099",
                "cfop": "1102",
                "csosn": "400",
                "origem": "2",
            }
        ]
        r = propagar_fiscal_nf_catalogo_entrada_nota(linhas)
        self.assertTrue(r.get("ok"))
        self.assertEqual(r.get("atualizados_overlay"), 1)
        p.refresh_from_db()
        self.assertEqual(p.ncm, "30049099")
        ov = ProdutoGestaoOverlayAgro.objects.get(produto_externo_id="erp-fiscal-1")
        fis = ov.cadastro_extras.get("fiscal") or {}
        self.assertEqual(fis.get("ncm"), "30049099")
        self.assertEqual(fis.get("cfop"), "5102")
        self.assertEqual(fis.get("csosn"), "400")
        self.assertEqual(fis.get("origem"), "2")

    def test_mongo_atualizado_quando_db_disponivel(self):
        p = Produto.objects.create(
            produto_externo_id="erp-fiscal-2",
            nome="Y",
            ncm="23099020",
            preco_venda=Decimal("1"),
        )
        mock_col = MagicMock()
        mock_col.update_one.return_value = MagicMock(matched_count=1)
        mock_db = MagicMock()
        mock_db.__getitem__.return_value = mock_col
        client = MagicMock(col_p="DtoProduto")
        with patch(
            "produtos.views._mongo_filtro_id_produto_externo",
            return_value={"Id": "erp-fiscal-2"},
        ), patch(
            "produtos.views._mongo_doc_produto_entrada_resolve",
            return_value={"Id": "erp-fiscal-2"},
        ):
            r = propagar_fiscal_nf_catalogo_entrada_nota(
                [{"produto_id": str(p.pk), "ncm": "12345678", "cfop": "5405", "csosn": "500"}],
                db=mock_db,
                client_m=client,
            )
        self.assertEqual(r.get("atualizados_mongo"), 1)
        args = mock_col.update_one.call_args
        set_doc = args[0][1]["$set"]
        self.assertEqual(set_doc["NCM"], "12345678")
        self.assertEqual(set_doc["CFOP"], "5405")
        self.assertEqual(set_doc["CSOSN"], "500")
