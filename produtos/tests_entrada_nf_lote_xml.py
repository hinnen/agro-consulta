"""NF-LOTE-XML — lote/validade vêm de prod/rastro (schema NF-e 4.00), não só de det."""
from __future__ import annotations

from django.test import SimpleTestCase

from produtos.nfe_entrada_util import parse_nfe_xml_bytes


def _xml(corpo_det: str) -> bytes:
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<nfeProc xmlns="http://www.portalfiscal.inf.br/nfe">
  <NFe>
    <infNFe Id="NFe35260911111111000191550010002692631000000010" versao="4.00">
      <ide><nNF>269263</nNF><serie>1</serie><dhEmi>2026-09-11T10:00:00-03:00</dhEmi></ide>
      <emit><CNPJ>11111111000191</CNPJ><xNome>Lab Vet</xNome></emit>
      <dest><CNPJ>48900774000103</CNPJ></dest>
      <det nItem="1">
        {corpo_det}
      </det>
      <total><ICMSTot><vProd>10.00</vProd><vNF>10.00</vNF></ICMSTot></total>
    </infNFe>
  </NFe>
</nfeProc>
""".encode("utf-8")


_PROD = """
        <prod>
          <cProd>A1</cProd>
          <cEAN>7898185261131</cEAN>
          <xProd>ATROPINA CALBOS INJ.</xProd>
          <NCM>30049099</NCM>
          <CFOP>5405</CFOP>
          <uCom>UN</uCom>
          <qCom>1.0000</qCom>
          <vUnCom>10.00</vUnCom>
          <vProd>10.00</vProd>
          {extra}
        </prod>
"""


class EntradaNfLoteXmlTests(SimpleTestCase):
    def test_rastro_dentro_de_prod(self):
        extra = """
          <rastro>
            <nLote>L24015</nLote>
            <qLote>1.000</qLote>
            <dFab>2025-01-10</dFab>
            <dVal>2027-09-30</dVal>
          </rastro>
        """
        parsed = parse_nfe_xml_bytes(_xml(_PROD.format(extra=extra)))
        self.assertTrue(parsed.get("ok"), parsed.get("erro"))
        it = parsed["itens"][0]
        self.assertEqual(it["lote_numero"], "L24015")
        self.assertEqual(it["lote_fabricacao"], "2025-01-10")
        self.assertEqual(it["lote_validade"], "2027-09-30")
        self.assertTrue(it["lote_xml"])

    def test_rastro_filho_de_det_ainda_vale(self):
        corpo = _PROD.format(extra="") + """
        <rastro>
          <nLote>DET1</nLote>
          <dVal>2028-01-15</dVal>
        </rastro>
        """
        parsed = parse_nfe_xml_bytes(_xml(corpo))
        self.assertTrue(parsed.get("ok"), parsed.get("erro"))
        it = parsed["itens"][0]
        self.assertEqual(it["lote_numero"], "DET1")
        self.assertEqual(it["lote_validade"], "2028-01-15")
        self.assertTrue(it["lote_xml"])

    def test_infadprod_quando_nao_tem_rastro(self):
        extra = ""
        corpo = _PROD.format(extra=extra) + (
            "<infAdProd>Lote: ABC99 Validade: 31/12/2027 Fab: 01/02/2026</infAdProd>"
        )
        parsed = parse_nfe_xml_bytes(_xml(corpo))
        self.assertTrue(parsed.get("ok"), parsed.get("erro"))
        it = parsed["itens"][0]
        self.assertEqual(it["lote_numero"], "ABC99")
        self.assertEqual(it["lote_validade"], "2027-12-31")
        self.assertEqual(it["lote_fabricacao"], "2026-02-01")
        self.assertTrue(it["lote_xml"])

    def test_sem_lote_fica_vazio(self):
        parsed = parse_nfe_xml_bytes(_xml(_PROD.format(extra="")))
        self.assertTrue(parsed.get("ok"), parsed.get("erro"))
        it = parsed["itens"][0]
        self.assertEqual(it["lote_numero"], "")
        self.assertEqual(it["lote_validade"], "")
        self.assertFalse(it["lote_xml"])
