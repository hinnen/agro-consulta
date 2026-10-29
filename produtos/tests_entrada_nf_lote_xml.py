"""NF-LOTE-XML — lote/validade vêm de prod/rastro (schema NF-e 4.00), não só de det."""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import patch

from django.core.files.uploadedfile import SimpleUploadedFile
from django.test import RequestFactory, SimpleTestCase
from django.urls import reverse

from produtos.models import parse_data_validade_entrada_nf
from produtos.nfe_entrada_util import parse_nfe_xml_bytes
from produtos.views import api_entrada_nota_parse_xml


def _xml_dets(dets: list[str], *, numero: str = "269263") -> bytes:
    blocos = "\n".join(f'<det nItem="{i + 1}">{corpo}</det>' for i, corpo in enumerate(dets))
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<nfeProc xmlns="http://www.portalfiscal.inf.br/nfe">
  <NFe>
    <infNFe Id="NFe35260911111111000191550010002692631000000010" versao="4.00">
      <ide><nNF>{numero}</nNF><serie>1</serie><dhEmi>2026-09-11T10:00:00-03:00</dhEmi></ide>
      <emit><CNPJ>11111111000191</CNPJ><xNome>Lab Vet</xNome></emit>
      <dest><CNPJ>48900774000103</CNPJ></dest>
      {blocos}
      <total><ICMSTot><vProd>10.00</vProd><vNF>10.00</vNF></ICMSTot></total>
    </infNFe>
  </NFe>
</nfeProc>
""".encode("utf-8")


def _xml(corpo_det: str) -> bytes:
    return _xml_dets([corpo_det])


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

    def test_rastro_vence_infadprod(self):
        extra = """
          <rastro>
            <nLote>RASTRO1</nLote>
            <dVal>2027-09-30</dVal>
          </rastro>
        """
        corpo = _PROD.format(extra=extra) + (
            "<infAdProd>Lote: TXT99 Validade: 31/12/2028</infAdProd>"
        )
        it = parse_nfe_xml_bytes(_xml(corpo))["itens"][0]
        self.assertEqual(it["lote_numero"], "RASTRO1")
        self.assertEqual(it["lote_validade"], "2027-09-30")

    def test_med_legado(self):
        extra = """
          <med>
            <nLote>MED77</nLote>
            <dFab>2024-03-01</dFab>
            <dVal>2026-03-01</dVal>
          </med>
        """
        it = parse_nfe_xml_bytes(_xml(_PROD.format(extra=extra)))["itens"][0]
        self.assertEqual(it["lote_numero"], "MED77")
        self.assertEqual(it["lote_validade"], "2026-03-01")
        self.assertTrue(it["lote_xml"])

    def test_dval_br_no_rastro(self):
        extra = """
          <rastro>
            <nLote>BR1</nLote>
            <dVal>30/09/2027</dVal>
          </rastro>
        """
        it = parse_nfe_xml_bytes(_xml(_PROD.format(extra=extra)))["itens"][0]
        self.assertEqual(it["lote_validade"], "2027-09-30")

    def test_trinta_e_dois_itens_como_269263(self):
        dets = []
        for i in range(32):
            extra = f"""
              <rastro>
                <nLote>L{i + 1:02d}</nLote>
                <dVal>2027-{(i % 12) + 1:02d}-15</dVal>
              </rastro>
            """
            dets.append(_PROD.format(extra=extra))
        parsed = parse_nfe_xml_bytes(_xml_dets(dets))
        self.assertTrue(parsed.get("ok"), parsed.get("erro"))
        self.assertEqual(len(parsed["itens"]), 32)
        self.assertTrue(all(it.get("lote_xml") for it in parsed["itens"]))
        self.assertEqual(parsed["itens"][0]["lote_numero"], "L01")
        self.assertEqual(parsed["itens"][31]["lote_numero"], "L32")
        self.assertEqual(parsed["numero"], "269263")

    def test_parse_data_validade_iso_e_br(self):
        self.assertEqual(str(parse_data_validade_entrada_nf("2027-09-30")), "2027-09-30")
        self.assertEqual(str(parse_data_validade_entrada_nf("30/09/2027")), "2027-09-30")
        self.assertIsNone(parse_data_validade_entrada_nf(""))


class EntradaNfLoteXmlHttpTests(SimpleTestCase):
    def test_api_parse_xml_devolve_lote(self):
        extra = """
          <rastro>
            <nLote>API1</nLote>
            <dFab>2025-01-10</dFab>
            <dVal>2027-09-30</dVal>
          </rastro>
        """
        xml = _xml(_PROD.format(extra=extra))
        arq = SimpleUploadedFile("nfe.xml", xml, content_type="application/xml")
        factory = RequestFactory()
        request = factory.post(reverse("api_entrada_nota_parse_xml"), {"arquivo": arq})
        request.user = SimpleNamespace(
            is_authenticated=True,
            is_anonymous=False,
            email="t@x",
            pk=1,
            get_username=lambda: "t",
        )
        with (
            patch("produtos.views._entrada_nfe_conexao", return_value=(None, None)),
            patch("produtos.views.casar_produtos_entrada_nfe", side_effect=lambda itens, **kw: itens),
        ):
            resp = api_entrada_nota_parse_xml(request)
        self.assertEqual(resp.status_code, 200)
        import json

        body = json.loads(resp.content)
        self.assertTrue(body.get("ok"))
        it = body["nota"]["itens"][0]
        self.assertEqual(it["lote_numero"], "API1")
        self.assertEqual(it["lote_validade"], "2027-09-30")
        self.assertTrue(it["lote_xml"])
