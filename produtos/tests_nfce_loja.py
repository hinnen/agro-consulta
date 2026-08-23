"""NFC-e emitente por loja (Centro × Vila) — helpers sem SEFAZ."""
from __future__ import annotations

from django.test import SimpleTestCase


class NfceLojaConfigTests(SimpleTestCase):
    def test_loja_de_venda_deposito(self):
        from produtos.nfce_config_util import nfce_loja_de_venda

        class V:
            deposito = "vila"
            sessao_caixa = None

        self.assertEqual(nfce_loja_de_venda(V()), "vila")
        V.deposito = "centro"
        self.assertEqual(nfce_loja_de_venda(V()), "centro")
        V.deposito = ""
        self.assertEqual(nfce_loja_de_venda(V()), "centro")

    def test_cnpj_chave_e_loja(self):
        from produtos.nfce_config_util import nfce_cnpj_da_chave, nfce_loja_de_cnpj

        # cUF(35) + AAMM + CNPJ Vila + resto
        chave = "352608" + "48900774000286" + "650210000000011234567890"
        self.assertEqual(nfce_cnpj_da_chave(chave), "48900774000286")
        self.assertEqual(nfce_loja_de_cnpj("48900774000286"), "vila")
        self.assertEqual(nfce_loja_de_cnpj("48900774000103"), "centro")


class NfceDestDocumentoTests(SimpleTestCase):
    def test_cpf_e_cnpj_validos(self):
        from produtos.nfce_sp_emissao_util import (
            cnpj_valido,
            cpf_valido,
            documento_dest_nfce,
            tipo_documento_dest_nfce,
        )

        self.assertTrue(cpf_valido("52998224725"))
        self.assertEqual(documento_dest_nfce("529.982.247-25"), "52998224725")
        self.assertEqual(tipo_documento_dest_nfce("52998224725"), "CPF")
        self.assertTrue(cnpj_valido("11222333000181"))
        self.assertEqual(documento_dest_nfce("11.222.333/0001-81"), "11222333000181")
        self.assertEqual(tipo_documento_dest_nfce("11222333000181"), "CNPJ")
        self.assertEqual(documento_dest_nfce("48900774000103"), "48900774000103")
        self.assertEqual(documento_dest_nfce("123"), "")
        self.assertEqual(documento_dest_nfce("11111111111"), "")
        self.assertEqual(documento_dest_nfce("00000000000000"), "")

    def test_xml_dest_cpf_nao_muda(self):
        import xml.etree.ElementTree as ET

        from produtos.nfce_sp_emissao_util import NS, _preencher_dest_nfce

        inf = ET.Element(f"{{{NS}}}infNFe")
        _preencher_dest_nfce(inf, "52998224725")
        dest = inf.find(f"{{{NS}}}dest")
        self.assertIsNotNone(dest)
        self.assertEqual(dest.findtext(f"{{{NS}}}CPF"), "52998224725")
        self.assertIsNone(dest.find(f"{{{NS}}}CNPJ"))
        self.assertEqual(dest.findtext(f"{{{NS}}}indIEDest"), "9")
        self.assertIsNone(dest.find(f"{{{NS}}}xNome"))

    def test_xml_dest_cnpj_com_nome(self):
        import xml.etree.ElementTree as ET

        from produtos.nfce_sp_emissao_util import NS, _preencher_dest_nfce

        class V:
            cliente_nome = "PADARIA TESTE LTDA"

        inf = ET.Element(f"{{{NS}}}infNFe")
        _preencher_dest_nfce(inf, "11222333000181", V())
        dest = inf.find(f"{{{NS}}}dest")
        self.assertIsNotNone(dest)
        self.assertEqual(dest.findtext(f"{{{NS}}}CNPJ"), "11222333000181")
        self.assertIsNone(dest.find(f"{{{NS}}}CPF"))
        self.assertEqual(dest.findtext(f"{{{NS}}}indIEDest"), "9")
        self.assertEqual(dest.findtext(f"{{{NS}}}xNome"), "PADARIA TESTE LTDA")

    def test_payload_aceita_cnpj(self):
        from produtos.views_nfce import _nfce_opts_payload

        doc, sem = _nfce_opts_payload({"nfce_cpf": "11.222.333/0001-81"})
        self.assertEqual(doc, "11222333000181")
        self.assertFalse(sem)
        doc, sem = _nfce_opts_payload({"cliente_documento": "52998224725"})
        self.assertEqual(doc, "52998224725")
        self.assertFalse(sem)
        doc, sem = _nfce_opts_payload({"nfce_sem_identificacao": True})
        self.assertEqual(doc, "")
        self.assertTrue(sem)
