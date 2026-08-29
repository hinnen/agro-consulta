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


class NfceDescontoRateioTests(SimpleTestCase):
    """Bug loja #7 — desconto geral no total sem vDesc nos itens → SEFAZ 531."""

    def test_rateio_soma_bate_e_nao_passa_do_item(self):
        from decimal import Decimal

        from produtos.nfce_sp_emissao_util import _ratear_valor_proporcional

        pesos = [Decimal("10.00"), Decimal("20.00"), Decimal("30.00")]
        partes = _ratear_valor_proporcional(pesos, Decimal("6.00"))
        self.assertEqual(sum(partes), Decimal("6.00"))
        for p, peso in zip(partes, pesos):
            self.assertLessEqual(p, peso)
            self.assertGreaterEqual(p, Decimal("0"))

    def test_rateio_centavos_no_ultimo(self):
        from decimal import Decimal

        from produtos.nfce_sp_emissao_util import _ratear_valor_proporcional

        pesos = [Decimal("10.00"), Decimal("10.00")]
        partes = _ratear_valor_proporcional(pesos, Decimal("0.01"))
        self.assertEqual(sum(partes), Decimal("0.01"))
        self.assertEqual(sorted(partes), [Decimal("0.00"), Decimal("0.01")])

    def test_xml_itens_tem_vdesc_igual_total(self):
        import xml.etree.ElementTree as ET
        from datetime import datetime
        from decimal import Decimal
        from unittest.mock import patch

        from produtos.nfce_sp_emissao_util import NS, _montar_xml_nfce

        class Item:
            def __init__(self, vt, vu=None, qtd=1):
                self.quantidade = Decimal(str(qtd))
                self.valor_unitario = Decimal(str(vu if vu is not None else vt))
                self.valor_total = Decimal(str(vt))
                self.codigo = "GM1"
                self.produto_id_externo = "1"
                self.descricao = "PRODUTO TESTE"
                self.unidade = "UN"

        class Venda:
            pk = 99
            total = Decimal("90.00")
            frete = Decimal("0")
            pagamentos_json = [{"forma": "Dinheiro", "valor": 90}]
            cliente_nome = ""

        cfg = {
            "tp_amb": 2,
            "cnpj": "48900774000103",
            "razao_social": "TESTE",
            "fantasia": "TESTE",
            "logradouro": "RUA",
            "numero": "1",
            "bairro": "CENTRO",
            "cmun": "3524600",
            "cidade": "JACUPIRANGA",
            "uf": "SP",
            "cep": "11940000",
            "fone": "",
            "ie": "123",
            "csc_id": "1",
            "csc_token": "abc",
        }
        fiscal = {
            "ncm": "01012100",
            "cfop": "5102",
            "origem": "0",
            "csosn": "102",
            "cest": "",
        }
        itens = [Item("50.00"), Item("50.00")]
        with patch("produtos.nfce_sp_emissao_util.ibpt_valor_item", return_value=Decimal("0")), patch(
            "produtos.nfce_sp_emissao_util.calcular_ibpt_venda_itens",
            return_value={"ibpt_texto": "Trib approx R$ 0,00"},
        ), patch(
            "produtos.nfce_sp_emissao_util._qr_code_url",
            return_value="https://example.com/qr",
        ):
            xml_body, _qr = _montar_xml_nfce(
                cfg,
                Venda(),
                itens,
                serie=21,
                numero=1,
                chave="35" + "0" * 42,
                dh_emi=datetime(2026, 8, 29, 12, 0, 0),
                cpf_dest="",
                fiscal_itens=[fiscal, fiscal],
            )
        root = ET.fromstring(xml_body)
        ns = {"n": NS}
        v_desc_tot = Decimal(root.findtext(".//n:ICMSTot/n:vDesc", namespaces=ns) or "0")
        self.assertEqual(v_desc_tot, Decimal("10.00"))
        v_desc_itens = [
            Decimal(el.text or "0")
            for el in root.findall(".//n:det/n:prod/n:vDesc", namespaces=ns)
        ]
        self.assertEqual(sum(v_desc_itens), Decimal("10.00"))
        self.assertEqual(len(v_desc_itens), 2)
        v_nf = Decimal(root.findtext(".//n:ICMSTot/n:vNF", namespaces=ns) or "0")
        self.assertEqual(v_nf, Decimal("90.00"))
        v_pag = Decimal(root.findtext(".//n:detPag/n:vPag", namespaces=ns) or "0")
        self.assertEqual(v_pag, Decimal("90.00"))

    def test_xml_frete_e_desconto(self):
        import xml.etree.ElementTree as ET
        from datetime import datetime
        from decimal import Decimal
        from unittest.mock import patch

        from produtos.nfce_sp_emissao_util import NS, _montar_xml_nfce

        class Item:
            quantidade = Decimal("1")
            valor_unitario = Decimal("100.00")
            valor_total = Decimal("100.00")
            codigo = "GM1"
            produto_id_externo = "1"
            descricao = "PROD"
            unidade = "UN"

        class Venda:
            pk = 1
            total = Decimal("105.00")  # 100 + frete 10 − desc 5
            frete = Decimal("10.00")
            pagamentos_json = [{"forma": "Dinheiro", "valor": 105}]
            cliente_nome = ""

        cfg = {
            "tp_amb": 2,
            "cnpj": "48900774000103",
            "razao_social": "T",
            "fantasia": "T",
            "logradouro": "R",
            "numero": "1",
            "bairro": "C",
            "cmun": "3524600",
            "cidade": "J",
            "uf": "SP",
            "cep": "11940000",
            "fone": "",
            "ie": "1",
            "csc_id": "1",
            "csc_token": "x",
        }
        fis = {"ncm": "01012100", "cfop": "5102", "origem": "0", "csosn": "102", "cest": ""}
        with patch("produtos.nfce_sp_emissao_util.ibpt_valor_item", return_value=Decimal("0")), patch(
            "produtos.nfce_sp_emissao_util.calcular_ibpt_venda_itens",
            return_value={"ibpt_texto": "ok"},
        ), patch("produtos.nfce_sp_emissao_util._qr_code_url", return_value="https://x"):
            xml_body, _ = _montar_xml_nfce(
                cfg,
                Venda(),
                [Item()],
                serie=21,
                numero=2,
                chave="35" + "0" * 42,
                dh_emi=datetime(2026, 8, 29, 12, 0, 0),
                cpf_dest="",
                fiscal_itens=[fis],
            )
        root = ET.fromstring(xml_body)
        ns = {"n": NS}
        self.assertEqual(Decimal(root.findtext(".//n:ICMSTot/n:vDesc", namespaces=ns)), Decimal("5.00"))
        self.assertEqual(Decimal(root.findtext(".//n:ICMSTot/n:vFrete", namespaces=ns)), Decimal("10.00"))
        self.assertEqual(Decimal(root.findtext(".//n:ICMSTot/n:vNF", namespaces=ns)), Decimal("105.00"))
        self.assertEqual(
            Decimal(root.findtext(".//n:det/n:prod/n:vDesc", namespaces=ns) or "0"),
            Decimal("5.00"),
        )

    def test_xml_total_zero_com_frete_abate_frete(self):
        """Desconto cobre itens+frete: vDesc nos itens + frete zerado (531 ok)."""
        import xml.etree.ElementTree as ET
        from datetime import datetime
        from decimal import Decimal
        from unittest.mock import patch

        from produtos.nfce_sp_emissao_util import NS, _montar_xml_nfce

        class Item:
            quantidade = Decimal("1")
            valor_unitario = Decimal("50.00")
            valor_total = Decimal("50.00")
            codigo = "G"
            produto_id_externo = "1"
            descricao = "P"
            unidade = "UN"

        class Venda:
            pk = 2
            total = Decimal("0")
            frete = Decimal("10.00")
            pagamentos_json = []
            cliente_nome = ""
            forma_pagamento = "Dinheiro"

        cfg = {
            "tp_amb": 2,
            "cnpj": "48900774000103",
            "razao_social": "T",
            "fantasia": "T",
            "logradouro": "R",
            "numero": "1",
            "bairro": "C",
            "cmun": "3524600",
            "cidade": "J",
            "uf": "SP",
            "cep": "11940000",
            "fone": "",
            "ie": "1",
            "csc_id": "1",
            "csc_token": "x",
        }
        fis = {"ncm": "01012100", "cfop": "5102", "origem": "0", "csosn": "102", "cest": ""}
        with patch("produtos.nfce_sp_emissao_util.ibpt_valor_item", return_value=Decimal("0")), patch(
            "produtos.nfce_sp_emissao_util.calcular_ibpt_venda_itens",
            return_value={"ibpt_texto": "ok"},
        ), patch("produtos.nfce_sp_emissao_util._qr_code_url", return_value="https://x"):
            xml_body, _ = _montar_xml_nfce(
                cfg,
                Venda(),
                [Item(), Item()],
                serie=21,
                numero=3,
                chave="35" + "0" * 42,
                dh_emi=datetime(2026, 8, 29, 12, 0, 0),
                cpf_dest="",
                fiscal_itens=[fis, fis],
            )
        root = ET.fromstring(xml_body)
        ns = {"n": NS}
        v_desc = Decimal(root.findtext(".//n:ICMSTot/n:vDesc", namespaces=ns) or "0")
        v_frete = Decimal(root.findtext(".//n:ICMSTot/n:vFrete", namespaces=ns) or "0")
        v_nf = Decimal(root.findtext(".//n:ICMSTot/n:vNF", namespaces=ns) or "0")
        soma = sum(
            Decimal(el.text or "0")
            for el in root.findall(".//n:det/n:prod/n:vDesc", namespaces=ns)
        )
        self.assertEqual(v_desc, Decimal("100.00"))
        self.assertEqual(soma, Decimal("100.00"))
        self.assertEqual(v_frete, Decimal("0.00"))
        self.assertEqual(v_nf, Decimal("0.00"))

