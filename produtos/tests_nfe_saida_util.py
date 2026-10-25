"""NF-e saída 55 — validação e defaults (sem Mongo/SEFAZ)."""
from __future__ import annotations

from decimal import Decimal

from django.test import SimpleTestCase, TestCase


class NfeSaidaDanfeLayoutTests(SimpleTestCase):
    def test_html_danfe_padrao(self):
        from produtos.nfe_saida_danfe_html import montar_html_danfe

        doc = {
            "status": "autorizada",
            "tp_amb": 2,
            "chave": "35261011222333000181550010000000011234567890",
            "nNF": 1,
            "serie": 1,
            "protocolo": "SIM123",
            "destinatario": {
                "nome": "Cliente",
                "documento": "52998224725",
                "uf": "SP",
                "cidade": "Jacupiranga",
                "logradouro": "Rua A",
                "numero": "1",
            },
            "fiscal": {"natureza": "VENDA"},
            "itens": [
                {
                    "descricao": "Produto",
                    "qtd": 1,
                    "v_unit": 10,
                    "v_total": 10,
                    "ncm": "23099020",
                    "cfop": "5102",
                    "csosn": "102",
                }
            ],
        }
        html = montar_html_danfe(doc, emitente={"razao": "Loja", "cnpj": "11222333000181", "uf": "SP"})
        self.assertIn("DANFE", html)
        self.assertIn("RECEBEMOS DE", html)
        self.assertIn("SEM VALOR FISCAL", html)
        self.assertIn("Produto", html)
        self.assertIn("data:image/png;base64", html)
        self.assertIn("Folha", html)

    def test_danfe_exemplo(self):
        from produtos.nfe_saida_danfe_html import gerar_html_danfe_exemplo

        html = gerar_html_danfe_exemplo()
        self.assertIn("AGRO MAIS", html.upper())
        self.assertNotIn("SEM VALOR FISCAL", html)


class NfeSaidaCfopTests(SimpleTestCase):
    def test_cfop_interno_e_interestadual(self):
        from produtos.nfe_saida_util import cfop_padrao_destino

        self.assertEqual(cfop_padrao_destino("SP", "SP"), "5102")
        self.assertEqual(cfop_padrao_destino("RJ", "SP"), "6102")


class NfeSaidaDestinatarioTests(SimpleTestCase):
    def test_documento_tipo(self):
        from produtos.nfe_saida_util import documento_dest_tipo

        self.assertEqual(documento_dest_tipo("11222333000181"), "cnpj")
        self.assertEqual(documento_dest_tipo("52998224725"), "cpf")
        self.assertEqual(documento_dest_tipo("123"), "")

    def test_validar_destinatario_cnpj_exige_ie(self):
        from produtos.nfe_saida_util import validar_destinatario

        dest = {
            "nome": "Fazenda X",
            "documento": "11222333000181",
            "uf": "SP",
            "cidade": "São Paulo",
            "logradouro": "Rua A",
            "ie": "",
        }
        erros = validar_destinatario(dest)
        self.assertTrue(any("Inscrição" in e for e in erros))
        dest["ie"] = "ISENTO"
        self.assertEqual(validar_destinatario(dest), [])


class NfeSaidaPostgresDbTests(TestCase):
    def test_criar_e_listar_postgres(self):
        from unittest.mock import patch

        from produtos.nfe_saida_util import criar_nfe_saida_vazia, listar_nfe_saida, obter_documento_nfe_saida

        with patch("produtos.nfe_saida_util.nfe_saida_usa_postgres", return_value=True):
            r = criar_nfe_saida_vazia(None, usuario="pytest")
            self.assertTrue(r.get("ok"))
            doc_id = r["id"]
            self.assertEqual(len(doc_id), 24)
            doc = obter_documento_nfe_saida(None, doc_id)
            self.assertIsNotNone(doc)
            self.assertEqual(doc.get("status"), "rascunho")
            itens = listar_nfe_saida(None, filtro="rascunho")
            self.assertTrue(any(x.get("id") == doc_id for x in itens))


class NfeSaidaPostgresStoreTests(SimpleTestCase):
    def test_persistencia_ok_sem_mongo(self):
        from unittest.mock import patch

        from produtos.nfe_saida_util import criar_nfe_saida_vazia, listar_nfe_saida, nfe_saida_persistencia_ok

        with patch("produtos.nfe_saida_util.nfe_saida_usa_postgres", return_value=True):
            with patch("produtos.nfe_saida_rascunho_pg_util.pg_insert", return_value="aabbccddeeff001122334455"):
                self.assertTrue(nfe_saida_persistencia_ok(None))
                r = criar_nfe_saida_vazia(None, usuario="teste")
                self.assertTrue(r.get("ok"))
                self.assertEqual(r.get("id"), "aabbccddeeff001122334455")

        with patch("produtos.nfe_saida_util.nfe_saida_usa_postgres", return_value=True):
            with patch("produtos.nfe_saida_rascunho_pg_util.pg_find_filter", return_value=[]):
                self.assertEqual(listar_nfe_saida(None), [])


class NfeSaidaEmailTests(SimpleTestCase):
    def test_email_destinatario_valido(self):
        from produtos.nfe_saida_email_util import email_destinatario_valido, mesclar_destinatario_emit

        self.assertTrue(email_destinatario_valido("a@b.com.br"))
        self.assertFalse(email_destinatario_valido("invalido"))
        doc = {"destinatario": {"nome": "X", "email": ""}}
        out = mesclar_destinatario_emit(doc, {"email": " Renan@exemplo.com "})
        self.assertEqual(out["destinatario"]["email"], "renan@exemplo.com")


class NfeSaidaPagamentoTests(SimpleTestCase):
    def test_validar_pagamentos_total(self):
        from produtos.nfe_saida_util import validar_pagamentos

        total = Decimal("100.00")
        self.assertEqual(validar_pagamentos([], total), ["Informe forma de pagamento (etapa 4)."])
        ok = [{"forma": "Dinheiro", "valor": 100}]
        self.assertEqual(validar_pagamentos(ok, total), [])
        bad = [{"forma": "Dinheiro", "valor": 50}]
        self.assertTrue(validar_pagamentos(bad, total))


class NfeSaidaXmlMontagemTests(SimpleTestCase):
    def test_xml_mod_55(self):
        from datetime import datetime

        from produtos.nfe_saida_sp_emissao_util import _montar_xml_nfe_saida
        from produtos.nfe_saida_util import doc_novo_vazio

        doc = doc_novo_vazio()
        doc["destinatario"].update(
            {
                "nome": "Cliente Teste",
                "documento": "39053344705",
                "uf": "SP",
                "cidade": "Jacupiranga",
                "logradouro": "Rua A",
                "numero": "1",
                "bairro": "Centro",
                "cep": "11940000",
            }
        )
        doc["itens"] = [
            {
                "descricao": "Produto",
                "qtd": 1,
                "v_unit": 10,
                "v_total": 10,
                "ncm": "23099020",
                "cfop": "5102",
                "csosn": "102",
            }
        ]
        doc["pagamento"] = [{"forma": "Dinheiro", "tPag": "01", "valor": 10}]
        cfg = {
            "tp_amb": 2,
            "cnpj": "11222333000181",
            "razao_social": "Emitente Teste",
            "fantasia": "Emitente",
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
        chave = "3" * 43 + "5"
        xml = _montar_xml_nfe_saida(
            doc,
            cfg,
            serie=1,
            numero=99,
            chave=chave,
            dh_emi=datetime(2026, 3, 18, 12, 0, 0),
        )
        self.assertIn("<mod>55</mod>", xml)
        self.assertIn("<tpImp>1</tpImp>", xml)
        self.assertIn("<indIntermed>0</indIntermed>", xml)
        self.assertIn("NF-E EMITIDA EM AMBIENTE DE HOMOLOGACAO", xml)
        import re

        dest_blk = re.search(r"<dest>.*?</dest>", xml)
        self.assertIsNotNone(dest_blk)
        dest_xml = dest_blk.group(0)
        self.assertLess(dest_xml.find("<CPF>"), dest_xml.find("<xNome>"))

    def test_xml_dest_ordem_cnpj(self):
        from datetime import datetime

        from produtos.nfe_saida_sp_emissao_util import _montar_xml_nfe_saida
        from produtos.nfe_saida_util import doc_novo_vazio

        doc = doc_novo_vazio()
        doc["tp_amb"] = 1
        doc["destinatario"].update(
            {
                "nome": "Empresa",
                "documento": "11222333000181",
                "ie": "ISENTO",
                "uf": "SP",
                "cidade": "Jacupiranga",
                "logradouro": "Rua A",
                "numero": "1",
                "bairro": "Centro",
                "cep": "11940000",
            }
        )
        doc["itens"] = [
            {
                "descricao": "P",
                "qtd": 1,
                "v_unit": 1,
                "v_total": 1,
                "ncm": "23099020",
                "cfop": "5102",
                "csosn": "102",
            }
        ]
        doc["pagamento"] = [{"tPag": "01", "valor": 1}]
        cfg = {
            "tp_amb": 1,
            "cnpj": "48900774000110",
            "razao_social": "Emitente",
            "fantasia": "Emitente",
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
        chave = "3" * 43 + "5"
        xml = _montar_xml_nfe_saida(
            doc, cfg, serie=2, numero=1, chave=chave, dh_emi=datetime(2026, 3, 18, 12, 0, 0)
        )
        import re

        dest_blk = re.search(r"<dest>.*?</dest>", xml)
        self.assertIsNotNone(dest_blk)
        dest_xml = dest_blk.group(0)
        self.assertLess(dest_xml.find("<CNPJ>"), dest_xml.find("<xNome>"))


class NfeSaidaTributacaoMotorTests(SimpleTestCase):
    def test_dest_cnpj_ie_nao_forca_revenda(self):
        from produtos.nfe_saida_tributacao_util import normalizar_destinatario_fiscal

        d = normalizar_destinatario_fiscal(
            {
                "documento": "11222333000181",
                "ie": "123456789012",
                "finalidade_mercadoria": "uso_proprio",
            }
        )
        self.assertEqual(d["ind_final"], 1)
        self.assertEqual(d["ind_ie_dest"], "1")

    def test_produtor_cpf_com_ie(self):
        from produtos.nfe_saida_tributacao_util import normalizar_destinatario_fiscal

        d = normalizar_destinatario_fiscal(
            {"documento": "39053344705", "ie": "1234567890", "situacao_icms": "contribuinte"}
        )
        self.assertEqual(d["ind_ie_dest"], "1")
        self.assertEqual(d["ie_xml"], "1234567890")

    def test_xml_csosn102_pis49(self):
        import re
        from datetime import datetime

        from produtos.nfe_saida_sp_emissao_util import _montar_xml_nfe_saida
        from produtos.nfe_saida_util import doc_novo_vazio

        doc = doc_novo_vazio()
        doc["destinatario"].update(
            {
                "nome": "Consumidor",
                "documento": "39053344705",
                "uf": "SP",
                "cidade": "Jacupiranga",
                "logradouro": "Rua A",
                "numero": "1",
                "bairro": "Centro",
                "cep": "11940000",
            }
        )
        doc["itens"] = [
            {
                "descricao": "Ração",
                "qtd": 1,
                "v_unit": 50,
                "v_total": 50,
                "ncm": "23099020",
                "cfop": "5102",
                "csosn": "102",
                "cst_pis_cofins": "49",
            }
        ]
        doc["pagamento"] = [{"tPag": "01", "valor": 50}]
        cfg = {
            "tp_amb": 2,
            "cnpj": "11222333000181",
            "razao_social": "Emitente",
            "fantasia": "Emitente",
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
        chave = "3" * 43 + "5"
        xml = _montar_xml_nfe_saida(
            doc, cfg, serie=1, numero=1, chave=chave, dh_emi=datetime(2026, 10, 1, 10, 0, 0)
        )
        self.assertIn("<ICMSSN102>", xml)
        self.assertNotRegex(xml, r"<ICMSSN102>.*?<CSOSN>500</CSOSN>", re.DOTALL)
        self.assertIn("<PISOutr>", xml)
        self.assertIn("<CST>49</CST>", xml)
        self.assertNotIn("<PISNT>", xml)
        self.assertNotIn("<CST>07</CST>", xml)

    def test_csosn101_bloqueio_sem_aliquota(self):
        from produtos.nfe_saida_tributacao_util import apurar_nfe_saida
        from produtos.nfe_saida_util import doc_novo_vazio
        from unittest.mock import patch

        doc = doc_novo_vazio()
        doc["destinatario"].update(
            {
                "nome": "Loja",
                "documento": "11222333000181",
                "ie": "123",
                "uf": "SP",
                "cidade": "X",
                "logradouro": "R",
                "finalidade_mercadoria": "revenda",
            }
        )
        doc["itens"] = [
            {
                "descricao": "P",
                "qtd": 1,
                "v_unit": 10,
                "v_total": 10,
                "ncm": "23099020",
                "csosn": "101",
            }
        ]
        with patch("produtos.nfe_saida_tributacao_util._cfg_cred_sn_aliquota", return_value=None):
            ap = apurar_nfe_saida(doc, uf_emit="SP")
        self.assertTrue(any("NFE_SAIDA_PCRED_SN_ALIQ" in b for b in ap.bloqueios))

    def test_csosn101_xml_credito(self):
        from datetime import datetime
        from unittest.mock import patch

        from produtos.nfe_saida_sp_emissao_util import _montar_xml_nfe_saida
        from produtos.nfe_saida_util import doc_novo_vazio

        doc = doc_novo_vazio()
        doc["destinatario"].update(
            {
                "nome": "Revenda",
                "documento": "11222333000181",
                "ie": "123456789012",
                "uf": "SP",
                "cidade": "Jacupiranga",
                "logradouro": "Rua A",
                "numero": "1",
                "bairro": "Centro",
                "cep": "11940000",
                "finalidade_mercadoria": "revenda",
            }
        )
        doc["itens"] = [
            {
                "descricao": "P",
                "qtd": 1,
                "v_unit": 100,
                "v_total": 100,
                "ncm": "23099020",
                "csosn": "101",
                "cst_pis_cofins": "49",
            }
        ]
        doc["pagamento"] = [{"tPag": "01", "valor": 100}]
        cfg = {
            "tp_amb": 2,
            "cnpj": "11222333000181",
            "razao_social": "Emitente",
            "fantasia": "Emitente",
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
        with patch("produtos.nfe_saida_tributacao_util._cfg_cred_sn_aliquota", return_value=Decimal("2.5")):
            xml = _montar_xml_nfe_saida(
                doc,
                cfg,
                serie=1,
                numero=2,
                chave="3" * 43 + "5",
                dh_emi=datetime(2026, 10, 1, 10, 0, 0),
            )
        self.assertIn("<ICMSSN101>", xml)
        self.assertIn("<pCredSN>2.5</pCredSN>", xml)
        self.assertIn("<vCredICMSSN>2.50</vCredICMSSN>", xml)
