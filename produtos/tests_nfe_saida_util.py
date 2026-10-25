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

    def test_validar_destinatario_cnpj_ie_conforme_situacao(self):
        from produtos.nfe_saida_util import validar_destinatario

        base = {
            "nome": "Fazenda X",
            "documento": "11222333000181",
            "uf": "SP",
            "cidade": "São Paulo",
            "logradouro": "Rua A",
        }
        dest = {**base, "ie": ""}
        self.assertEqual(validar_destinatario(dest), [])
        dest = {**base, "situacao_icms": "contribuinte", "ie": ""}
        self.assertTrue(any("Inscrição" in e for e in validar_destinatario(dest)))
        dest = {**base, "situacao_icms": "isento", "ie": "ISENTO"}
        self.assertEqual(validar_destinatario(dest), [])
        dest = {**base, "situacao_icms": "nao_contribuinte", "ie": ""}
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

    def test_grupos_xml_csosn_mapeados(self):
        from produtos.nfe_saida_tributacao_util import _apurar_icms_sn, _grupo_xml_icms_csosn

        for cs in ("102", "103", "300", "400"):
            self.assertEqual(_grupo_xml_icms_csosn(cs), "ICMSSN102")
            g, campos, bl = _apurar_icms_sn(cs, "0", Decimal("10"))
            self.assertEqual(g, "ICMSSN102")
            self.assertEqual(campos["CSOSN"], cs)
            self.assertEqual(bl, [])
        for cs in ("202", "203"):
            self.assertEqual(_grupo_xml_icms_csosn(cs), "ICMSSN202")
            g, _, bl = _apurar_icms_sn(cs, "0", Decimal("10"))
            self.assertEqual(g, "ICMSSN202")
            self.assertTrue(bl)

    def test_cfop_interestadual_bloqueia_5102_na_linha(self):
        from produtos.nfe_saida_tributacao_util import apurar_nfe_saida
        from produtos.nfe_saida_util import doc_novo_vazio

        doc = doc_novo_vazio()
        doc["destinatario"].update(
            {
                "nome": "Cliente RJ",
                "documento": "39053344705",
                "uf": "RJ",
                "cidade": "Rio",
                "logradouro": "Rua",
            }
        )
        doc["itens"] = [
            {
                "descricao": "P",
                "qtd": 1,
                "v_unit": 10,
                "v_total": 10,
                "ncm": "23099020",
                "cfop": "5102",
                "csosn": "102",
            }
        ]
        ap = apurar_nfe_saida(doc, uf_emit="SP")
        self.assertTrue(any("CFOP 5102" in b for b in ap.bloqueios))
        self.assertEqual(ap.itens[0].cfop, "6102")

    def test_cst_pis_do_cadastro_produto(self):
        from unittest.mock import patch

        from produtos.nfe_saida_tributacao_util import apurar_nfe_saida
        from produtos.nfe_saida_util import doc_novo_vazio

        doc = doc_novo_vazio()
        doc["destinatario"].update(
            {
                "nome": "C",
                "documento": "39053344705",
                "uf": "SP",
                "cidade": "J",
                "logradouro": "R",
            }
        )
        doc["itens"] = [
            {"descricao": "P", "qtd": 1, "v_unit": 1, "v_total": 1, "ncm": "23099020", "produto_id": "99", "csosn": "102"}
        ]
        with patch(
            "produtos.nfce_fiscal_produto_util.fiscal_por_produto_id",
            return_value={"ncm": "23099020", "csosn": "102", "cst_pis_cofins": "06"},
        ):
            ap = apurar_nfe_saida(doc, uf_emit="SP")
        self.assertEqual(ap.itens[0].cst_pis, "06")
        self.assertEqual(ap.itens[0].pis_grupo, "PISNT")

    def test_xml_interestadual_cfop_6xxx(self):
        from datetime import datetime

        from produtos.nfe_saida_sp_emissao_util import _montar_xml_nfe_saida
        from produtos.nfe_saida_util import doc_novo_vazio

        doc = doc_novo_vazio()
        doc["destinatario"].update(
            {
                "nome": "Cliente",
                "documento": "39053344705",
                "uf": "RJ",
                "cidade": "Rio",
                "logradouro": "Rua A",
                "numero": "1",
                "bairro": "Centro",
                "cep": "20000000",
            }
        )
        doc["itens"] = [
            {
                "descricao": "P",
                "qtd": 1,
                "v_unit": 10,
                "v_total": 10,
                "ncm": "23099020",
                "cfop": "6102",
                "csosn": "102",
                "cst_pis_cofins": "49",
            }
        ]
        doc["pagamento"] = [{"tPag": "01", "valor": 10}]
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
        xml = _montar_xml_nfe_saida(
            doc,
            cfg,
            serie=1,
            numero=3,
            chave="3" * 43 + "5",
            dh_emi=datetime(2026, 10, 1, 10, 0, 0),
        )
        self.assertIn("<CFOP>6102</CFOP>", xml)
        self.assertIn("<idDest>2</idDest>", xml)

    def test_xml_xsd_obrigatorio(self):
        from datetime import datetime

        from produtos.nfe_saida_sp_emissao_util import _montar_xml_nfe_saida
        from produtos.nfe_saida_util import doc_novo_vazio
        from produtos.nfe_saida_xml_validate_util import validar_xml_nfe_xsd, xsd_nfe_disponivel

        self.assertTrue(xsd_nfe_disponivel(), "Pacote XSD NF-e 4.00 deve estar em produtos/schemas/nfe/")
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
                "descricao": "P",
                "qtd": 1,
                "v_unit": 10,
                "v_total": 10,
                "ncm": "23099020",
                "cfop": "5102",
                "csosn": "103",
                "cst_pis_cofins": "49",
            }
        ]
        doc["pagamento"] = [{"tPag": "01", "valor": 10}]
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
        xml = _montar_xml_nfe_saida(
            doc, cfg, serie=1, numero=4, chave="3" * 43 + "5", dh_emi=datetime(2026, 10, 1, 10, 0, 0)
        )
        self.assertIn("<CSOSN>103</CSOSN>", xml)
        self.assertIn("<ICMSSN102>", xml)
        ok, det = validar_xml_nfe_xsd(xml)
        self.assertEqual(det, "ok")
        self.assertTrue(ok, det)


class NfeSaidaPrefillVendaTests(SimpleTestCase):
    def _venda_mock(self, *, uf: str, items):
        class Qs:
            def __init__(self, rows):
                self._rows = rows

            def all(self):
                return self._rows

        class V:
            pk = 9001
            cliente_nome = "Cliente teste"
            cliente_documento = "39053344705"
            cliente_id_erp = ""

            def __init__(self, uf_dest, rows):
                self._uf = uf_dest
                self.itens = Qs(rows)

        class It:
            def __init__(self, pid, desc="Prod", qtd=1, vu=10.0, vt=10.0):
                self.produto_id_externo = pid
                self.codigo = pid
                self.descricao = desc
                self.quantidade = qtd
                self.valor_unitario = vu
                self.valor_total = vt
                self.unidade = "UN"

        v = V(uf, items)
        return v

    def test_prefill_interestadual_cfop_6102_sem_cadastro(self):
        from unittest.mock import patch

        from produtos.nfe_saida_util import itens_de_venda, prefill_de_venda_agro

        v = self._venda_mock(uf="RJ", items=[type("It", (), {"produto_id_externo": "1", "codigo": "1", "descricao": "P", "quantidade": 1, "valor_unitario": 10, "valor_total": 10, "unidade": "UN"})()])
        with patch("produtos.nfe_saida_util.destinatario_de_venda", return_value={"uf": "RJ"}):
            with patch(
                "produtos.nfe_saida_util._fiscal_produto_buscar",
                return_value={"ncm": "12345678", "cfop": "", "csosn": "102", "origem": "0"},
            ):
                itens = itens_de_venda(v)
        self.assertEqual(itens[0]["cfop"], "6102")
        self.assertEqual(itens[0]["ncm"], "12345678")

    def test_prefill_preserva_ncm_23099020_cadastrado(self):
        from unittest.mock import patch

        from produtos.nfe_saida_util import _fiscal_produto_buscar

        with patch(
            "produtos.nfce_fiscal_produto_util.fiscal_por_produto_id",
            return_value={"ncm": "23099020", "cfop": "5102", "csosn": "102", "origem": "0"},
        ):
            fis = _fiscal_produto_buscar("42")
        self.assertEqual(fis["ncm"], "23099020")

    def test_prefill_venda_xml_xsd(self):
        from datetime import datetime
        from unittest.mock import patch

        from produtos.nfe_saida_sp_emissao_util import _montar_xml_nfe_saida
        from produtos.nfe_saida_util import pagamentos_de_venda, prefill_de_venda_agro
        from produtos.nfe_saida_xml_validate_util import validar_xml_nfe_xsd

        item = type(
            "It",
            (),
            {
                "produto_id_externo": "10",
                "codigo": "10",
                "descricao": "Ração",
                "quantidade": 2,
                "valor_unitario": 25,
                "valor_total": 50,
                "unidade": "UN",
            },
        )()
        v = self._venda_mock(uf="SP", items=[item])
        v.total = 50
        v.deposito = ""
        with patch("produtos.nfe_saida_util.destinatario_de_venda") as dest_m:
            dest_m.return_value = {
                "nome": "Consumidor",
                "documento": "39053344705",
                "uf": "SP",
                "cidade": "Jacupiranga",
                "logradouro": "Rua A",
                "numero": "1",
                "bairro": "Centro",
                "cep": "11940000",
            }
            with patch(
                "produtos.nfe_saida_util._fiscal_produto_buscar",
                return_value={
                    "ncm": "23099020",
                    "cfop": "5102",
                    "csosn": "102",
                    "origem": "0",
                    "cst_pis_cofins": "49",
                },
            ):
                with patch("produtos.nfe_saida_util.pagamentos_de_venda", return_value=[{"tPag": "01", "valor": 50}]):
                    doc = prefill_de_venda_agro(v, usuario="test")
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
        xml = _montar_xml_nfe_saida(
            doc, cfg, serie=1, numero=5, chave="3" * 43 + "5", dh_emi=datetime(2026, 10, 2, 12, 0, 0)
        )
        ok, det = validar_xml_nfe_xsd(xml)
        self.assertTrue(ok, det)
        self.assertIn("<NCM>23099020</NCM>", xml)


class NfeSaidaSimulacaoEmitTests(SimpleTestCase):
    def test_homolog_sem_opt_in_nao_simula(self):
        import os
        from unittest.mock import patch

        from produtos.nfe_saida_emissao_util import emitir_nfe_saida_documento, emissao_simulada_ativa
        from produtos.nfe_saida_util import doc_novo_vazio

        os.environ.pop("NFE_SAIDA_SIMULAR_EMIT", None)
        self.assertFalse(emissao_simulada_ativa(2))
        doc = doc_novo_vazio()
        doc["tp_amb"] = 2
        doc["destinatario"].update(
            {
                "nome": "C",
                "documento": "39053344705",
                "uf": "SP",
                "cidade": "J",
                "logradouro": "R",
            }
        )
        doc["itens"] = [
            {"descricao": "P", "qtd": 1, "v_unit": 10, "v_total": 10, "ncm": "23099020", "cfop": "5102", "csosn": "102"}
        ]
        doc["pagamento"] = [{"tPag": "01", "valor": 10}]
        with patch("produtos.sefaz_dfe_client.distribuicao_dfe_configurada", return_value=True):
            with patch(
                "produtos.nfe_saida_sp_emissao_util.emitir_nfe_saida_sefaz",
                return_value={"ok": False, "status": "rejeitada", "erro": "mock", "c_stat": 0},
            ) as sefaz:
                emitir_nfe_saida_documento(doc, cnpj_emit="11222333000181")
                sefaz.assert_called_once()

    def test_simulacao_exige_flag(self):
        import os
        from unittest.mock import patch

        from produtos.nfe_saida_emissao_util import emitir_nfe_saida_documento
        from produtos.nfe_saida_util import doc_novo_vazio

        os.environ["NFE_SAIDA_SIMULAR_EMIT"] = "1"
        doc = doc_novo_vazio()
        doc["tp_amb"] = 2
        doc["destinatario"].update(
            {
                "nome": "C",
                "documento": "39053344705",
                "uf": "SP",
                "cidade": "J",
                "logradouro": "R",
            }
        )
        doc["itens"] = [
            {"descricao": "P", "qtd": 1, "v_unit": 10, "v_total": 10, "ncm": "23099020", "cfop": "5102", "csosn": "102"}
        ]
        doc["pagamento"] = [{"tPag": "01", "valor": 10}]
        with patch("produtos.sefaz_dfe_client.distribuicao_dfe_configurada", return_value=True):
            out = emitir_nfe_saida_documento(doc, cnpj_emit="11222333000181")
        self.assertTrue(out.get("simulado"))
        self.assertFalse(out.get("autorizacao_sefaz_real", True))
        os.environ.pop("NFE_SAIDA_SIMULAR_EMIT", None)
