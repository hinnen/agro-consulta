"""NF-e saída 55 — validação e defaults (sem Mongo/SEFAZ)."""
from __future__ import annotations

import re
from contextlib import contextmanager
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

from django.test import SimpleTestCase, TestCase

from produtos.models import ClienteAgro
from produtos.nfe_saida_regras_validas_util import RegraFiscalResolvida


def _regra_da_linha(doc, ln, idx, **kwargs):
    cfop = re.sub(r"\D", "", str(ln.get("cfop") or ""))[:4]
    csosn = re.sub(r"\D", "", str(ln.get("csosn") or ""))[:3]
    cst = re.sub(r"\D", "", str(ln.get("cst_pis") or ln.get("cst_pis_cofins") or "49"))[:2]
    cst_c = re.sub(r"\D", "", str(ln.get("cst_cofins") or cst))[:2]
    if len(cfop) != 4 or len(csosn) != 3:
        return None, [f"Item {idx}: sem regra Validada."]
    return (
        RegraFiscalResolvida(
            pk=1,
            estabelecimento="centro",
            escopo="geral",
            cfop=cfop,
            csosn=csosn,
            cst_pis=cst or "49",
            cst_cofins=cst_c or "49",
        ),
        [],
    )


@contextmanager
def xml_com_regra(*, p_cred=None):
    patches = [
        patch("produtos.nfe_saida_tributacao_util.resolver_regra_item", side_effect=_regra_da_linha),
        patch("produtos.nfe_saida_tributacao_util.estabelecimento_do_doc", return_value="centro"),
        patch("produtos.nfe_saida_tributacao_util.cmun_destinatario_ou_erro", return_value=("3524600", None)),
    ]
    if p_cred is not None:
        patches.append(
            patch(
                "produtos.nfe_saida_tributacao_util.aliquota_credito_sn_validada",
                return_value=(p_cred, None),
            )
        )
    for p in patches:
        p.start()
    try:
        yield
    finally:
        for p in reversed(patches):
            p.stop()


class _XmlRegraPatchMixin:
    def setUp(self):
        self._p_regra = patch(
            "produtos.nfe_saida_tributacao_util.resolver_regra_item",
            side_effect=_regra_da_linha,
        )
        self._p_loja = patch(
            "produtos.nfe_saida_tributacao_util.estabelecimento_do_doc",
            return_value="centro",
        )
        self._p_cmun = patch(
            "produtos.nfe_saida_tributacao_util.cmun_destinatario_ou_erro",
            return_value=("3524600", None),
        )
        self._p_regra.start()
        self._p_loja.start()
        self._p_cmun.start()

    def tearDown(self):
        self._p_cmun.stop()
        self._p_loja.stop()
        self._p_regra.stop()


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


class NfeSaidaXmlMontagemTests(_XmlRegraPatchMixin, SimpleTestCase):
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
    def _xml_patches(self):
        return (
            patch("produtos.nfe_saida_tributacao_util.resolver_regra_item", side_effect=_regra_da_linha),
            patch("produtos.nfe_saida_tributacao_util.estabelecimento_do_doc", return_value="centro"),
            patch("produtos.nfe_saida_tributacao_util.cmun_destinatario_ou_erro", return_value=("3524600", None)),
        )
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
        p1, p2, p3 = self._xml_patches()
        with p1, p2, p3:
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
        ap = apurar_nfe_saida(doc, uf_emit="SP")
        self.assertTrue(ap.bloqueios)

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
                "cfop": "5102",
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
        with xml_com_regra(p_cred=Decimal("2.5")):
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
        self.assertEqual(ap.itens[0].cfop, "5102")

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
        self.assertTrue(any("regra Validada" in b or "estabelecimento" in b.lower() for b in ap.bloqueios))

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
        with xml_com_regra():
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
        with xml_com_regra():
            xml = _montar_xml_nfe_saida(
                doc, cfg, serie=1, numero=4, chave="3" * 43 + "5", dh_emi=datetime(2026, 10, 1, 10, 0, 0)
            )
        self.assertIn("<CSOSN>103</CSOSN>", xml)
        self.assertIn("<ICMSSN102>", xml)
        ok, det = validar_xml_nfe_xsd(xml)
        self.assertEqual(det, "ok")
        self.assertTrue(ok, det)


class NfeSaidaClienteFiscalPersistTests(TestCase):
    def test_fiscal_nfe_salvo_sem_alterar_endereco_pdv(self):
        from produtos.nfe_saida_util import cliente_agro_para_destinatario, salvar_fiscal_cliente_agro

        c = ClienteAgro.objects.create(
            nome="Nome PDV",
            cpf="39053344705",
            logradouro="Rua Entrega PDV",
            cidade="Jacupiranga",
            uf="SP",
            cep="11940000",
        )
        salvar_fiscal_cliente_agro(
            c.pk,
            {
                "nome": "Razão Fiscal NF",
                "documento": "39053344705",
                "logradouro": "Av Fiscal 55",
                "numero": "100",
                "cidade": "Registro",
                "uf": "SP",
                "cep": "11900000",
                "bairro": "Centro",
                "ie": "ISENTO",
            },
        )
        c.refresh_from_db()
        self.assertEqual(c.logradouro, "Rua Entrega PDV")
        self.assertEqual(c.cidade, "Jacupiranga")
        dest = cliente_agro_para_destinatario(c)
        self.assertEqual(dest["logradouro"], "Av Fiscal 55")
        self.assertEqual(dest["nome"], "Razão Fiscal NF")
        self.assertEqual(dest["ie"], "ISENTO")


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
        self.assertEqual(itens[0]["cfop"], "")
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

    def test_overlay_ncm_sobre_modelo_legado(self):
        from unittest.mock import MagicMock, patch

        from produtos.nfe_saida_util import _fiscal_produto_buscar

        with patch(
            "produtos.nfce_fiscal_produto_util.fiscal_por_produto_id",
            return_value={"ncm": "12345678", "cfop": "5102", "csosn": "102", "origem": "0"},
        ):
            with patch("produtos.catalogo_agro.obter_produto_model") as obter:
                p = MagicMock()
                p.ncm = "23099020"
                obter.return_value = p
                fis = _fiscal_produto_buscar("99")
        self.assertEqual(fis["ncm"], "12345678")

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
        with xml_com_regra():
            xml = _montar_xml_nfe_saida(
                doc, cfg, serie=1, numero=5, chave="3" * 43 + "5", dh_emi=datetime(2026, 10, 2, 12, 0, 0)
            )
        ok, det = validar_xml_nfe_xsd(xml)
        self.assertTrue(ok, det)
        self.assertIn("<NCM>23099020</NCM>", xml)


class NfeSaidaXsdValidationTests(SimpleTestCase):
    def test_pacote_pl010f_presente(self):
        from pathlib import Path

        from django.conf import settings

        from produtos.nfe_saida_xml_validate_util import xsd_nfe_disponivel, xsd_nfe_v4_path

        self.assertTrue(xsd_nfe_disponivel())
        p = xsd_nfe_v4_path()
        self.assertIsNotNone(p)
        self.assertIn("PL_010f", str(p))
        ver = Path(settings.BASE_DIR) / "produtos/schemas/nfe/PL_010f/VERSION.txt"
        self.assertTrue(ver.is_file())

    def test_xml_invalido_rejeitado_xsd(self):
        from produtos.nfe_saida_xml_validate_util import validar_xml_nfe_xsd

        ok, det = validar_xml_nfe_xsd("<NFe xmlns='http://www.portalfiscal.inf.br/nfe'><x/></NFe>")
        self.assertFalse(ok)
        self.assertTrue(det.startswith("xsd_invalid:") or det.startswith("xsd_error:"))

    def test_xml_montado_valida_pl010f(self):
        from datetime import datetime

        from produtos.nfe_saida_sp_emissao_util import _montar_xml_nfe_saida
        from produtos.nfe_saida_util import doc_novo_vazio
        from produtos.nfe_saida_xml_validate_util import _xml_para_validacao_xsd, validar_xml_nfe_xsd

        doc = doc_novo_vazio()
        doc["destinatario"].update(
            {
                "nome": "C",
                "documento": "39053344705",
                "uf": "SP",
                "cidade": "Ja",
                "logradouro": "Ru",
                "numero": "1",
                "bairro": "Ce",
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
                "csosn": "102",
                "cst_pis_cofins": "49",
            }
        ]
        doc["pagamento"] = [{"tPag": "01", "valor": 10}]
        cfg = {
            "tp_amb": 2,
            "cnpj": "11222333000181",
            "razao_social": "Em",
            "fantasia": "Em",
            "logradouro": "Ru",
            "numero": "1",
            "bairro": "Ce",
            "cmun": "3524600",
            "cidade": "Ja",
            "uf": "SP",
            "cep": "11940000",
            "ie": "123456789012",
            "fone": "",
        }
        with xml_com_regra():
            xml = _montar_xml_nfe_saida(
                doc, cfg, serie=1, numero=8, chave="3" * 43 + "5", dh_emi=datetime(2026, 8, 31, 10, 0, 0)
            )
        ok, det = validar_xml_nfe_xsd(xml)
        self.assertTrue(ok, det)
        assinado = _xml_para_validacao_xsd(xml).decode("utf-8")
        ok2, det2 = validar_xml_nfe_xsd(assinado)
        self.assertTrue(ok2, det2)


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
                "produtos.nfe_saida_tributacao_util.erros_fiscal_emissao",
                return_value=[],
            ):
                with patch(
                    "produtos.nfe_saida_regras_validas_util.resolver_cnpj_serie_emitente",
                    return_value=("11222333000181", 1, None),
                ):
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
            with patch("produtos.nfe_saida_tributacao_util.erros_fiscal_emissao", return_value=[]):
                with patch(
                    "produtos.nfe_saida_regras_validas_util.resolver_cnpj_serie_emitente",
                    return_value=("11222333000181", 1, None),
                ):
                    out = emitir_nfe_saida_documento(doc, cnpj_emit="11222333000181")
        self.assertTrue(out.get("simulado"))
        self.assertFalse(out.get("autorizacao_sefaz_real", True))
        os.environ.pop("NFE_SAIDA_SIMULAR_EMIT", None)


def _doc_checklist(*, uf_dest="SP", cfop="5102", estab="centro"):
    from produtos.nfe_saida_util import doc_novo_vazio

    doc = doc_novo_vazio()
    doc["estabelecimento"] = estab
    doc["destinatario"].update(
        {
            "nome": "Cliente",
            "documento": "39053344705",
            "uf": uf_dest,
            "cidade": "Jacupiranga",
            "logradouro": "Rua A",
            "cmun": "3524600",
        }
    )
    doc["itens"] = [
        {
            "descricao": "Item",
            "qtd": 1,
            "v_unit": 10,
            "v_total": 10,
            "ncm": "12345678",
            "cfop": cfop,
            "csosn": "102",
        }
    ]
    doc["pagamento"] = [{"forma": "Dinheiro", "tPag": "01", "valor": 10}]
    return doc


class NfeSaidaChecklistEmitenteTests(SimpleTestCase):
    def _cfop_row(self, rows):
        return next(r for r in rows if r["label"] == "CFOP coerente com UF")

    def test_avulsa_sem_estabelecimento(self):
        from produtos.nfe_saida_util import checklist_conferencia, doc_novo_vazio

        doc = doc_novo_vazio()
        self.assertEqual(doc.get("estabelecimento"), "")
        rows = checklist_conferencia(doc, cert_ok=True)
        est = next(r for r in rows if r["label"] == "Estabelecimento emitente")
        self.assertFalse(est["ok"])

    def test_sp_cfop_5102_ok(self):
        from produtos.nfe_saida_util import checklist_conferencia

        rows = checklist_conferencia(_doc_checklist(uf_dest="sp", cfop="5102"), cert_ok=True)
        row = self._cfop_row(rows)
        self.assertTrue(row["ok"], row)
        self.assertIn("SP", row["det"])
        self.assertIn("interna", row["det"])

    def test_interestadual_5102_explica_ufs(self):
        from produtos.nfe_saida_util import checklist_conferencia

        rows = checklist_conferencia(_doc_checklist(uf_dest="RJ", cfop="5102"), cert_ok=True)
        row = self._cfop_row(rows)
        self.assertFalse(row["ok"])
        self.assertIn("5102", row["det"])
        self.assertIn("RJ", row["det"])
        self.assertIn("interestadual", row["det"].lower())
        self.assertIn("Não convertemos", row["det"])

    def test_doc_novo_nao_presume_centro(self):
        from produtos.nfe_saida_util import aplicar_estabelecimento_no_doc, doc_novo_vazio

        doc = doc_novo_vazio()
        aplicar_estabelecimento_no_doc(doc)
        self.assertEqual(doc["estabelecimento"], "")

    def test_emit_bloqueia_cfop_interestadual(self):
        from produtos.nfe_saida_util import erros_cfop_coerente_uf

        erros = erros_cfop_coerente_uf(_doc_checklist(uf_dest="RJ", cfop="5102"))
        self.assertTrue(erros)
        self.assertIn("5102", erros[0])
        self.assertIn("RJ", erros[0])
        self.assertFalse(erros_cfop_coerente_uf(_doc_checklist(uf_dest="SP", cfop="5102")))

    def test_sugerido_venda_so_deposito_claro(self):
        from produtos.nfe_saida_util import estabelecimento_sugerido_venda

        v = type("V", (), {"deposito": "vila"})()
        self.assertEqual(estabelecimento_sugerido_venda(v), "vila")
        v2 = type("V", (), {"deposito": ""})()
        self.assertIsNone(estabelecimento_sugerido_venda(v2))
        v3 = type("V", (), {"deposito": "loja3"})()
        self.assertIsNone(estabelecimento_sugerido_venda(v3))

    def test_vila_nao_herda_serie_centro(self):
        from produtos.nfe_saida_util import aplicar_estabelecimento_no_doc, doc_novo_vazio, nfe_saida_serie_default

        doc = doc_novo_vazio()
        doc["serie"] = nfe_saida_serie_default()
        doc["fiscal"]["serie"] = nfe_saida_serie_default()
        doc["estabelecimento"] = "vila"
        aplicar_estabelecimento_no_doc(doc)
        self.assertEqual(doc["estabelecimento"], "vila")
        self.assertNotEqual(doc.get("serie"), nfe_saida_serie_default())


class NfeSaidaJsStaticTests(SimpleTestCase):
    def test_js_sem_codigos_inventados_e_emit_exige_save(self):
        js = Path("produtos/static/produtos/js/nfe_saida.js").read_text(encoding="utf-8")
        self.assertNotIn("23099020", js)
        self.assertNotIn("p.cfop || '5102'", js)
        self.assertNotIn("p.csosn || p.CSOSN || '102'", js)
        self.assertIn("j.ok !== true", js)
        self.assertIn("transmissão não foi iniciada", js)
        self.assertIn("function setEstabelecimento", js)
        self.assertIn("function parseBrNum", js)
        self.assertIn("function moneyQtdVu", js)
        self.assertIn('data-it="un"', js)


class NfeSaidaQtdDecimalTests(SimpleTestCase):
    def test_virgula_e_ponto(self):
        from produtos.nfe_saida_util import decimal_qtd, validar_itens

        self.assertEqual(decimal_qtd("0,645"), Decimal("0.6450"))
        self.assertEqual(decimal_qtd("0.645"), Decimal("0.6450"))
        self.assertEqual(decimal_qtd("4,355"), Decimal("4.3550"))
        self.assertEqual(decimal_qtd("4.355"), Decimal("4.3550"))
        erros = validar_itens(
            [
                {
                    "descricao": "Calcário",
                    "qtd": "0,645",
                    "v_unit": "2870",
                    "v_total": "1851,15",
                    "ncm": "23099020",
                    "un": "TON",
                }
            ]
        )
        self.assertFalse(any("quantidade" in e.lower() for e in erros), erros)

    def test_prefeitura_totais(self):
        from produtos.nfe_saida_tributacao_util import _dec, _dec_qtd
        from produtos.nfe_saida_util import decimal_moeda, decimal_qtd, normalizar_linha_item_nfe

        q1 = decimal_qtd("4,355")
        q2 = decimal_qtd("0,645")
        vu = decimal_moeda("2870")
        self.assertEqual((q1 * vu).quantize(Decimal("0.01")), Decimal("12498.85"))
        self.assertEqual((q2 * vu).quantize(Decimal("0.01")), Decimal("1851.15"))
        self.assertEqual(_dec_qtd("4,355"), Decimal("4.3550"))
        self.assertNotEqual(_dec_qtd("4,355"), Decimal("4.36"))
        self.assertEqual(_dec("2870"), Decimal("2870.00"))
        ln = normalizar_linha_item_nfe(
            {"descricao": "X", "qtd": "4,355", "v_unit": "2.870,00", "v_total": 0, "un": "tonelada"}
        )
        self.assertEqual(ln["qtd"], "4.355")
        self.assertEqual(ln["un"], "TON")
        self.assertEqual(ln["v_total"], 12498.85)

    def test_mais_de_4_casas_e_inteiro(self):
        from produtos.nfe_saida_util import validar_itens

        er = validar_itens(
            [{"descricao": "X", "qtd": "0,64555", "v_unit": 1, "v_total": 1, "ncm": "23099020"}]
        )
        self.assertTrue(any("4 casas" in e for e in er), er)
        er2 = validar_itens(
            [{"descricao": "Saco", "qtd": 2, "v_unit": 10, "v_total": 20, "ncm": "23099020", "un": "UN"}]
        )
        self.assertFalse(any("quantidade" in e.lower() for e in er2), er2)

    def test_xml_qcom_ton_e_xsd(self):
        from datetime import datetime

        from produtos.nfe_saida_danfe_html import _linha_produto
        from produtos.nfe_saida_sp_emissao_util import _montar_xml_nfe_saida
        from produtos.nfe_saida_util import doc_novo_vazio
        from produtos.nfe_saida_xml_validate_util import validar_xml_nfe_xsd

        def _doc(qtd, total):
            d = doc_novo_vazio()
            d["estabelecimento"] = "centro"
            d["destinatario"].update(
                {
                    "nome": "PREFEITURA MUNICIPAL DE JACUPIRANGA",
                    "documento": "53221728000100",
                    "uf": "SP",
                    "cidade": "Jacupiranga",
                    "logradouro": "Rua A",
                    "numero": "1",
                    "bairro": "Centro",
                    "cep": "11940000",
                    "cmun": "3524600",
                }
            )
            d["itens"] = [
                {
                    "descricao": "Produto TON",
                    "qtd": qtd,
                    "v_unit": "2870",
                    "v_total": total,
                    "ncm": "23099020",
                    "cfop": "5102",
                    "csosn": "102",
                    "cst_pis_cofins": "49",
                    "un": "TON",
                }
            ]
            d["pagamento"] = [{"tPag": "01", "valor": total}]
            return d

        cfg = {
            "tp_amb": 2,
            "cnpj": "11222333000181",
            "razao_social": "Em",
            "fantasia": "Em",
            "logradouro": "Ru",
            "numero": "1",
            "bairro": "Ce",
            "cmun": "3524600",
            "cidade": "Ja",
            "uf": "SP",
            "cep": "11940000",
            "ie": "123456789012",
            "fone": "",
        }
        casos = (("4,355", "12498.85", "4.3550", "12498.85"), ("0,645", "1851.15", "0.6450", "1851.15"))
        for qtd, total, qcom, vprod in casos:
            with xml_com_regra():
                xml = _montar_xml_nfe_saida(
                    _doc(qtd, total),
                    cfg,
                    serie=1,
                    numero=8,
                    chave="3" * 43 + "5",
                    dh_emi=datetime(2026, 8, 31, 10, 0, 0),
                )
            self.assertIn(f"<qCom>{qcom}</qCom>", xml)
            self.assertIn(f"<qTrib>{qcom}</qTrib>", xml)
            self.assertIn("<uCom>TON</uCom>", xml)
            self.assertIn("<uTrib>TON</uTrib>", xml)
            self.assertIn(f"<vProd>{vprod}</vProd>", xml)
            ok, det = validar_xml_nfe_xsd(xml)
            self.assertTrue(ok, det)
            html = _linha_produto(_doc(qtd, total)["itens"][0])
            self.assertIn("TON", html)
            self.assertIn(qtd.replace(".", ","), html)
