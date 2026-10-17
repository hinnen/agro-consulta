"""NF-e saída 55 — validação e defaults (sem Mongo/SEFAZ)."""
from __future__ import annotations

from decimal import Decimal

from django.test import SimpleTestCase, TestCase


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


class NfeSaidaPagamentoTests(SimpleTestCase):
    def test_validar_pagamentos_total(self):
        from produtos.nfe_saida_util import validar_pagamentos

        total = Decimal("100.00")
        self.assertEqual(validar_pagamentos([], total), ["Informe forma de pagamento (etapa 4)."])
        ok = [{"forma": "Dinheiro", "valor": 100}]
        self.assertEqual(validar_pagamentos(ok, total), [])
        bad = [{"forma": "Dinheiro", "valor": 50}]
        self.assertTrue(validar_pagamentos(bad, total))
