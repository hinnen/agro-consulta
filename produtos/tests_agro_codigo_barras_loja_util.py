"""Código de barras interno 230… — EAN bip vs cadastro legado."""
from unittest.mock import patch

from django.test import SimpleTestCase

from produtos.agro_codigo_barras_loja_util import (
    _seqs_para_max_alocacao,
    alocar_proximo_codigo_barras_loja,
    ean13_checksum_ok,
    ean13_para_bip_codigo_barras_loja,
    formatar_codigo_barras_loja,
    variantes_busca_codigo_barras_loja,
)


class CodigoBarrasLojaEanTests(SimpleTestCase):
    def test_legado_1480_bip_dv_correto(self):
        cadastro = "2300000001480"
        self.assertFalse(ean13_checksum_ok(cadastro))
        bip = ean13_para_bip_codigo_barras_loja(cadastro)
        self.assertEqual(bip, "2300000001488")
        self.assertTrue(ean13_checksum_ok(bip))

    def test_variantes_incluem_cadastro_e_bip(self):
        cadastro = "2300000001480"
        v = variantes_busca_codigo_barras_loja(cadastro)
        self.assertIn(cadastro, v)
        self.assertIn("2300000001488", v)

    def test_bip_valido_acha_legado(self):
        bip = "2300000001488"
        v = variantes_busca_codigo_barras_loja(bip)
        self.assertIn("2300000001480", v)

    @patch("produtos.agro_codigo_barras_loja_util._cb_loja_ocupado_unificado", return_value=False)
    @patch("produtos.agro_codigo_barras_loja_util._max_seq_cb_loja_unificado", return_value=1480)
    def test_alocar_sem_mongo(self, _max, _occ):
        err, cb = alocar_proximo_codigo_barras_loja(None, None)
        self.assertIsNone(err)
        self.assertTrue(ean13_checksum_ok(str(cb or "")))
        self.assertEqual(cb, formatar_codigo_barras_loja(1481))

    def test_novo_formato_ja_valido(self):
        cb = formatar_codigo_barras_loja(1572)
        self.assertTrue(ean13_checksum_ok(cb))
        self.assertEqual(ean13_para_bip_codigo_barras_loja(cb), cb)
        self.assertIn(cb, variantes_busca_codigo_barras_loja(cb))

    def test_ncm_padded_nao_infla_max_seq(self):
        """NCM 23099020 em campo errado (13 dígitos) não deve esgotar faixa 230."""
        self.assertEqual(_seqs_para_max_alocacao("2309902000000"), [])
        self.assertEqual(_seqs_para_max_alocacao("2309902012345"), [])
        self.assertEqual(_seqs_para_max_alocacao("2300000001480"), [1480])

    @patch("produtos.agro_codigo_barras_loja_util._max_seq_cb_loja_postgres", return_value=999_999_998)
    @patch("produtos.agro_codigo_barras_loja_util._cb_loja_ocupado_unificado", return_value=False)
    def test_alocar_apos_max_seq_alto(self, _occ, _max_pg):
        err, cb = alocar_proximo_codigo_barras_loja(None, None)
        self.assertIsNone(err)
        self.assertTrue(ean13_checksum_ok(str(cb or "")))
