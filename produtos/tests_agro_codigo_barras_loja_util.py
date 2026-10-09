"""Código de barras interno 230… — EAN bip vs cadastro legado."""
from django.test import SimpleTestCase

from produtos.agro_codigo_barras_loja_util import (
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

    def test_novo_formato_ja_valido(self):
        cb = formatar_codigo_barras_loja(1572)
        self.assertTrue(ean13_checksum_ok(cb))
        self.assertEqual(ean13_para_bip_codigo_barras_loja(cb), cb)
        self.assertEqual(variantes_busca_codigo_barras_loja(cb), [cb])
