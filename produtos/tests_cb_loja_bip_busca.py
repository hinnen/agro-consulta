"""Bip 230… legado ↔ EAN válido na busca."""
from django.test import SimpleTestCase

from produtos.cadastro_busca_codigo_util import (
    cb_loja_bip_equivalente,
    index_codigos_de_campos,
    termo_bate_codigos_produto,
)


class CbLojaBipBuscaTests(SimpleTestCase):
    def test_equivalente_1479_1471(self):
        self.assertTrue(cb_loja_bip_equivalente("2300000001471", "2300000001479"))
        self.assertTrue(cb_loja_bip_equivalente("2300000001479", "2300000001471"))

    def test_termo_bate_cadastro_vs_bip(self):
        self.assertTrue(
            termo_bate_codigos_produto(
                "2300000001471",
                codigo_barras="2300000001479",
            )
        )

    def test_index_inclui_variantes(self):
        ix = index_codigos_de_campos(codigo_barras="2300000001479")
        self.assertIn("2300000001479", ix)
        self.assertIn("2300000001471", ix)
