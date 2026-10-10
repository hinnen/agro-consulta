"""Busca 230… não cruza cadastro legado com EAN válido de outro produto."""
from django.test import SimpleTestCase

from produtos.cadastro_busca_codigo_util import (
    cb_loja_bip_equivalente,
    index_codigos_de_campos,
    termo_bate_codigos_produto,
)


class CbLojaBipBuscaTests(SimpleTestCase):
    def test_1479_e_1471_nao_sao_equivalentes(self):
        self.assertFalse(cb_loja_bip_equivalente("2300000001471", "2300000001479"))
        self.assertFalse(cb_loja_bip_equivalente("2300000001479", "2300000001471"))

    def test_bip_valido_nao_bate_cadastro_legado(self):
        self.assertFalse(
            termo_bate_codigos_produto(
                "2300000001471",
                codigo_barras="2300000001479",
            )
        )

    def test_index_nao_inclui_canonicalizacao_ambigua(self):
        ix = index_codigos_de_campos(codigo_barras="2300000001479")
        self.assertIn("2300000001479", ix)
        self.assertNotIn("2300000001471", ix)
