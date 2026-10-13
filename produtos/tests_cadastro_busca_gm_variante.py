"""Busca cadastro — GM com hífen (ex. GM0037-1) não deve casar barras «00371» de outro produto."""
from django.test import SimpleTestCase

from produtos.cadastro_busca_codigo_util import (
    digitos_somente_barras_cadastro,
    q_codigo_exato_cadastro,
    termo_bate_codigos_produto,
    termo_gm_etiqueta_completa,
)


class CadastroBuscaGmVarianteTests(SimpleTestCase):
    def test_digitos_gm_nao_vira_barras_soltos(self):
        self.assertIsNone(digitos_somente_barras_cadastro("GM0037-1"))
        self.assertEqual(digitos_somente_barras_cadastro("7891234567890"), "7891234567890")

    def test_etiqueta_completa(self):
        self.assertTrue(termo_gm_etiqueta_completa("GM0037-1"))
        self.assertFalse(termo_gm_etiqueta_completa("GM0037"))

    def test_q_codigo_exato_nao_busca_barras_so_digitos(self):
        q = q_codigo_exato_cadastro("GM0037-1")
        self.assertIsNotNone(q)
        sql = str(q)
        self.assertNotIn("codigo_barras", sql)

    def test_gm0037_1_nao_bate_gm1097_1(self):
        self.assertFalse(
            termo_bate_codigos_produto(
                "GM0037-1",
                codigo_interno=None,
                codigo_nfe="GM1097-1",
                codigo_barras="00371",
            )
        )

    def test_gm0037_1_bate_exato(self):
        self.assertTrue(
            termo_bate_codigos_produto(
                "GM0037-1",
                codigo_interno=None,
                codigo_nfe="GM0037-1",
                codigo_barras=None,
            )
        )

    def test_gm0037_1_nao_bate_gm0037_10(self):
        self.assertFalse(
            termo_bate_codigos_produto(
                "GM0037-1",
                codigo_interno=None,
                codigo_nfe="GM0037-10",
                codigo_barras=None,
            )
        )
