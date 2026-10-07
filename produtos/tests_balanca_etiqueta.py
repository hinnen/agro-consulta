"""Etiqueta de balança EAN-13 (4 dígitos + preço total)."""
from decimal import Decimal

from django.test import SimpleTestCase

from produtos.views import _ean13_digito_verificador, _parse_etiqueta_balanca_ean13_br


class EtiquetaBalancaEan13Tests(SimpleTestCase):
    def test_parse_plu_0010_preco_4_81(self):
        r = _parse_etiqueta_balanca_ean13_br("2001000004812")
        self.assertIsNotNone(r)
        cod4, preco = r
        self.assertEqual(cod4, "0010")
        self.assertEqual(preco, Decimal("4.81"))

    def test_dv_invalido_rejeita(self):
        self.assertIsNone(_parse_etiqueta_balanca_ean13_br("2001000004810"))

    def test_dv_esperado(self):
        self.assertEqual(_ean13_digito_verificador("200100000481"), 2)
