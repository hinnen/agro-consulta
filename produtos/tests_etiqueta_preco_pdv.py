"""Aviso de etiqueta no PDV quando o preço de venda muda."""
from decimal import Decimal

from django.test import TestCase

from produtos.etiqueta_preco_pendente_util import avisar_mudanca_preco_venda, confirmar, listar_pendentes
from produtos.models import EtiquetaPrecoGondolaAgro


class EtiquetaPrecoPdvTests(TestCase):
    def test_mudanca_avisa_as_duas_lojas_e_ok_e_por_loja(self):
        n = avisar_mudanca_preco_venda(
            produto_id="gm1",
            antes="10.00",
            depois="12.50",
            nome="Racao",
            codigo="GM1",
        )
        self.assertEqual(n, 2)
        centro = listar_pendentes("centro")
        vila = listar_pendentes("vila")
        self.assertEqual(len(centro), 1)
        self.assertEqual(centro[0]["preco_de"], "10,00")
        self.assertEqual(centro[0]["preco_para"], "12,50")
        self.assertEqual(len(vila), 1)
        self.assertTrue(confirmar(centro[0]["id"], "centro"))
        self.assertEqual(listar_pendentes("centro"), [])
        self.assertEqual(len(listar_pendentes("vila")), 1)

    def test_segunda_mudanca_atualiza_o_mesmo_aviso(self):
        avisar_mudanca_preco_venda(produto_id="gm2", antes="5", depois="6", nome="A")
        avisar_mudanca_preco_venda(produto_id="gm2", antes="6", depois="7", nome="A")
        self.assertEqual(EtiquetaPrecoGondolaAgro.objects.filter(produto_externo_id="gm2").count(), 2)
        item = listar_pendentes("centro")[0]
        self.assertEqual(item["preco_de"], "5,00")
        self.assertEqual(item["preco_para"], "7,00")

    def test_mesmo_preco_nao_avisa(self):
        n = avisar_mudanca_preco_venda(produto_id="gm3", antes="4,00", depois="4,00", nome="B")
        self.assertEqual(n, 0)
        self.assertFalse(EtiquetaPrecoGondolaAgro.objects.exists())
