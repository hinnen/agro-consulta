"""Aba 10 registra mudança do preço de venda (nota e cadastro)."""
from decimal import Decimal

from django.test import TestCase

from produtos.cadastro_alteracao_historico_util import (
    aplicar_preco_venda_com_historico,
    moeda_br_para_decimal,
)
from produtos.models import Produto, ProdutoCadastroAlteracaoAgro, ProdutoGestaoOverlayAgro


class HistPrecoVendaTests(TestCase):
    def setUp(self):
        self.pid = "pid-preco-hist"
        Produto.objects.create(
            produto_externo_id=self.pid,
            codigo_interno="4252",
            nome="Pomada teste",
            unidade="UN",
            preco_venda=Decimal("23.80"),
        )
        self.ov = ProdutoGestaoOverlayAgro.objects.create(
            produto_externo_id=self.pid,
            preco_venda=Decimal("23.80"),
        )

    def test_milhar_br(self):
        self.assertEqual(moeda_br_para_decimal("1.234,56"), Decimal("1234.56"))
        self.assertEqual(moeda_br_para_decimal("23,80"), Decimal("23.80"))

    def test_nota_grava_preco_na_aba_alteracoes(self):
        n = aplicar_preco_venda_com_historico(
            self.ov,
            produto_id=self.pid,
            preco_novo=Decimal("24.00"),
            origem="nf",
        )
        self.assertEqual(n, 1)
        row = ProdutoCadastroAlteracaoAgro.objects.get(produto_externo_id=self.pid)
        self.assertEqual(row.campo, "preco_venda")
        self.assertEqual(row.valor_antes, "23.8")
        self.assertEqual(row.valor_depois, "24")
        self.assertEqual(row.origem, "nf")
        self.ov.refresh_from_db()
        self.assertEqual(self.ov.preco_venda, Decimal("24.00"))

    def test_mesmo_valor_nao_gera_linha(self):
        n = aplicar_preco_venda_com_historico(
            self.ov,
            produto_id=self.pid,
            preco_novo=Decimal("23.80"),
            origem="nf",
        )
        self.assertEqual(n, 0)
        self.assertFalse(ProdutoCadastroAlteracaoAgro.objects.exists())
