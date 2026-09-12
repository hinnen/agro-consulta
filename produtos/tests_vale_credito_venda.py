"""Baixa de vale crédito ao pagar a venda (bug loja #16)."""
from __future__ import annotations

from decimal import Decimal

from django.test import SimpleTestCase, TestCase

from produtos.models import ClienteAgro, ClienteAgroEventoAgro
from produtos.vale_credito_venda_util import (
    aplicar_movimento_vale_credito_venda,
    valor_vale_credito_usado_no_payload,
    validar_vale_credito_payload,
)


class ValePayloadTests(SimpleTestCase):
    def test_soma_forma_vale(self):
        data = {
            "pagamentos": [
                {"formaPagamento": "Dinheiro", "valorPagamento": 10},
                {"formaPagamento": "Vale crédito", "valorPagamento": "15,50"},
            ]
        }
        self.assertEqual(valor_vale_credito_usado_no_payload(data), Decimal("15.50"))

    def test_compra_vale_nao_baixa(self):
        data = {
            "compra_vale_credito": True,
            "pagamentos": [{"formaPagamento": "Vale crédito", "valorPagamento": 20}],
            "itens": [{"id": "vale-credito", "qtd": 1, "preco": 20}],
        }
        self.assertEqual(valor_vale_credito_usado_no_payload(data), Decimal("0"))


class ValeDebitoTests(TestCase):
    def test_baixa_saldo(self):
        cli = ClienteAgro.objects.create(
            nome="CLI VALE #16",
            saldo_vale_credito=Decimal("40.00"),
        )
        data = {
            "cliente_agro_pk": cli.pk,
            "pagamentos": [{"forma": "Vale crédito", "valor": 12.5}],
        }
        ok, msg, _ = validar_vale_credito_payload(data, cliente_agro=cli)
        self.assertTrue(ok, msg)
        out = aplicar_movimento_vale_credito_venda(data, cliente_agro=cli, venda_pk=1)
        self.assertTrue(out["aplicado"])
        cli.refresh_from_db()
        self.assertEqual(cli.saldo_vale_credito, Decimal("27.50"))
        self.assertTrue(
            ClienteAgroEventoAgro.objects.filter(
                cliente_agro=cli, tipo=ClienteAgroEventoAgro.Tipo.VALE_USADO
            ).exists()
        )

    def test_acima_do_saldo(self):
        cli = ClienteAgro.objects.create(
            nome="CLI VALE CHEIO",
            saldo_vale_credito=Decimal("5.00"),
        )
        data = {
            "cliente_agro_pk": cli.pk,
            "pagamentos": [{"forma": "Vale crédito", "valor": 9}],
        }
        ok, msg, _ = validar_vale_credito_payload(data, cliente_agro=cli)
        self.assertFalse(ok)
        self.assertIn("acima", msg.lower())
