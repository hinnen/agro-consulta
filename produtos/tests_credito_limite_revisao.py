"""Testes puros da revisão de limite (sem ORM)."""
from decimal import Decimal

from django.test import SimpleTestCase

from produtos.credito_limite_revisao import (
    novo_limite_aplicavel,
    normalizar_pct_aumento_max,
    resolver_limite_aprovacao,
    teto_aumento_limite,
)


class CreditoLimiteRevisaoPurezaTests(SimpleTestCase):
    def test_desbloqueio_usa_sugerido_inteiro(self):
        self.assertEqual(novo_limite_aplicavel("0.01", "350.50"), Decimal("350.50"))

    def test_desbloqueio_manual_aceita_acima_do_pct(self):
        self.assertEqual(
            resolver_limite_aprovacao("0.01", "200", "500", pct_max=20),
            Decimal("500.00"),
        )

    def test_desbloqueio_rejeita_001(self):
        self.assertIsNone(resolver_limite_aprovacao("0.01", "200", "0.01", pct_max=20))

    def test_aumento_normal_mantem_teto_pct(self):
        self.assertEqual(novo_limite_aplicavel("100", "200", pct_max=20), Decimal("120.00"))
        self.assertEqual(teto_aumento_limite("100", pct_max=20), Decimal("120.00"))

    def test_pct_normaliza(self):
        self.assertEqual(normalizar_pct_aumento_max("15"), 20)
        self.assertEqual(normalizar_pct_aumento_max("150"), 100)
