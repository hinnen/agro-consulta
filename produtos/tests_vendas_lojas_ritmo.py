"""Ritmo «até agora» — histórico PDV por hora (sem banco)."""
from datetime import date, datetime
from decimal import Decimal
from unittest.mock import patch

from django.test import SimpleTestCase
from django.utils import timezone

from produtos import vendas_lojas_ritmo_util as ritmo
from produtos.vendas_lojas_util import vendas_lojas_meta_c_modos_de_serie


class VendasLojasRitmoTests(SimpleTestCase):
    def test_hora_referencia_usa_hora_local(self):
        tz = timezone.get_current_timezone()
        agora = timezone.make_aware(datetime(2026, 10, 11, 15, 37), tz)
        self.assertEqual(ritmo.vendas_lojas_hora_referencia(agora), 15)
        self.assertEqual(
            ritmo.vendas_lojas_hora_referencia_label(agora), "Até 15h"
        )

    def test_fracao_domingo_tarde_pede_mais_que_expediente_curto(self):
        # Dois domingos: quase tudo até 13h; às 15h o acumulado histórico ~100 %.
        dom1 = date(2026, 9, 27)
        dom2 = date(2026, 10, 4)
        hoje = date(2026, 10, 11)
        vendas = [
            {
                "quando": datetime(2026, 9, 27, 10, 0),
                "deposito": "centro",
                "total": 800,
            },
            {
                "quando": datetime(2026, 9, 27, 12, 30),
                "deposito": "centro",
                "total": 200,
            },
            {
                "quando": datetime(2026, 10, 4, 9, 0),
                "deposito": "centro",
                "total": 900,
            },
            {
                "quando": datetime(2026, 10, 4, 12, 0),
                "deposito": "centro",
                "total": 100,
            },
        ]

        def fake_carregar(d0, d1):
            return vendas

        with patch(
            "produtos.relatorios_hora_util.carregar_vendas_intervalo",
            side_effect=fake_carregar,
        ):
            with patch(
                "produtos.vendas_lojas_ritmo_util._dias_mesmo_weekday_historico",
                return_value=[dom1, dom2],
            ):
                tz = timezone.get_current_timezone()
                agora = timezone.make_aware(datetime(2026, 10, 11, 15, 30), tz)
                ritmo.cache.clear()
                frac_hist = ritmo.vendas_lojas_fracao_meta_ate_agora(
                    hoje=hoje, agora=agora, deposito="centro"
                )
        from produtos.vendas_lojas_util import vendas_lojas_fracao_expediente

        frac_exp = vendas_lojas_fracao_expediente(agora)
        self.assertGreater(frac_hist, frac_exp)
        self.assertGreaterEqual(frac_hist, Decimal("0.95"))

    def test_modos_de_serie_ate_agora_sobe_com_fracao_maior(self):
        serie = [Decimal("1000.00"), Decimal("1000.00")]
        tz = timezone.get_current_timezone()
        agora = timezone.make_aware(datetime(2026, 10, 2, 15, 0), tz)
        hoje = date(2026, 10, 2)
        with patch(
            "produtos.vendas_lojas_ritmo_util.vendas_lojas_fracao_meta_ate_agora",
            return_value=Decimal("0.50"),
        ):
            dia, ate, toggle = vendas_lojas_meta_c_modos_de_serie(
                serie,
                date(2026, 10, 1),
                date(2026, 10, 2),
                hoje=hoje,
                agora=agora,
                deposito="centro",
            )
        self.assertEqual(dia, Decimal("2000.00"))
        self.assertEqual(ate, Decimal("1500.00"))
        self.assertTrue(toggle)
