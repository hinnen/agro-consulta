"""Testes da campanha inauguração Vila (5% automático)."""

from datetime import date
from decimal import Decimal
from unittest import mock

from django.test import SimpleTestCase, override_settings

from produtos.campanha_pdv_util import (
    CAMPANHA_ID,
    aplicar_desconto_campanha_nos_itens,
    bootstrap_campanha,
    campanha_ativa_para_deposito,
    campanha_no_calendario,
)


class CampanhaInauguracaoVilaTests(SimpleTestCase):
    def test_ativa_vila_no_dia(self):
        camp = campanha_ativa_para_deposito("vila", agora=date(2026, 8, 8))
        self.assertIsNotNone(camp)
        self.assertEqual(camp["id"], CAMPANHA_ID)
        self.assertEqual(camp["percentual"], 5.0)
        self.assertAlmostEqual(camp["fator"], 0.95)

    def test_centro_nao_aplica(self):
        self.assertIsNone(campanha_ativa_para_deposito("centro", agora=date(2026, 8, 8)))

    def test_fora_da_data_nao_aplica(self):
        self.assertIsNone(campanha_ativa_para_deposito("vila", agora=date(2026, 8, 9)))
        self.assertIsNone(campanha_ativa_para_deposito("vila", agora=date(2026, 8, 7)))

    @override_settings(AGRO_CAMPANHA_INAUGURACAO_OFF="1")
    def test_kill_switch(self):
        self.assertIsNone(campanha_ativa_para_deposito("vila", agora=date(2026, 8, 8)))

    @override_settings(AGRO_CAMPANHA_INAUGURACAO_TEST="1")
    def test_modo_teste_fora_da_data(self):
        camp = campanha_ativa_para_deposito("vila", agora=date(2026, 8, 1))
        self.assertIsNotNone(camp)
        self.assertTrue(camp.get("teste"))

    def test_aplica_preco_itens(self):
        itens = [{"id": "1", "nome": "X", "qtd": 2, "preco": 100}]
        out, camp = aplicar_desconto_campanha_nos_itens(itens, "vila", agora=date(2026, 8, 8))
        self.assertIsNotNone(camp)
        self.assertEqual(out[0]["preco"], 95.0)
        self.assertEqual(out[0]["campanha_id"], CAMPANHA_ID)
        # original intacto
        self.assertEqual(itens[0]["preco"], 100)

    def test_bootstrap_centro_regra_visivel_mas_inativa(self):
        with mock.patch(
            "produtos.campanha_pdv_util.campanha_no_calendario",
            return_value={
                "id": CAMPANHA_ID,
                "deposito": "vila",
                "percentual": 5.0,
                "fator": 0.95,
                "rotulo": "x",
            },
        ):
            boot = bootstrap_campanha("centro")
        self.assertFalse(boot["ativa"])
        self.assertIsNotNone(boot["regra"])

    def test_bootstrap_vila_ativa(self):
        boot = bootstrap_campanha("vila")
        # depende da data real — força calendário
        with mock.patch(
            "produtos.campanha_pdv_util.campanha_no_calendario",
            return_value={
                "id": CAMPANHA_ID,
                "deposito": "vila",
                "percentual": 5.0,
                "fator": 0.95,
                "rotulo": "x",
            },
        ):
            boot = bootstrap_campanha("vila")
        self.assertTrue(boot["ativa"])
        self.assertEqual(boot["regra"]["id"], CAMPANHA_ID)

    def test_fator_decimal_preciso(self):
        itens = [{"id": "1", "preco": Decimal("33.33")}]
        out, _ = aplicar_desconto_campanha_nos_itens(itens, "vila", agora=date(2026, 8, 8))
        self.assertAlmostEqual(out[0]["preco"], 31.6635, places=4)
