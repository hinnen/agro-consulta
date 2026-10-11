"""Allowlist Vendas/Tarefas — slot codigo_vendedor vs senha_rapida."""

from unittest.mock import patch

from django.test import TestCase, override_settings

from produtos.vendas_lojas_hub_acesso_util import pin_liberado_vendas_tarefas


class VendasLojasHubAcessoTests(TestCase):
    @override_settings(AGRO_VL_HUB_PINS_VENDAS_TAREFAS="1,2,3,4,8")
    def test_pin_curto_na_lista(self):
        self.assertTrue(pin_liberado_vendas_tarefas("1"))
        self.assertFalse(pin_liberado_vendas_tarefas("5"))

    @override_settings(AGRO_VL_HUB_PINS_VENDAS_TAREFAS="1,2,3,4,8")
    @patch(
        "produtos.vendas_lojas_hub_acesso_util._slot_operador_por_senha_rapida",
        return_value="1",
    )
    def test_senha_longa_liberada_por_slot(self, _slot):
        self.assertTrue(pin_liberado_vendas_tarefas("9973"))

    @override_settings(AGRO_VL_HUB_PINS_VENDAS_TAREFAS="1,2,3,4,8")
    @patch(
        "produtos.vendas_lojas_hub_acesso_util._slot_operador_por_senha_rapida",
        return_value="12",
    )
    def test_slot_fora_da_lista(self, _slot):
        self.assertFalse(pin_liberado_vendas_tarefas("5555"))
