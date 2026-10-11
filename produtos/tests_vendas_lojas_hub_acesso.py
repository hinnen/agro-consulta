"""Allowlist Vendas/Tarefas — slot codigo_vendedor vs senha_rapida."""

from unittest.mock import patch

from django.test import TestCase, override_settings

from produtos.vendas_lojas_hub_acesso_util import pin_liberado_vendas_tarefas


class VendasLojasHubAcessoTests(TestCase):
    @override_settings(AGRO_VL_HUB_PINS_VENDAS_TAREFAS="1,2,3,4,8")
    @patch(
        "produtos.vendas_lojas_hub_acesso_util.codigo_operador_por_pin_digitado",
        return_value="1",
    )
    def test_pin_pessoal_liberado_por_codigo_operador(self, _cod):
        self.assertTrue(pin_liberado_vendas_tarefas("9973"))

    @override_settings(AGRO_VL_HUB_PINS_VENDAS_TAREFAS="1,2,3,4,8")
    @patch(
        "produtos.vendas_lojas_hub_acesso_util.codigo_operador_por_pin_digitado",
        return_value="0001",
    )
    def test_codigo_0001_nao_confunde_com_1(self, _cod):
        self.assertFalse(pin_liberado_vendas_tarefas("1111"))

    @override_settings(AGRO_VL_HUB_PINS_VENDAS_TAREFAS="1,2,3,4,8")
    @patch(
        "produtos.vendas_lojas_hub_acesso_util.codigo_operador_por_pin_digitado",
        return_value="6",
    )
    def test_codigo_fora_da_lista(self, _cod):
        self.assertFalse(pin_liberado_vendas_tarefas("5555"))
