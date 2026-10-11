"""Allowlist Vendas/Tarefas — código # e nomes da gerência."""

from unittest.mock import MagicMock, patch

from django.test import TestCase, override_settings

from produtos.vendas_lojas_hub_acesso_util import (
    _norm_nome,
    nomes_vendas_tarefas_hub,
    operador_liberado_vendas_tarefas,
    pin_liberado_vendas_tarefas,
)


class VendasLojasHubAcessoTests(TestCase):
    def test_nomes_padrao_gerencia(self):
        nomes = nomes_vendas_tarefas_hub()
        self.assertIn(_norm_nome("Renan Hinnen"), nomes)
        self.assertIn(_norm_nome("Geraldinho"), nomes)
        self.assertIn(_norm_nome("Zuleide Hinnen"), nomes)

    @override_settings(AGRO_VL_HUB_PINS_VENDAS_TAREFAS="1,2,3,4,8")
    @patch("produtos.vendas_lojas_hub_acesso_util._perfil_por_pin")
    @patch("base.operador_util.nome_exibicao_perfil", return_value="Renan Hinnen")
    def test_pin_pessoal_renan_por_codigo(self, _nome, mock_perfil):
        mock_perfil.return_value = MagicMock(codigo_vendedor="1")
        self.assertTrue(pin_liberado_vendas_tarefas("9973"))

    @override_settings(AGRO_VL_HUB_PINS_VENDAS_TAREFAS="99")
    @patch("produtos.vendas_lojas_hub_acesso_util._perfil_por_pin")
    @patch("base.operador_util.nome_exibicao_perfil", return_value="Renan Hinnen")
    def test_pin_pessoal_renan_por_nome(self, _nome, mock_perfil):
        mock_perfil.return_value = MagicMock(codigo_vendedor="9999")
        self.assertTrue(operador_liberado_vendas_tarefas("9973"))

    @override_settings(AGRO_VL_HUB_PINS_VENDAS_TAREFAS="1,2,3,4,8")
    @patch("produtos.vendas_lojas_hub_acesso_util._perfil_por_pin")
    @patch("base.operador_util.nome_exibicao_perfil", return_value="Nathan Hinnen")
    def test_fora_da_lista(self, _nome, mock_perfil):
        mock_perfil.return_value = MagicMock(codigo_vendedor="6")
        self.assertFalse(pin_liberado_vendas_tarefas("5555"))
