from django.test import SimpleTestCase, override_settings

from produtos.vendas_lojas_hub_acesso_util import pin_liberado_vendas_tarefas


class VendasLojasHubAcessoTests(SimpleTestCase):
    @override_settings(AGRO_VL_HUB_PINS_VENDAS_TAREFAS="1,2,3,4,8")
    def test_pins_autorizados(self):
        self.assertTrue(pin_liberado_vendas_tarefas("1"))
        self.assertTrue(pin_liberado_vendas_tarefas("8"))
        self.assertFalse(pin_liberado_vendas_tarefas("5"))
        self.assertFalse(pin_liberado_vendas_tarefas(""))
