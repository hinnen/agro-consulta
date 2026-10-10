"""--liberar-intruso na migração legado 230."""
from types import SimpleNamespace
from unittest.mock import patch

from django.test import SimpleTestCase

from produtos.cb_loja_legado_migrate_util import (
    migrar_cb_loja_legado_em_produto,
    migrar_cb_loja_legado_lote,
)


class LiberarIntrusoMigrarTests(SimpleTestCase):
    @patch("produtos.models.ProdutoGestaoOverlayAgro.objects")
    @patch("produtos.cb_loja_legado_migrate_util.preparar_codigo_barras_loja_legado")
    @patch("produtos.cb_loja_legado_migrate_util._liberar_intruso_grupo_bip")
    def test_liberar_intruso_destrava_preparar(self, liberar, preparar, ov_mgr):
        ov_mgr.filter.return_value.first.return_value = None
        liberar.return_value = {
            "intruso_id": "outro",
            "intruso_rotulo": "GM9999",
            "reatribuir": {"codigo_novo": "2300000001999"},
        }
        preparar.side_effect = [
            ("2300000001558", None, "ocupado"),
            ("2300000001556", "2300000001558", None),
        ]
        p = SimpleNamespace(
            pk=1,
            produto_externo_id="gm0024",
            codigo_barras="2300000001558",
        )

        r = migrar_cb_loja_legado_em_produto(
            p,
            dry_run=True,
            liberar_intruso=True,
        )

        self.assertIsNotNone(r)
        self.assertEqual(r["principal_novo"], "2300000001556")
        self.assertIn("liberar_intruso", r)
        liberar.assert_called_once()

    @patch("produtos.cb_loja_legado_migrate_util.migrar_cb_loja_legado_em_produto")
    @patch("produtos.cb_loja_legado_migrate_util.iter_produtos_cb_loja_legado")
    def test_lote_repassa_liberar_intruso(self, iter_mock, migrar_mock):
        p = SimpleNamespace(produto_externo_id="a")
        iter_mock.return_value = [(p, None)]
        migrar_mock.return_value = {"produto_externo_id": "a"}

        migrar_cb_loja_legado_lote(dry_run=True, liberar_intruso=True)

        _args, kwargs = migrar_mock.call_args
        self.assertTrue(kwargs.get("liberar_intruso"))
