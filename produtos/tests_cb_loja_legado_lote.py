"""Varredura em massa migrar_cb_loja_legado_lote."""
from types import SimpleNamespace
from unittest.mock import patch

from django.test import SimpleTestCase

from produtos.cb_loja_legado_migrate_util import migrar_cb_loja_legado_lote


class CbLojaLegadoLoteTests(SimpleTestCase):
    @patch("produtos.cb_loja_legado_migrate_util.migrar_cb_loja_legado_em_produto")
    @patch("produtos.cb_loja_legado_migrate_util.iter_produtos_cb_loja_legado")
    def test_lote_conta_ok_e_colisao(self, iter_mock, migrar_mock):
        p1 = SimpleNamespace(produto_externo_id="a")
        p2 = SimpleNamespace(produto_externo_id="b")
        iter_mock.return_value = [(p1, None), (p2, None)]
        migrar_mock.side_effect = [
            {"produto_externo_id": "a", "legado": "2300000001558", "principal_novo": "2300000001556"},
            {
                "produto_externo_id": "b",
                "legado": "2300000001479",
                "principal_novo": "2300000001471",
                "erro": "ocupado",
            },
        ]

        res = migrar_cb_loja_legado_lote(limit=10, dry_run=True)

        self.assertEqual(res["corrigidos"], 1)
        self.assertEqual(res["colisoes"], 1)
        self.assertEqual(len(res["colisoes_detalhe"]), 1)
