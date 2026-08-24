"""AJUSTE-CB-PENDENTE-CADASTRO — Feito grava opcional no overlay (sem DB)."""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from django.test import SimpleTestCase

from produtos.ajuste_codigo_pendente_views import aplicar_codigo_pendente_no_cadastro


class AplicarCodigoPendenteCadastroTests(SimpleTestCase):
    def _obj(self, codigo="7898752405197", pid="prod-1"):
        return SimpleNamespace(
            codigo_bipado=codigo,
            produto_externo_id=pid,
        )

    def test_curto_rejeita(self):
        r = aplicar_codigo_pendente_no_cadastro(self._obj(codigo="1234567"))
        self.assertFalse(r["ok"])
        self.assertIn("8", r["erro"])

    def test_sem_produto_rejeita(self):
        r = aplicar_codigo_pendente_no_cadastro(self._obj(pid=""))
        self.assertFalse(r["ok"])

    @patch("produtos.ajuste_codigo_pendente_views._refresh_index_codigos_mongo")
    @patch("produtos.ajuste_codigo_pendente_views._principal_codigo_produto", return_value="2300000001490")
    @patch("produtos.ajuste_codigo_pendente_views.ProdutoGestaoOverlayAgro")
    def test_grava_opcional(self, M, _prin, _ref):
        ov = MagicMock()
        ov.codigo_barras = "2300000001490"
        ov.cadastro_extras = {}
        M.objects.get_or_create.return_value = (ov, True)

        r = aplicar_codigo_pendente_no_cadastro(self._obj())
        self.assertTrue(r["ok"])
        self.assertFalse(r["ja_era"])
        self.assertEqual(r["codigos_barras_opcionais"], ["7898752405197"])
        self.assertEqual(ov.cadastro_extras["codigos_barras_opcionais"], ["7898752405197"])
        ov.save.assert_called_once()

    @patch("produtos.ajuste_codigo_pendente_views._refresh_index_codigos_mongo")
    @patch("produtos.ajuste_codigo_pendente_views._principal_codigo_produto", return_value="7898752405197")
    @patch("produtos.ajuste_codigo_pendente_views.ProdutoGestaoOverlayAgro")
    def test_igual_principal_ja_era(self, M, _prin, _ref):
        ov = MagicMock()
        ov.codigo_barras = "7898752405197"
        ov.cadastro_extras = {}
        M.objects.get_or_create.return_value = (ov, False)

        r = aplicar_codigo_pendente_no_cadastro(self._obj())
        self.assertTrue(r["ok"])
        self.assertTrue(r["ja_era"])
        ov.save.assert_not_called()

    @patch("produtos.ajuste_codigo_pendente_views._refresh_index_codigos_mongo")
    @patch("produtos.ajuste_codigo_pendente_views._principal_codigo_produto", return_value="2300000001490")
    @patch("produtos.ajuste_codigo_pendente_views.ProdutoGestaoOverlayAgro")
    def test_ja_na_lista_idempotente(self, M, _prin, _ref):
        ov = MagicMock()
        ov.codigo_barras = "2300000001490"
        ov.cadastro_extras = {"codigos_barras_opcionais": ["7898752405197"]}
        M.objects.get_or_create.return_value = (ov, False)

        r = aplicar_codigo_pendente_no_cadastro(self._obj())
        self.assertTrue(r["ok"])
        self.assertTrue(r["ja_era"])
        ov.save.assert_called_once()
