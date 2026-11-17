"""Entrada NF grava o fornecedor da nota no cadastro do produto."""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from django.test import SimpleTestCase

from produtos.nfe_entrada_util import propagar_fornecedor_nf_catalogo_entrada_nota


class PropagarFornecedorNfTests(SimpleTestCase):
    def test_grava_nome_no_produto_e_no_overlay(self):
        produto = SimpleNamespace(fornecedor_texto="", save=MagicMock())
        overlay = SimpleNamespace(fornecedor_texto="", save=MagicMock())
        with (
            patch("produtos.catalogo_agro.obter_produto_model", return_value=produto),
            patch("produtos.models.ProdutoGestaoOverlayAgro.objects.get_or_create", return_value=(overlay, False)),
            patch("produtos.cadastro_alteracao_historico_util.snapshot_overlay", side_effect=lambda ov: {"fornecedor_texto": ov.fornecedor_texto}),
            patch("produtos.cadastro_alteracao_historico_util.registrar_diffs_cadastro") as hist,
        ):
            r = propagar_fornecedor_nf_catalogo_entrada_nota(
                [{"produto_id": "abc"}],
                "Sn - Petropolis",
            )
        self.assertEqual(r["atualizados"], 1)
        self.assertEqual(produto.fornecedor_texto, "Sn - Petropolis")
        self.assertEqual(overlay.fornecedor_texto, "Sn - Petropolis")
        produto.save.assert_called_once()
        overlay.save.assert_called_once()
        hist.assert_called_once()

    def test_vazio_nao_apaga(self):
        with patch("produtos.catalogo_agro.obter_produto_model") as obter:
            r = propagar_fornecedor_nf_catalogo_entrada_nota([{"produto_id": "abc"}], "  ")
        self.assertEqual(r["atualizados"], 0)
        obter.assert_not_called()

    def test_igual_nao_grava_de_novo(self):
        produto = SimpleNamespace(fornecedor_texto="Sn - Petropolis", save=MagicMock())
        overlay = SimpleNamespace(fornecedor_texto="Sn - Petropolis", save=MagicMock())
        with (
            patch("produtos.catalogo_agro.obter_produto_model", return_value=produto),
            patch("produtos.models.ProdutoGestaoOverlayAgro.objects.get_or_create", return_value=(overlay, False)),
        ):
            r = propagar_fornecedor_nf_catalogo_entrada_nota(
                [{"produto_id": "abc"}],
                "Sn - Petropolis",
            )
        self.assertEqual(r["atualizados"], 0)
        produto.save.assert_not_called()
        overlay.save.assert_not_called()


class AplicarCadastroManualLinhaTests(SimpleTestCase):
    def test_grava_fornecedor_e_nao_troca_codigo_preenchido(self):
        from produtos.nfe_entrada_util import aplicar_cadastro_manual_linha_nf

        produto = SimpleNamespace(fornecedor_texto="", codigo_barras="789", save=MagicMock())
        overlay = SimpleNamespace(fornecedor_texto="", codigo_barras="789", save=MagicMock())
        with (
            patch("produtos.catalogo_agro.obter_produto_model", return_value=produto),
            patch("produtos.models.ProdutoGestaoOverlayAgro.objects.filter") as filtro,
        ):
            filtro.return_value.first.return_value = overlay
            r = aplicar_cadastro_manual_linha_nf(
                produto_id="abc",
                fornecedor_nome="Sn - Petropolis",
                ean="7891234567895",
            )
        self.assertTrue(r["ok"])
        self.assertEqual(produto.fornecedor_texto, "Sn - Petropolis")
        self.assertEqual(produto.codigo_barras, "789")
        self.assertIn("fornecedor", r["mudou"])
        self.assertNotIn("código de barras", r["mudou"])

    def test_sem_produto_nao_cria(self):
        from produtos.nfe_entrada_util import aplicar_cadastro_manual_linha_nf

        with (
            patch("produtos.catalogo_agro.obter_produto_model", return_value=None),
            patch("produtos.models.ProdutoGestaoOverlayAgro.objects.filter") as filtro,
        ):
            filtro.return_value.first.return_value = None
            r = aplicar_cadastro_manual_linha_nf(produto_id="abc", fornecedor_nome="Sn")
        self.assertFalse(r["ok"])
        self.assertIn("Não crio outro", r["erro"])
