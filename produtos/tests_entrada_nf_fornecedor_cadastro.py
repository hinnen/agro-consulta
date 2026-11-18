"""Entrada NF grava o fornecedor da nota no cadastro do produto."""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from django.test import SimpleTestCase

from produtos.nfe_entrada_util import (
    propagar_campos_vazios_nf_catalogo_entrada_nota,
    propagar_fornecedor_nf_catalogo_entrada_nota,
)


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

    def test_nao_troca_se_ja_tem_texto(self):
        produto = SimpleNamespace(fornecedor_texto="Fornecedor antigo", save=MagicMock())
        overlay = SimpleNamespace(fornecedor_texto="", save=MagicMock())
        with (
            patch("produtos.catalogo_agro.obter_produto_model", return_value=produto),
            patch("produtos.models.ProdutoGestaoOverlayAgro.objects.get_or_create", return_value=(overlay, False)),
        ):
            r = propagar_fornecedor_nf_catalogo_entrada_nota(
                [{"produto_id": "abc"}],
                "Sn - Petropolis",
            )
        self.assertEqual(r["atualizados"], 0)
        self.assertEqual(produto.fornecedor_texto, "Fornecedor antigo")
        produto.save.assert_not_called()


class PropagarCamposVaziosNfTests(SimpleTestCase):
    def test_preenche_nome_vazio_e_nao_troca_o_que_tem(self):
        vazio = SimpleNamespace(nome="", unidade="UN", save=MagicMock())
        cheio = SimpleNamespace(nome="Cadeado 35mm", unidade="UN", save=MagicMock())
        ov_vazio = SimpleNamespace(nome="", unidade="", save=MagicMock())
        ov_cheio = SimpleNamespace(nome="Cadeado 35mm", unidade="UN", save=MagicMock())

        def obter(pid):
            return vazio if pid == "novo" else cheio

        def overlay(produto_externo_id, defaults=None):
            ov = ov_vazio if produto_externo_id == "novo" else ov_cheio
            return ov, False

        with (
            patch("produtos.catalogo_agro.obter_produto_model", side_effect=obter),
            patch("produtos.models.ProdutoGestaoOverlayAgro.objects.get_or_create", side_effect=overlay),
            patch("produtos.cadastro_alteracao_historico_util.snapshot_overlay", side_effect=lambda ov: {"nome": ov.nome}),
            patch("produtos.cadastro_alteracao_historico_util.registrar_diffs_cadastro"),
        ):
            r1 = propagar_campos_vazios_nf_catalogo_entrada_nota(
                [{"produto_id": "novo", "x_prod": "Racao 1kg", "u_com": "UN"}]
            )
            r2 = propagar_campos_vazios_nf_catalogo_entrada_nota(
                [{"produto_id": "velho", "x_prod": "Outro nome"}]
            )
        self.assertEqual(r1["atualizados"], 1)
        self.assertEqual(vazio.nome, "Racao 1kg")
        self.assertEqual(r2["atualizados"], 0)
        self.assertEqual(cheio.nome, "Cadeado 35mm")
