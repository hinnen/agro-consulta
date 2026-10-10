"""Regressões da colisão entre código 230 legado e EAN fisicamente bipado."""
from types import SimpleNamespace
from unittest.mock import patch

from django.test import SimpleTestCase

from produtos.cb_loja_reatribuir_util import (
    _extras_sem_grupo_antigo,
    reatribuir_cb_loja_exclusivo,
)
from produtos.mongo_index_codigos import produto_termo_bate_campos_principais


class BuscaMongoCbLojaCanonicoTests(SimpleTestCase):
    def test_ean_valido_ignora_indice_legado_desatualizado(self):
        gm4045 = {
            "CodigoNFe": "GM4045",
            "CodigoBarras": "2300000001479",
            "index_codigos": ["2300000001479", "2300000001471"],
        }
        outro_produto = {
            "CodigoNFe": "GM9999",
            "CodigoBarras": "2300000001471",
            "index_codigos": ["2300000001471"],
        }

        self.assertFalse(
            produto_termo_bate_campos_principais(gm4045, "2300000001471")
        )
        self.assertTrue(
            produto_termo_bate_campos_principais(outro_produto, "2300000001471")
        )


class ReatribuirCbLojaExclusivoTests(SimpleTestCase):
    def setUp(self):
        self.produto = SimpleNamespace(
            pk=4045,
            produto_externo_id="4045",
            codigo_barras="2300000001479",
        )

    @patch(
        "produtos.cb_loja_reatribuir_util.alocar_proximo_codigo_barras_loja",
        return_value=(None, "2300000001488"),
    )
    def test_dry_run_gm4045_retorna_ean_novo_sem_mutar(self, alocar):
        resumo = reatribuir_cb_loja_exclusivo(
            self.produto,
            esperado_atual="2300000001479",
        )

        self.assertEqual(resumo["codigo_anterior"], "2300000001479")
        self.assertEqual(resumo["codigo_novo"], "2300000001488")
        self.assertTrue(resumo["dry_run"])
        self.assertEqual(self.produto.codigo_barras, "2300000001479")
        alocar.assert_called_once_with(None, None)

    @patch("produtos.cb_loja_reatribuir_util.alocar_proximo_codigo_barras_loja")
    def test_divergencia_do_codigo_atual_bloqueia_operacao(self, alocar):
        with self.assertRaisesMessage(ValueError, "Código atual divergiu"):
            reatribuir_cb_loja_exclusivo(
                self.produto,
                esperado_atual="2300000001471",
            )
        alocar.assert_not_called()

    def test_codigo_antigo_nao_permanece_como_alias_opcional(self):
        extras = {
            "codigos_barras_opcionais": [
                "7898006191586",
                "2300000001479",
                "2300000001471",
            ],
            "campo_preservado": "ok",
        }

        limpos = _extras_sem_grupo_antigo(
            extras,
            {f"230000000147{tail}" for tail in "0123456789"},
            "2300000001488",
        )

        self.assertEqual(limpos["codigos_barras_opcionais"], ["7898006191586"])
        self.assertEqual(limpos["campo_preservado"], "ok")
