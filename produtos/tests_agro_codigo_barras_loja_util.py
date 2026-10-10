"""Código de barras interno 230… — EAN bip vs cadastro legado."""
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from django.test import SimpleTestCase

from produtos.agro_codigo_barras_loja_util import (
    _cb_loja_ocupado_overlays,
    _seqs_para_max_alocacao,
    alocar_proximo_codigo_barras_loja,
    bloquear_alocacao_codigo_barras_loja,
    codigos_grupo_bip_canonico,
    ean13_checksum_ok,
    ean13_para_bip_codigo_barras_loja,
    formatar_codigo_barras_loja,
    validar_codigo_barras_loja_para_salvar,
    variantes_busca_codigo_barras_loja,
)


class CodigoBarrasLojaEanTests(SimpleTestCase):
    def test_legado_1480_bip_dv_correto(self):
        cadastro = "2300000001480"
        self.assertFalse(ean13_checksum_ok(cadastro))
        bip = ean13_para_bip_codigo_barras_loja(cadastro)
        self.assertEqual(bip, "2300000001488")
        self.assertTrue(ean13_checksum_ok(bip))

    def test_busca_legado_e_bip_canonico_sao_literais_distintos(self):
        cadastro = "2300000001480"
        bip = "2300000001488"
        self.assertEqual(variantes_busca_codigo_barras_loja(cadastro), [cadastro])
        self.assertEqual(variantes_busca_codigo_barras_loja(bip), [bip])

    def test_grupo_canonico_contem_todos_os_sufixos(self):
        self.assertEqual(
            codigos_grupo_bip_canonico("2300000001471"),
            [f"230000000147{tail}" for tail in "0123456789"],
        )
        self.assertEqual(
            codigos_grupo_bip_canonico("2300000001479"),
            [f"230000000147{tail}" for tail in "0123456789"],
        )

    @patch("produtos.agro_codigo_barras_loja_util._cb_loja_ocupado_unificado", return_value=False)
    @patch("produtos.agro_codigo_barras_loja_util._max_seq_cb_loja_unificado", return_value=1480)
    def test_alocar_sem_mongo(self, _max, _occ):
        err, cb = alocar_proximo_codigo_barras_loja(None, None)
        self.assertIsNone(err)
        self.assertTrue(ean13_checksum_ok(str(cb or "")))
        self.assertEqual(cb, formatar_codigo_barras_loja(1481))

    def test_novo_formato_ja_valido(self):
        cb = formatar_codigo_barras_loja(1572)
        self.assertTrue(ean13_checksum_ok(cb))
        self.assertEqual(ean13_para_bip_codigo_barras_loja(cb), cb)
        self.assertIn(cb, variantes_busca_codigo_barras_loja(cb))

    def test_ncm_padded_nao_infla_max_seq(self):
        """NCM 23099020 em campo errado (13 dígitos) não deve esgotar faixa 230."""
        self.assertEqual(_seqs_para_max_alocacao("2309902000000"), [])
        self.assertEqual(_seqs_para_max_alocacao("2309902012345"), [])
        self.assertEqual(_seqs_para_max_alocacao("2300000001480"), [1480])

    @patch("produtos.agro_codigo_barras_loja_util._max_seq_cb_loja_postgres", return_value=999_999_998)
    @patch("produtos.agro_codigo_barras_loja_util._cb_loja_ocupado_unificado", return_value=False)
    def test_alocar_apos_max_seq_alto(self, _occ, _max_pg):
        err, cb = alocar_proximo_codigo_barras_loja(None, None)
        self.assertIsNone(err)
        self.assertTrue(ean13_checksum_ok(str(cb or "")))

    @patch(
        "produtos.agro_codigo_barras_loja_util._max_seq_cb_loja_unificado",
        return_value=146,
    )
    @patch("produtos.agro_codigo_barras_loja_util._cb_loja_ocupado_postgres")
    def test_alocador_pula_grupo_canonico_ocupado_por_legado(self, ocupado, _max):
        ocupado.side_effect = lambda cb: cb == "2300000001479"

        err, cb = alocar_proximo_codigo_barras_loja(None, None)

        self.assertIsNone(err)
        self.assertEqual(cb, formatar_codigo_barras_loja(148))
        self.assertTrue(ean13_checksum_ok(str(cb or "")))
        self.assertNotIn(cb, codigos_grupo_bip_canonico("2300000001479"))

    @patch("produtos.models.ProdutoMarcaVariacaoAgro.objects")
    @patch("produtos.models.ProdutoGestaoOverlayAgro.objects")
    def test_ocupacao_considera_codigo_opcional_overlay(self, overlays, variacoes):
        overlays.filter.return_value.exists.return_value = False
        variacoes.filter.return_value.exists.return_value = False
        overlays.exclude.return_value.only.return_value = [
            SimpleNamespace(
                cadastro_extras={
                    "codigos_barras_opcionais": ["2300000001479"],
                }
            )
        ]

        self.assertTrue(_cb_loja_ocupado_overlays("2300000001479"))
        self.assertFalse(_cb_loja_ocupado_overlays("2300000001488"))

    def test_save_rejeita_codigo_230_novo_com_dv_invalido(self):
        erro = validar_codigo_barras_loja_para_salvar("2300000014800")

        self.assertIn("EAN-13", str(erro))
        self.assertIn("botão 230", str(erro))

    @patch(
        "produtos.agro_codigo_barras_loja_util._cb_loja_pertence_ao_produto_postgres",
        return_value=False,
    )
    @patch(
        "produtos.agro_codigo_barras_loja_util._cb_loja_ocupado_postgres_por_outro"
    )
    def test_save_rejeita_grupo_fisico_ocupado_por_outro(
        self, ocupado, _pertence
    ):
        ocupado.side_effect = lambda cb, _pid="": cb == "2300000014800"

        erro = validar_codigo_barras_loja_para_salvar(
            "2300000014808",
            produto_externo_id="NOVO",
        )

        self.assertIn("valor físico bipado", str(erro))
        self.assertIn("Clique em 230 novamente", str(erro))

    @patch(
        "produtos.agro_codigo_barras_loja_util._cb_loja_ocupado_postgres_por_outro",
        return_value=False,
    )
    @patch(
        "produtos.agro_codigo_barras_loja_util._cb_loja_pertence_ao_produto_postgres",
        return_value=True,
    )
    def test_legado_inalterado_do_mesmo_produto_nao_bloqueia_edicao(
        self, _pertence, _ocupado
    ):
        erro = validar_codigo_barras_loja_para_salvar(
            "2300000001479",
            produto_externo_id="4045",
        )

        self.assertIsNone(erro)

    def test_save_postgres_adquire_lock_transacional_compartilhado(self):
        conexao = MagicMock()
        conexao.vendor = "postgresql"
        cursor = conexao.cursor.return_value.__enter__.return_value

        with patch(
            "produtos.agro_codigo_barras_loja_util.connection",
            conexao,
        ):
            bloquear_alocacao_codigo_barras_loja()

        cursor.execute.assert_called_once()
        self.assertIn("pg_advisory_xact_lock", cursor.execute.call_args.args[0])
