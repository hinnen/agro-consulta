"""GM0024-P: legado 1558 bipa 1556 — promover EAN válido ao salvar/migrar."""
from types import SimpleNamespace
from unittest.mock import patch

from django.test import SimpleTestCase

from produtos.agro_codigo_barras_loja_util import ean13_checksum_ok
from produtos.cb_loja_legado_migrate_util import (
    mesclar_legado_cb_em_cadastro_extras,
    preparar_codigo_barras_loja_legado,
)


class CbLojaLegadoGm0024Tests(SimpleTestCase):
    @patch(
        "produtos.agro_codigo_barras_loja_util._cb_loja_ocupado_postgres_por_outro",
        return_value=False,
    )
    @patch(
        "produtos.agro_codigo_barras_loja_util._cb_loja_pertence_ao_produto_postgres",
        return_value=True,
    )
    def test_1558_bip_canonico_e_1556(self, _pertence, _ocupado):
        legado = "2300000001558"
        bip = "2300000001556"
        self.assertFalse(ean13_checksum_ok(legado))
        self.assertTrue(ean13_checksum_ok(bip))

        principal, leg, err = preparar_codigo_barras_loja_legado(
            legado,
            produto_externo_id="gm0024",
        )
        self.assertIsNone(err)
        self.assertEqual(principal, bip)
        self.assertEqual(leg, legado)

    @patch(
        "produtos.agro_codigo_barras_loja_util._cb_loja_ocupado_postgres_por_outro",
        return_value=True,
    )
    @patch(
        "produtos.agro_codigo_barras_loja_util._cb_loja_pertence_ao_produto_postgres",
        return_value=True,
    )
    def test_colisao_1556_em_outro_produto_bloqueia_promocao(self, _pertence, _ocupado):
        _ocupado.side_effect = lambda cb, _pid="": cb == "2300000001556"

        principal, leg, err = preparar_codigo_barras_loja_legado(
            "2300000001558",
            produto_externo_id="gm0024",
        )
        self.assertIsNone(leg)
        self.assertIn("2300000001556", str(err or ""))
        self.assertEqual(principal, "2300000001558")

    def test_legado_vai_para_opcionais(self):
        ce = mesclar_legado_cb_em_cadastro_extras(
            {"codigos_barras_opcionais": ["7898006191586"]},
            legado="2300000001558",
            principal="2300000001556",
        )
        self.assertEqual(ce["codigos_barras_opcionais"][0], "7898006191586")
        self.assertIn("2300000001558", ce["codigos_barras_opcionais"])
