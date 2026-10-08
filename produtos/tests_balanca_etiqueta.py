"""Etiqueta de balança EAN-13 (4 dígitos + preço total)."""
from decimal import Decimal
from unittest.mock import MagicMock, patch

from django.test import SimpleTestCase

from produtos.views import (
    _buscar_produto_por_codigo_interno_balanca,
    _ean13_digito_verificador,
    _escolher_produto_plu_balanca,
    _parse_etiqueta_balanca_ean13_br,
    _produto_casa_plu_balanca,
)


class EtiquetaBalancaEan13Tests(SimpleTestCase):
    def test_parse_plu_0010_preco_4_81(self):
        r = _parse_etiqueta_balanca_ean13_br("2001000004812")
        self.assertIsNotNone(r)
        cod4, preco = r
        self.assertEqual(cod4, "0010")
        self.assertEqual(preco, Decimal("4.81"))

    def test_dv_invalido_rejeita(self):
        self.assertIsNone(_parse_etiqueta_balanca_ean13_br("2001000004810"))

    def test_dv_esperado(self):
        self.assertEqual(_ean13_digito_verificador("200100000481"), 2)

    def test_busca_prefere_plu_4_digitos(self):
        client = MagicMock()
        client.col_p = "DtoProduto"
        col = MagicMock()
        db = {client.col_p: col}
        calls = []

        def find_one(q, *a, **k):
            calls.append(q.get("index_codigos"))
            if q.get("index_codigos") == "0010":
                return {"Id": "ok", "index_codigos": ["0010"]}
            if q.get("index_codigos") == "10":
                return {"Id": "errado", "index_codigos": ["10"]}
            return None

        col.find_one.side_effect = find_one
        got = _buscar_produto_por_codigo_interno_balanca(db, client, "0010")
        self.assertEqual(got["Id"], "ok")
        self.assertEqual(calls[0], "0010")

    def test_busca_overlay_quando_sem_index(self):
        client = MagicMock()
        client.col_p = "DtoProduto"
        col = MagicMock()
        db = {client.col_p: col}
        col.find_one.return_value = None
        col.find.return_value.limit.return_value = []
        with patch(
            "produtos.views._mongo_produtos_por_overlay_codigo_busca",
            return_value=[{"Id": "ov1", "Nome": "Racao"}],
        ):
            got = _buscar_produto_por_codigo_interno_balanca(db, client, "0010")
        self.assertEqual(got["Id"], "ov1")

    def test_casa_plu_gm_e_barras(self):
        self.assertTrue(
            _produto_casa_plu_balanca({"Codigo": "GM0010-1", "index_codigos": []}, "0010")
        )
        self.assertTrue(
            _produto_casa_plu_balanca({"CodigoBarras": "0010", "index_codigos": []}, "0010")
        )
        self.assertFalse(
            _produto_casa_plu_balanca({"Codigo": "GM0143", "index_codigos": []}, "0010")
        )

    def test_escolhe_gm_menos_1_entre_varios(self):
        cand = [
            {"Id": "a", "Codigo": "GM0010-25", "CodigoBarras": ""},
            {"Id": "b", "Codigo": "GM0010-1", "CodigoBarras": ""},
            {"Id": "c", "Codigo": "GM0010-S", "CodigoBarras": ""},
        ]
        got = _escolher_produto_plu_balanca(cand, "0010")
        self.assertEqual(got["Id"], "b")
