from types import SimpleNamespace

from django.test import SimpleTestCase

from produtos.cadastro_planilha_util import (
    COL_DEL_ATIVO,
    COL_DEL_CAT,
    COL_DEL_EMBALAGENS,
    COL_DEL_PESO,
    COL_DEL_SUB1,
    COL_DEL_TITULO,
    DELIVERY_IMPORT_KEYS,
    EXPORT_COL_KEYS,
    _aplicar_patch_delivery,
    _map_headers,
    _parse_bool_planilha,
    _patch_da_linha,
    _sim_nao,
    headers_export,
    linha_export_planilha,
    normalizar_colunas_export,
)


class CadastroPlanilhaDeliveryTests(SimpleTestCase):
    def test_headers_incluem_delivery(self):
        for k in DELIVERY_IMPORT_KEYS:
            self.assertIn(k, EXPORT_COL_KEYS)
        labels = [lab for lab, _ in headers_export()]
        self.assertIn("Delivery ativo", labels)
        self.assertIn("Delivery categoria", labels)
        self.assertIn("Delivery embalagens", labels)

    def test_aliases_delivery(self):
        m = _map_headers(
            [
                "ID",
                "Delivery ativo",
                "Delivery título",
                "Delivery peso",
                "Delivery categoria",
                "Delivery sub 1",
                "Delivery embalagens",
            ]
        )
        self.assertEqual(m[COL_DEL_ATIVO], "Delivery ativo")
        self.assertEqual(m[COL_DEL_TITULO], "Delivery título")
        self.assertEqual(m[COL_DEL_PESO], "Delivery peso")
        self.assertEqual(m[COL_DEL_CAT], "Delivery categoria")
        self.assertEqual(m[COL_DEL_SUB1], "Delivery sub 1")
        self.assertEqual(m[COL_DEL_EMBALAGENS], "Delivery embalagens")

    def test_export_cols_querystring_delivery(self):
        cols = normalizar_colunas_export("delivery_ativo,delivery_categoria,nome")
        self.assertEqual(cols[0], "id")
        self.assertIn(COL_DEL_ATIVO, cols)
        self.assertIn(COL_DEL_CAT, cols)

    def test_patch_da_linha_delivery(self):
        colmap = _map_headers(["ID", "Delivery ativo", "Delivery peso", "Delivery categoria"])
        patch = _patch_da_linha(
            {
                "ID": "x",
                "Delivery ativo": "Sim",
                "Delivery peso": "15 kg",
                "Delivery categoria": "Cães",
            },
            colmap,
        )
        self.assertEqual(patch[COL_DEL_ATIVO], "Sim")
        self.assertEqual(patch[COL_DEL_PESO], "15 kg")
        self.assertEqual(patch[COL_DEL_CAT], "Cães")

    def test_celula_vazia_delivery_nao_entra(self):
        colmap = _map_headers(["ID", "Delivery ativo", "Delivery peso"])
        patch = _patch_da_linha(
            {"ID": "x", "Delivery ativo": "", "Delivery peso": "10 kg"},
            colmap,
        )
        self.assertNotIn(COL_DEL_ATIVO, patch)
        self.assertEqual(patch[COL_DEL_PESO], "10 kg")

    def test_parse_bool(self):
        self.assertTrue(_parse_bool_planilha("Sim"))
        self.assertTrue(_parse_bool_planilha("1"))
        self.assertFalse(_parse_bool_planilha("Não"))
        self.assertFalse(_parse_bool_planilha("nao"))
        self.assertIsNone(_parse_bool_planilha("talvez"))

    def test_linha_export_defaults(self):
        line = linha_export_planilha({"id": "1", "nome": "X"})
        self.assertEqual(line[COL_DEL_ATIVO], "Não")
        self.assertEqual(line[COL_DEL_PESO], "")

    def test_aplicar_patch_delivery_ativo(self):
        ov = SimpleNamespace(cadastro_extras={}, produto_externo_id="p1")
        err = _aplicar_patch_delivery(
            ov,
            {COL_DEL_ATIVO: "Sim", COL_DEL_TITULO: "Ração Premium"},
        )
        self.assertIsNone(err)
        d = ov.cadastro_extras.get("delivery") or {}
        self.assertTrue(d.get("ativo"))
        self.assertEqual(d.get("titulo"), "Ração Premium")

    def test_sim_nao(self):
        self.assertEqual(_sim_nao(True), "Sim")
        self.assertEqual(_sim_nao(False), "Não")
