from django.test import SimpleTestCase

from produtos.cadastro_planilha_util import (
    COL_MODELO,
    COL_PESO,
    COL_SUBCATEGORIA,
    COL_SUBCATEGORIA_2,
    COL_UNIDADE,
    EXPORT_COL_KEYS,
    _map_headers,
    _patch_da_linha,
    headers_export,
)


class CadastroPlanilhaNovasColunasTests(SimpleTestCase):
    def test_headers_incluem_novas_colunas(self):
        for k in (COL_SUBCATEGORIA_2, COL_UNIDADE, COL_MODELO, COL_PESO):
            self.assertIn(k, EXPORT_COL_KEYS)
        labels = [lab for lab, _ in headers_export()]
        self.assertIn("Subcategoria 2", labels)
        self.assertIn("Unidade", labels)
        self.assertIn("Modelo", labels)
        self.assertIn("Peso", labels)

    def test_subcategoria_nao_come_sub2(self):
        m = _map_headers(["Subcategoria", "Subcategoria 2"])
        self.assertEqual(m[COL_SUBCATEGORIA], "Subcategoria")
        self.assertEqual(m[COL_SUBCATEGORIA_2], "Subcategoria 2")

    def test_patch_da_linha_novos_campos(self):
        colmap = _map_headers(["ID", "Subcategoria 2", "Unidade", "Modelo", "Peso"])
        patch = _patch_da_linha(
            {
                "ID": "x",
                "Subcategoria 2": "Cachorro",
                "Unidade": "KG",
                "Modelo": "Azul",
                "Peso": "15 kg",
            },
            colmap,
        )
        self.assertEqual(patch[COL_SUBCATEGORIA_2], "Cachorro")
        self.assertEqual(patch[COL_UNIDADE], "KG")
        self.assertEqual(patch[COL_MODELO], "Azul")
        self.assertEqual(patch[COL_PESO], "15 kg")
