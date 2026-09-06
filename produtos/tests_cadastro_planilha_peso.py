from pathlib import Path
from tempfile import TemporaryDirectory

from django.test import SimpleTestCase

from produtos.cadastro_planilha_util import (
    COL_MODELO,
    COL_PESO,
    COL_SUBCATEGORIA,
    COL_SUBCATEGORIA_2,
    COL_SUBCATEGORIA_3,
    COL_SUBCATEGORIA_4,
    COL_UNIDADE,
    EXPORT_COL_KEYS,
    IMPORT_KEYS,
    OVERLAY_IMPORT_KEYS,
    _ler_planilha,
    _map_headers,
    _patch_da_linha,
    _tem_alteracao,
    headers_export,
    linha_export_planilha,
    montar_xlsx_cadastro,
    normalizar_colunas_export,
)


class CadastroPlanilhaPesoLojaTests(SimpleTestCase):
    def test_headers_incluem_peso_e_subs(self):
        for k in (
            COL_SUBCATEGORIA_2,
            COL_SUBCATEGORIA_3,
            COL_SUBCATEGORIA_4,
            COL_UNIDADE,
            COL_MODELO,
            COL_PESO,
        ):
            self.assertIn(k, EXPORT_COL_KEYS)
            self.assertIn(k, IMPORT_KEYS)
        self.assertIn(COL_PESO, OVERLAY_IMPORT_KEYS)
        labels = [lab for lab, _ in headers_export()]
        self.assertIn("Subcategoria 2", labels)
        self.assertIn("Unidade", labels)
        self.assertIn("Modelo", labels)
        self.assertIn("Peso", labels)

    def test_subcategoria_nao_come_sub2(self):
        m = _map_headers(["Subcategoria", "Subcategoria 2", "Subcategoria 3", "Subcategoria 4"])
        self.assertEqual(m[COL_SUBCATEGORIA], "Subcategoria")
        self.assertEqual(m[COL_SUBCATEGORIA_2], "Subcategoria 2")
        self.assertEqual(m[COL_SUBCATEGORIA_3], "Subcategoria 3")
        self.assertEqual(m[COL_SUBCATEGORIA_4], "Subcategoria 4")

    def test_aliases_unidade_modelo_peso(self):
        m = _map_headers(["Unidade", "Modelo", "Peso etiqueta"])
        self.assertEqual(m[COL_UNIDADE], "Unidade")
        self.assertEqual(m[COL_MODELO], "Modelo")
        self.assertEqual(m[COL_PESO], "Peso etiqueta")

    def test_export_cols_querystring(self):
        cols = normalizar_colunas_export("modelo,unidade,peso_etiqueta,subcategoria_2")
        self.assertEqual(cols[0], "id")
        self.assertIn(COL_MODELO, cols)
        self.assertIn(COL_UNIDADE, cols)
        self.assertIn(COL_PESO, cols)
        self.assertIn(COL_SUBCATEGORIA_2, cols)

    def test_patch_e_celula_vazia(self):
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
        vazio = _patch_da_linha(
            {
                "ID": "x",
                "Subcategoria 2": "",
                "Unidade": None,
                "Modelo": "   ",
                "Peso": "15 kg",
            },
            colmap,
        )
        self.assertNotIn(COL_SUBCATEGORIA_2, vazio)
        self.assertNotIn(COL_UNIDADE, vazio)
        self.assertNotIn(COL_MODELO, vazio)
        self.assertEqual(vazio[COL_PESO], "15 kg")

    def test_tem_alteracao_peso(self):
        atual = {
            "unidade": "KG",
            "modelo": "Azul",
            "peso_etiqueta": "15 kg",
            "subcategoria_2": "Cachorro",
        }
        self.assertFalse(_tem_alteracao(atual, {COL_PESO: "15 kg"}))
        self.assertTrue(_tem_alteracao(atual, {COL_PESO: "20 kg"}))

    def test_xlsx_roundtrip_peso(self):
        rows = [
            {
                "id": "AGROTEST01",
                "codigo_nfe": "GM4071",
                "nome": "Racao teste",
                "marca": "AKILES",
                "categoria": "Racoes",
                "subcategoria": "Cachorro",
                "subcategoria_2": "Adulto",
                "subcategoria_3": "",
                "subcategoria_4": "",
                "unidade": "KG",
                "modelo": "Azul",
                "peso_etiqueta": "15 kg",
                "codigo_barras": "7891234567890",
                "preco_custo": 10.5,
                "preco_venda": 20,
            }
        ]
        blob = montar_xlsx_cadastro(rows)
        line = linha_export_planilha(rows[0])
        self.assertEqual(line[COL_SUBCATEGORIA_2], "Adulto")
        self.assertEqual(line[COL_UNIDADE], "KG")
        self.assertEqual(line[COL_MODELO], "Azul")
        self.assertEqual(line[COL_PESO], "15 kg")
        with TemporaryDirectory() as td:
            path = Path(td) / "cad.xlsx"
            path.write_bytes(blob)
            headers, raw_rows = _ler_planilha(path)
        self.assertIn("Peso", headers)
        colmap = _map_headers(headers)
        patch = _patch_da_linha(raw_rows[0], colmap)
        self.assertEqual(patch[COL_PESO], "15 kg")
        self.assertEqual(patch[COL_UNIDADE], "KG")
        self.assertEqual(patch[COL_MODELO], "Azul")
