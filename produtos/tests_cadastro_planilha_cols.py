from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from django.test import SimpleTestCase

from produtos.cadastro_planilha_util import (
    COL_FORN_COMPRA_1,
    COL_FORN_COMPRA_2,
    COL_FORN_COMPRA_3,
    COL_MODELO,
    COL_PESO,
    COL_SUBCATEGORIA,
    COL_SUBCATEGORIA_2,
    COL_SUBCATEGORIA_3,
    COL_SUBCATEGORIA_4,
    COL_UNIDADE,
    EXPORT_COL_KEYS,
    FORNECEDOR_EXPORT_KEYS,
    IMPORT_KEYS,
    _aplicar_patch_no_produto_pg,
    _gravar_overlay_import_campo,
    _ler_overlay_import_campo,
    _ler_planilha,
    _map_headers,
    _patch_da_linha,
    _restaurar_produto_pg,
    _tem_alteracao,
    enriquecer_rows_ultimos_fornecedores,
    headers_export,
    linha_export_planilha,
    montar_xlsx_cadastro,
    normalizar_colunas_export,
)


class CadastroPlanilhaNovasColunasTests(SimpleTestCase):
    def test_headers_incluem_novas_colunas(self):
        for k in (
            COL_SUBCATEGORIA_2,
            COL_SUBCATEGORIA_3,
            COL_SUBCATEGORIA_4,
            COL_UNIDADE,
            COL_MODELO,
            COL_PESO,
            COL_FORN_COMPRA_1,
            COL_FORN_COMPRA_2,
            COL_FORN_COMPRA_3,
        ):
            self.assertIn(k, EXPORT_COL_KEYS)
        labels = [lab for lab, _ in headers_export()]
        self.assertIn("Subcategoria 2", labels)
        self.assertIn("Unidade", labels)
        self.assertIn("Modelo", labels)
        self.assertIn("Peso", labels)
        self.assertIn("Últ. fornecedor", labels)
        self.assertIn("2º fornecedor", labels)
        self.assertIn("3º fornecedor", labels)

    def test_fornecedores_so_export_nao_import(self):
        for k in FORNECEDOR_EXPORT_KEYS:
            self.assertIn(k, EXPORT_COL_KEYS)
            self.assertNotIn(k, IMPORT_KEYS)

    def test_enriquecer_fornecedores_preenche_quando_pedido(self):
        rows = [{"id": "100", "nome": "X"}]
        with patch(
            "produtos.compras_ultimas_compras_util.ultimos_fornecedores_por_produto_ids",
            return_value={"100": ["Agromaia", "Outro"]},
        ):
            enriquecer_rows_ultimos_fornecedores(rows, [COL_FORN_COMPRA_1, COL_FORN_COMPRA_2])
        self.assertEqual(rows[0][COL_FORN_COMPRA_1], "Agromaia")
        self.assertEqual(rows[0][COL_FORN_COMPRA_2], "Outro")
        self.assertEqual(rows[0][COL_FORN_COMPRA_3], "")

    def test_enriquecer_fornecedores_pula_se_coluna_nao_pedida(self):
        rows = [{"id": "100"}]
        with patch(
            "produtos.compras_ultimas_compras_util.ultimos_fornecedores_por_produto_ids"
        ) as mock_fn:
            enriquecer_rows_ultimos_fornecedores(rows, ["nome"])
            mock_fn.assert_not_called()
        self.assertNotIn(COL_FORN_COMPRA_1, rows[0])

    def test_subcategoria_nao_come_sub2(self):
        m = _map_headers(["Subcategoria", "Subcategoria 2", "Sub 3", "Subcategoria 4"])
        self.assertEqual(m[COL_SUBCATEGORIA], "Subcategoria")
        self.assertEqual(m[COL_SUBCATEGORIA_2], "Subcategoria 2")
        self.assertEqual(m[COL_SUBCATEGORIA_3], "Sub 3")
        self.assertEqual(m[COL_SUBCATEGORIA_4], "Subcategoria 4")

    def test_aliases_unidade_modelo_peso(self):
        m = _map_headers(["Unid", "Modelo", "Peso etiqueta"])
        self.assertEqual(m[COL_UNIDADE], "Unid")
        self.assertEqual(m[COL_MODELO], "Modelo")
        self.assertEqual(m[COL_PESO], "Peso etiqueta")

    def test_export_cols_querystring(self):
        cols = normalizar_colunas_export("modelo,unidade,peso_etiqueta,subcategoria_2")
        self.assertEqual(cols[0], "id")
        self.assertIn(COL_MODELO, cols)
        self.assertIn(COL_UNIDADE, cols)
        self.assertIn(COL_PESO, cols)
        self.assertIn(COL_SUBCATEGORIA_2, cols)

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

    def test_celula_vazia_nao_entra_no_patch(self):
        colmap = _map_headers(["ID", "Subcategoria 2", "Unidade", "Modelo", "Peso"])
        patch = _patch_da_linha(
            {
                "ID": "x",
                "Subcategoria 2": "",
                "Unidade": None,
                "Modelo": "   ",
                "Peso": "15 kg",
            },
            colmap,
        )
        self.assertNotIn(COL_SUBCATEGORIA_2, patch)
        self.assertNotIn(COL_UNIDADE, patch)
        self.assertNotIn(COL_MODELO, patch)
        self.assertEqual(patch[COL_PESO], "15 kg")

    def test_tem_alteracao_iguais_e_diferentes(self):
        atual = {
            "unidade": "KG",
            "modelo": "Azul",
            "peso_etiqueta": "15 kg",
            "subcategoria_2": "Cachorro",
        }
        self.assertFalse(_tem_alteracao(atual, {COL_UNIDADE: "KG"}))
        self.assertTrue(_tem_alteracao(atual, {COL_UNIDADE: "SC"}))
        self.assertTrue(_tem_alteracao(atual, {COL_MODELO: "Verde"}))
        self.assertFalse(_tem_alteracao(atual, {COL_PESO: "15 kg"}))
        self.assertTrue(_tem_alteracao(atual, {COL_SUBCATEGORIA_2: "Gato"}))


class CadastroPlanilhaXlsxRoundtripTests(SimpleTestCase):
    def test_export_import_preserva_novas_colunas(self):
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
        self.assertIn("Subcategoria 2", headers)
        self.assertIn("Unidade", headers)
        self.assertIn("Modelo", headers)
        self.assertIn("Peso", headers)
        colmap = _map_headers(headers)
        patch = _patch_da_linha(raw_rows[0], colmap)
        self.assertEqual(patch[COL_SUBCATEGORIA_2], "Adulto")
        self.assertEqual(patch[COL_UNIDADE], "KG")
        self.assertEqual(patch[COL_MODELO], "Azul")
        self.assertEqual(patch[COL_PESO], "15 kg")


class CadastroPlanilhaOverlayProdutoTests(SimpleTestCase):
    def test_modelo_vai_e_volta_nos_extras(self):
        ov = SimpleNamespace(cadastro_extras={"preco_custo_overlay": 1.5}, preco_venda=None)
        _gravar_overlay_import_campo(ov, COL_MODELO, "Azul")
        self.assertEqual(ov.cadastro_extras["modelo"], "Azul")
        self.assertEqual(ov.cadastro_extras["preco_custo_overlay"], 1.5)
        self.assertEqual(_ler_overlay_import_campo(ov, COL_MODELO), "Azul")
        _gravar_overlay_import_campo(ov, COL_MODELO, "")
        self.assertNotIn("modelo", ov.cadastro_extras)
        self.assertEqual(_ler_overlay_import_campo(ov, COL_MODELO), "")

    def test_peso_e_unidade_no_overlay(self):
        ov = SimpleNamespace(peso_etiqueta="", unidade="KG", cadastro_extras={}, preco_venda=None)
        _gravar_overlay_import_campo(ov, COL_PESO, "15 kg")
        _gravar_overlay_import_campo(ov, COL_UNIDADE, "SC")
        self.assertEqual(ov.peso_etiqueta, "15 kg")
        self.assertEqual(ov.unidade, "SC")
        self.assertEqual(_ler_overlay_import_campo(ov, COL_PESO), "15 kg")

    def test_gravar_so_modelo_nao_zera_unidade_no_produto(self):
        p = SimpleNamespace(
            nome="Racao",
            marca="AKILES",
            modelo="",
            unidade="KG",
            categoria="Racoes",
            subcategoria="Cachorro",
            subcategoria_2="",
            subcategoria_3="",
            subcategoria_4="",
            codigo_barras="789",
            codigo_nfe="GM1",
            preco_venda=Decimal("20"),
            custo=Decimal("10"),
            save=MagicMock(),
        )
        ov = SimpleNamespace(
            nome="Racao",
            marca="AKILES",
            unidade="",
            categoria="Racoes",
            subcategoria="Cachorro",
            subcategoria_2="Adulto",
            subcategoria_3="",
            subcategoria_4="",
            codigo_barras="789",
            codigo_nfe="GM1",
            preco_venda=None,
            cadastro_extras={"modelo": "Azul"},
        )
        with patch("produtos.catalogo_agro.obter_produto_model", return_value=p):
            _aplicar_patch_no_produto_pg(
                "AGRO1",
                ov,
                {COL_MODELO: "Azul", COL_SUBCATEGORIA_2: "Adulto", COL_PESO: "15 kg"},
            )
        self.assertEqual(p.modelo, "Azul")
        self.assertEqual(p.subcategoria_2, "Adulto")
        self.assertEqual(p.unidade, "KG")
        self.assertEqual(p.marca, "AKILES")
        p.save.assert_called_once()

    def test_so_peso_nao_salva_produto(self):
        p = SimpleNamespace(unidade="KG", modelo="", save=MagicMock())
        ov = SimpleNamespace(peso_etiqueta="15 kg", cadastro_extras={}, preco_venda=None)
        with patch("produtos.catalogo_agro.obter_produto_model", return_value=p):
            _aplicar_patch_no_produto_pg("AGRO1", ov, {COL_PESO: "15 kg"})
        p.save.assert_not_called()
        self.assertEqual(p.unidade, "KG")

    def test_desfazer_restaura_modelo_unidade_sub2(self):
        p = SimpleNamespace(
            nome="Racao",
            marca="AKILES",
            modelo="Azul",
            unidade="SC",
            categoria="Racoes",
            subcategoria="Cachorro",
            subcategoria_2="Adulto",
            subcategoria_3="",
            subcategoria_4="",
            codigo_barras="789",
            codigo_nfe="GM1",
            preco_venda=Decimal("20"),
            custo=Decimal("10"),
            save=MagicMock(),
        )
        pg_antes = {
            "custo": 10.0,
            "preco_venda": 20.0,
            "nome": "Racao",
            "marca": "AKILES",
            "modelo": "",
            "unidade": "KG",
            "categoria": "Racoes",
            "subcategoria": "Cachorro",
            "subcategoria_2": "",
            "subcategoria_3": "",
            "subcategoria_4": "",
            "codigo_barras": "789",
            "codigo_nfe": "GM1",
        }
        changed = _restaurar_produto_pg(
            p,
            pg_antes,
            [COL_MODELO, COL_UNIDADE, COL_SUBCATEGORIA_2, COL_PESO],
        )
        self.assertTrue(changed)
        self.assertEqual(p.modelo, "")
        self.assertEqual(p.unidade, "KG")
        self.assertEqual(p.subcategoria_2, "")
        self.assertEqual(p.marca, "AKILES")


class CadastroPlanilhaPermitirNovosTests(SimpleTestCase):
    def test_resolver_facetas_bloqueia_novo_sem_flag(self):
        from produtos.cadastro_planilha_util import (
            COL_CATEGORIA,
            _resolver_facetas_no_patch,
        )

        facetas = {COL_CATEGORIA: ["Racoes", "Medicamentos"]}
        canonicos = {COL_CATEGORIA: {"racoes": "Racoes", "medicamentos": "Medicamentos"}}
        patch = {COL_CATEGORIA: "Hortifruti"}
        out, eventos, erros = _resolver_facetas_no_patch(
            patch,
            facetas,
            canonicos,
            permitir_novos=False,
            linha=5,
        )
        self.assertEqual(out[COL_CATEGORIA], "Hortifruti")
        self.assertTrue(any(e.get("acao") == "novo" for e in eventos))
        self.assertTrue(erros)

    def test_resolver_facetas_permite_novo_com_flag(self):
        from produtos.cadastro_planilha_util import (
            COL_CATEGORIA,
            _resolver_facetas_no_patch,
        )

        facetas = {COL_CATEGORIA: ["Racoes"]}
        canonicos = {COL_CATEGORIA: {"racoes": "Racoes"}}
        patch = {COL_CATEGORIA: "Hortifruti"}
        out, eventos, erros = _resolver_facetas_no_patch(
            patch,
            facetas,
            canonicos,
            permitir_novos=True,
            linha=5,
        )
        self.assertEqual(out[COL_CATEGORIA], "Hortifruti")
        self.assertTrue(any(e.get("acao") == "novo" for e in eventos))
        self.assertFalse(erros)

    def test_aplicar_importacao_aceita_kwarg_permitir_novos(self):
        import inspect

        from produtos.cadastro_planilha_util import aplicar_importacao_cadastro

        sig = inspect.signature(aplicar_importacao_cadastro)
        self.assertIn("permitir_novos", sig.parameters)
