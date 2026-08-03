"""ENTRADA-NF-CUSTO — V. unit puxa custo do Cadastro (ignora final Mongo 0)."""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from django.test import SimpleTestCase

from produtos.views import _aplicar_produto_gestao_overlay_em_dict, _float_api_json


def _valor_custo_produto_js(p: dict) -> float | None:
    """Espelho de ``entradaNfeValorCustoProduto`` (ignora <= 0)."""
    if not isinstance(p, dict):
        return None

    def _parse(v):
        if v is None or v == "":
            return None
        try:
            n = float(v)
        except (TypeError, ValueError):
            return None
        return n if n == n else None  # NaN guard

    fin = _parse(p.get("preco_custo_final"))
    acr = _parse(p.get("preco_custo_acrescimo"))
    base = _parse(p.get("preco_custo"))
    if fin is not None and fin > 0:
        return fin
    if acr is not None and acr > 0:
        return acr
    if base is not None and base > 0:
        return base
    return None


class EntradaNfValorCustoPreferenciaTests(SimpleTestCase):
    def test_final_zero_usa_base_cadastro(self):
        # Caso Renan: Mongo final=0 · Cadastro 27
        self.assertEqual(
            _valor_custo_produto_js(
                {"preco_custo_final": 0, "preco_custo_acrescimo": 0, "preco_custo": 27}
            ),
            27.0,
        )

    def test_final_positivo_ganha(self):
        self.assertEqual(
            _valor_custo_produto_js(
                {"preco_custo_final": 30.5, "preco_custo": 27}
            ),
            30.5,
        )

    def test_tudo_zero_retorna_null(self):
        self.assertIsNone(
            _valor_custo_produto_js(
                {"preco_custo_final": 0, "preco_custo_acrescimo": 0, "preco_custo": 0}
            )
        )

    def test_so_acrescimo(self):
        self.assertEqual(
            _valor_custo_produto_js({"preco_custo_final": 0, "preco_custo_acrescimo": 12.3}),
            12.3,
        )


class OverlayCustoSyncTests(SimpleTestCase):
    def test_overlay_preenche_final_quando_mongo_zero(self):
        ov = SimpleNamespace(
            nome="",
            marca="",
            categoria="",
            fornecedor_texto="",
            unidade="",
            peso_etiqueta="",
            preco_venda=None,
            codigo_barras="",
            codigo_nfe="",
            subcategoria="",
            descricao="",
            ativo_exibicao=None,
            cadastro_extras={"preco_custo_overlay": 27.0},
        )
        row = {
            "preco_custo": 0.0,
            "preco_custo_final": 0.0,
            "preco_custo_acrescimo": 0.0,
            "preco_venda": 30.0,
        }
        with patch("produtos.views._overlay_subcategorias_para_row"), patch(
            "produtos.views.extrair_precos_por_forma_overlay", return_value=None
        ), patch("produtos.views.extrair_precos_modo_overlay", return_value="por_forma"), patch(
            "produtos.views.extrair_precos_grupos_overlay", return_value=None
        ), patch(
            "produtos.cashback_venda_util.cashback_percentual_de_overlay", return_value=0.0
        ), patch(
            "produtos.catalogo_delivery_util.aplicar_imagem_delivery_no_row"
        ):
            out = _aplicar_produto_gestao_overlay_em_dict(row, ov)
        self.assertEqual(out["preco_custo"], 27.0)
        self.assertEqual(out["preco_custo_final"], 27.0)
        self.assertEqual(out["preco_custo_acrescimo"], 27.0)
        # Preferência JS após overlay = 27 (não 0)
        self.assertEqual(_valor_custo_produto_js(out), 27.0)

    def test_overlay_nao_sobrescreve_final_positivo(self):
        ov = SimpleNamespace(
            nome="",
            marca="",
            categoria="",
            fornecedor_texto="",
            unidade="",
            peso_etiqueta="",
            preco_venda=None,
            codigo_barras="",
            codigo_nfe="",
            subcategoria="",
            descricao="",
            ativo_exibicao=None,
            cadastro_extras={"preco_custo_overlay": 27.0},
        )
        row = {
            "preco_custo": 20.0,
            "preco_custo_final": 35.0,
            "preco_custo_acrescimo": 35.0,
        }
        with patch("produtos.views._overlay_subcategorias_para_row"), patch(
            "produtos.views.extrair_precos_por_forma_overlay", return_value=None
        ), patch("produtos.views.extrair_precos_modo_overlay", return_value="por_forma"), patch(
            "produtos.views.extrair_precos_grupos_overlay", return_value=None
        ), patch(
            "produtos.cashback_venda_util.cashback_percentual_de_overlay", return_value=0.0
        ), patch(
            "produtos.catalogo_delivery_util.aplicar_imagem_delivery_no_row"
        ):
            out = _aplicar_produto_gestao_overlay_em_dict(row, ov)
        self.assertEqual(out["preco_custo"], 27.0)
        self.assertEqual(out["preco_custo_final"], 35.0)

    def test_sem_overlay_mantem_row(self):
        row = {"preco_custo": 0.0, "preco_custo_final": 0.0}
        with patch(
            "produtos.cashback_venda_util.cashback_percentual_de_overlay", return_value=0.0
        ), patch("produtos.views._overlay_subcategorias_para_row"), patch(
            "produtos.catalogo_delivery_util.aplicar_imagem_delivery_no_row"
        ):
            out = _aplicar_produto_gestao_overlay_em_dict(row, None)
        self.assertEqual(out["preco_custo"], 0.0)
        self.assertIsNone(_valor_custo_produto_js(out))


class BuscarProdutoIdCustoPgFallbackTests(SimpleTestCase):
    """Fallback ``Produto.custo`` quando Mongo/overlay ainda zerados (trecho da API)."""

    def test_pg_preenche_quando_zerado(self):
        res = {"preco_custo": 0.0, "preco_custo_final": 0.0, "preco_custo_acrescimo": 0.0}
        pc_res = _float_api_json(res.get("preco_custo") or 0)
        self.assertTrue(pc_res <= 0)
        p_pg = SimpleNamespace(custo=27.0)
        custo_pg = float(p_pg.custo or 0)
        if custo_pg > 0:
            res["preco_custo"] = round(custo_pg, 2)
            res["preco_custo_final"] = round(custo_pg, 2)
            res["preco_custo_acrescimo"] = round(custo_pg, 2)
        self.assertEqual(_valor_custo_produto_js(res), 27.0)

    def test_final_zero_apos_base_positivo_alinha(self):
        res = {"preco_custo": 27.0, "preco_custo_final": 0.0, "preco_custo_acrescimo": 0.0}
        pc_res = _float_api_json(res.get("preco_custo") or 0)
        fin_res = _float_api_json(res.get("preco_custo_final") or 0)
        if pc_res > 0 and fin_res <= 0:
            res["preco_custo_final"] = round(pc_res, 2)
            res["preco_custo_acrescimo"] = round(pc_res, 2)
        self.assertEqual(res["preco_custo_final"], 27.0)
        self.assertEqual(_valor_custo_produto_js(res), 27.0)

    def test_caso_real_gm1821_sem_overlay_custo(self):
        """Cadastro mostra Produto.custo=27; overlay sem preco_custo_overlay; Mongo 0."""
        row = {
            "preco_custo": 0.0,
            "preco_custo_final": 0.0,
            "preco_custo_acrescimo": 0.0,
        }
        # overlay sem custo → row permanece 0
        self.assertIsNone(_valor_custo_produto_js(row))
        # api_buscar ?compras=1: custo_pg_map
        custo_pg = 27.0
        custo_row = _float_api_json(row.get("preco_custo") or 0)
        if custo_pg > 0 and custo_row <= 0:
            row["preco_custo"] = round(custo_pg, 2)
            row["preco_custo_acrescimo"] = round(custo_pg, 2)
            row["preco_custo_final"] = round(custo_pg, 2)
        self.assertEqual(_valor_custo_produto_js(row), 27.0)
