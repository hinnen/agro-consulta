import inspect
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import patch

from django.test import SimpleTestCase

from produtos.cadastro_planilha_util import (
    COL_CATEGORIA,
    COL_MARCA,
    COL_UNIDADE,
    _classificar_valor_faceta,
    _eh_limpar_planilha,
    _resolver_facetas_no_patch,
    aplicar_importacao_cadastro,
    preview_importacao_cadastro,
)


def _facetas_base():
    return {
        COL_MARCA: ["AKILES"],
        COL_CATEGORIA: ["Racoes"],
        COL_UNIDADE: ["KG"],
    }


def _canonicos(facetas):
    from produtos.cadastro_planilha_util import _mapa_canonico_faceta, _COLS_FACETA

    return {k: _mapa_canonico_faceta(facetas.get(k) or []) for k in _COLS_FACETA}


class PermitirNovosKwargTests(SimpleTestCase):
    def test_aplicar_aceita_permitir_novos(self):
        sig = inspect.signature(aplicar_importacao_cadastro)
        self.assertIn("permitir_novos", sig.parameters)
        self.assertFalse(sig.parameters["permitir_novos"].default)


class ClassificarFacetaTests(SimpleTestCase):
    def test_ok_canonico_e_typo_e_novo(self):
        fac = _facetas_base()
        can = _canonicos(fac)
        ok = _classificar_valor_faceta(COL_CATEGORIA, "Racoes", fac, can)
        self.assertEqual(ok["status"], "ok")
        self.assertEqual(ok["valor_final"], "Racoes")
        typo = _classificar_valor_faceta(COL_CATEGORIA, "Racoe", fac, can)
        self.assertEqual(typo["status"], "corrigir")
        self.assertEqual(typo["valor_final"], "Racoes")
        novo = _classificar_valor_faceta(COL_CATEGORIA, "Petiscos XYZ", fac, can)
        self.assertEqual(novo["status"], "novo")
        self.assertEqual(novo["valor_final"], "Petiscos XYZ")

    def test_limpar_nao_conta_como_novo(self):
        fac = _facetas_base()
        can = _canonicos(fac)
        self.assertTrue(_eh_limpar_planilha("-"))
        info = _classificar_valor_faceta(COL_CATEGORIA, "-", fac, can)
        self.assertEqual(info["status"], "vazio")


class ResolverFacetaPatchTests(SimpleTestCase):
    def test_bloqueia_categoria_nova_sem_flag(self):
        fac = _facetas_base()
        can = _canonicos(fac)
        patch, evs, errs = _resolver_facetas_no_patch(
            {COL_CATEGORIA: "Categoria Nova Loja"},
            fac,
            can,
            permitir_novos=False,
            linha=3,
        )
        self.assertTrue(errs)
        self.assertEqual(evs[0]["acao"], "novo")
        self.assertIn("Permitir criar novos", errs[0])

    def test_permite_categoria_nova_com_flag(self):
        fac = _facetas_base()
        can = _canonicos(fac)
        patch, evs, errs = _resolver_facetas_no_patch(
            {COL_CATEGORIA: "Categoria Nova Loja"},
            fac,
            can,
            permitir_novos=True,
            linha=3,
        )
        self.assertEqual(errs, [])
        self.assertEqual(patch[COL_CATEGORIA], "Categoria Nova Loja")
        self.assertEqual(evs[0]["acao"], "novo")

    def test_corrige_typo_automatico(self):
        fac = _facetas_base()
        can = _canonicos(fac)
        patch, evs, errs = _resolver_facetas_no_patch(
            {COL_CATEGORIA: "Racoe"},
            fac,
            can,
            permitir_novos=False,
        )
        self.assertEqual(errs, [])
        self.assertEqual(patch[COL_CATEGORIA], "Racoes")
        self.assertEqual(evs[0]["acao"], "corrigir")


class PreviewAplicarPermitirNovosTests(SimpleTestCase):
    def _csv(self, td: str) -> Path:
        p = Path(td) / "imp.csv"
        p.write_text("ID;Categoria\n12345;Petiscos Novos\n", encoding="utf-8")
        return p

    def _atual(self):
        return {
            "12345": {
                "id": "12345",
                "nome": "Racao teste",
                "marca": "AKILES",
                "categoria": "Racoes",
                "codigo_barras": "789",
                "preco_custo": 1,
                "preco_venda": 2,
            }
        }

    def test_previa_marca_valor_novo(self):
        with TemporaryDirectory() as td:
            path = self._csv(td)
            with (
                patch(
                    "produtos.cadastro_planilha_util.carregar_facetas_planilha",
                    return_value=_facetas_base(),
                ),
                patch(
                    "produtos.cadastro_planilha_util._mapa_estado_atual_produtos",
                    return_value=self._atual(),
                ),
                patch(
                    "produtos.cadastro_planilha_util._validar_patch_delivery",
                    return_value=None,
                ),
            ):
                prev = preview_importacao_cadastro(path)
        self.assertGreaterEqual(prev["n_valores_novos"], 1)
        self.assertTrue(any(e.get("tipo") == "valor_novo" for e in prev["erros"]))
        self.assertEqual(prev["valores_novos"][0]["valor"], "Petiscos Novos")

    def test_aplicar_sem_flag_bloqueia_e_com_flag_nao_typeerror(self):
        user = SimpleNamespace(is_authenticated=True)
        with TemporaryDirectory() as td:
            path = self._csv(td)
            with (
                patch(
                    "produtos.cadastro_planilha_util.carregar_facetas_planilha",
                    return_value=_facetas_base(),
                ),
                patch(
                    "produtos.cadastro_planilha_util._mapa_estado_atual_produtos",
                    return_value=self._atual(),
                ),
                patch(
                    "produtos.cadastro_planilha_util._validar_patch_delivery",
                    return_value=None,
                ),
                patch("produtos.agro_fonte_config.agro_catalogo_usa_postgres", return_value=True),
                patch("produtos.cadastro_planilha_util.transaction.atomic") as atomic_cm,
            ):
                atomic_cm.return_value.__enter__.return_value = None
                atomic_cm.return_value.__exit__.return_value = False
                with self.assertRaises(ValueError) as ctx:
                    aplicar_importacao_cadastro(path, user, nome_arquivo="x.csv", permitir_novos=False)
                self.assertIn("Permitir criar novos", str(ctx.exception))

                with (
                    patch("produtos.cadastro_planilha_util._gravar_patch_produto"),
                    patch("produtos.cadastro_planilha_util._snapshot_antes_import", return_value={"campos_alterados": ["categoria"]}),
                    patch("produtos.cadastro_planilha_util._invalidar_cache_catalogo_pdv"),
                    patch("produtos.cadastro_planilha_util.CadastroPlanilhaImportHistoricoAgro") as Hist,
                ):
                    Hist.objects.create.return_value = SimpleNamespace(pk=9)
                    Hist.Tipo.CADASTRO = "cadastro"
                    r = aplicar_importacao_cadastro(
                        path, user, nome_arquivo="x.csv", permitir_novos=True
                    )
        self.assertTrue(r["permitir_novos"])
        self.assertEqual(r["gravados"], 1)
