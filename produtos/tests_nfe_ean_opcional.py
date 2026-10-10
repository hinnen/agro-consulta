from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from django.test import SimpleTestCase

from produtos.models import Produto, ProdutoGestaoOverlayAgro
from produtos.nfe_entrada_util import casar_produtos_postgres


class EntradaNfeEanOpcionalTests(SimpleTestCase):
    EAN_XML = "7898006191586"

    def _produto(self, pid: str, *, codigo: str, barras: str = ""):
        return SimpleNamespace(
            pk=int(codigo),
            produto_externo_id=pid,
            erp_produto_id="",
            codigo_interno=codigo,
            codigo_nfe=f"GM{codigo}",
            codigo_barras=barras,
            nome=f"Produto {codigo}",
            preco_venda=None,
        )

    def _overlay_opcional(self, produto, ean: str | None = None):
        return SimpleNamespace(
            produto_externo_id=produto.produto_externo_id,
            codigo_nfe=produto.codigo_nfe,
            codigo_barras=produto.codigo_barras or "",
            preco_venda=None,
            cadastro_extras={
                "codigos_barras_opcionais": [
                    "7898006194266",
                    ean or self.EAN_XML,
                    "7898006852138",
                ]
            },
        )

    @staticmethod
    def _qs(*, first=None, optional_rows=None):
        qs = MagicMock()
        qs.order_by.return_value = qs
        qs.first.return_value = first
        qs.only.return_value = optional_rows if optional_rows is not None else []
        return qs

    def test_gm5218_casa_ean_exato_dos_codigos_opcionais(self):
        produto = self._produto("pid-gm5218", codigo="5218", barras="2300000001470")
        overlay = self._overlay_opcional(produto)

        def produto_filter(*args, **kwargs):
            if kwargs.get("produto_externo_id") == produto.produto_externo_id:
                return self._qs(first=produto)
            return self._qs()

        def overlay_filter(*args, **kwargs):
            if kwargs.get("produto_externo_id") == produto.produto_externo_id:
                return self._qs(first=overlay)
            if args:
                return self._qs(optional_rows=[overlay])
            return self._qs()

        with (
            patch.object(Produto.objects, "filter", side_effect=produto_filter),
            patch.object(ProdutoGestaoOverlayAgro.objects, "filter", side_effect=overlay_filter),
        ):
            [item] = casar_produtos_postgres(
                [{"ean": self.EAN_XML, "c_prod": "334", "x_prod": "J. - 10 ML"}]
            )

        self.assertEqual(item["produto_id"], "pid-gm5218")
        self.assertEqual(item["codigo_nfe"], "GM5218")
        self.assertEqual(item["match_tipo"], "ean_overlay_opcional")

    def test_ean_principal_mantem_prioridade_sobre_opcional(self):
        principal = self._produto("pid-principal", codigo="6001", barras=self.EAN_XML)
        opcional = self._produto("pid-opcional", codigo="5218", barras="2300000001470")
        overlay = self._overlay_opcional(opcional)

        def produto_filter(*args, **kwargs):
            if kwargs.get("codigo_barras__iexact") == self.EAN_XML:
                return self._qs(first=principal)
            return self._qs()

        with (
            patch.object(Produto.objects, "filter", side_effect=produto_filter),
            patch.object(
                ProdutoGestaoOverlayAgro.objects,
                "filter",
                return_value=self._qs(first=None, optional_rows=[overlay]),
            ),
        ):
            [item] = casar_produtos_postgres([{"ean": self.EAN_XML, "c_prod": ""}])

        self.assertEqual(item["produto_id"], principal.produto_externo_id)
        self.assertEqual(item["match_tipo"], "ean_pg")

    def test_ean_opcional_ambiguo_nao_escolhe_produto(self):
        primeiro = self._produto("pid-a", codigo="5218", barras="2300000001470")
        segundo = self._produto("pid-b", codigo="5219", barras="2300000001487")
        overlays = [self._overlay_opcional(primeiro), self._overlay_opcional(segundo)]

        def overlay_filter(*args, **kwargs):
            if args:
                return self._qs(optional_rows=overlays)
            return self._qs()

        with (
            patch.object(Produto.objects, "filter", return_value=self._qs()),
            patch.object(ProdutoGestaoOverlayAgro.objects, "filter", side_effect=overlay_filter),
            patch(
                "produtos.nfe_entrada_util.resolver_vinculo_historico_entrada_nfe_pg",
                return_value=(None, None),
            ),
        ):
            [item] = casar_produtos_postgres([{"ean": self.EAN_XML, "c_prod": ""}])

        self.assertIsNone(item["produto_id"])
        self.assertIsNone(item["match_tipo"])
