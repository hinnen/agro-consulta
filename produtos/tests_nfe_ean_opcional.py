from django.test import TestCase

from produtos.models import Produto, ProdutoGestaoOverlayAgro
from produtos.nfe_entrada_util import casar_produtos_postgres


class EntradaNfeEanOpcionalTests(TestCase):
    EAN_XML = "7898006191586"

    def _produto(self, pid: str, *, codigo: str, barras: str = "") -> Produto:
        return Produto.objects.create(
            produto_externo_id=pid,
            codigo_interno=codigo,
            codigo_nfe=f"GM{codigo}",
            codigo_barras=barras,
            nome=f"Produto {codigo}",
        )

    def _overlay_opcional(self, produto: Produto, ean: str | None = None) -> None:
        ProdutoGestaoOverlayAgro.objects.create(
            produto_externo_id=produto.produto_externo_id,
            codigo_nfe=produto.codigo_nfe,
            codigo_barras=produto.codigo_barras or "",
            cadastro_extras={
                "codigos_barras_opcionais": [
                    "7898006194266",
                    ean or self.EAN_XML,
                    "7898006852138",
                ]
            },
        )

    def test_gm5218_casa_ean_exato_dos_codigos_opcionais(self):
        produto = self._produto("pid-gm5218", codigo="5218", barras="2300000001470")
        self._overlay_opcional(produto)

        [item] = casar_produtos_postgres(
            [{"ean": self.EAN_XML, "c_prod": "334", "x_prod": "J. - 10 ML"}]
        )

        self.assertEqual(item["produto_id"], "pid-gm5218")
        self.assertEqual(item["codigo_nfe"], "GM5218")
        self.assertEqual(item["match_tipo"], "ean_overlay_opcional")

    def test_ean_principal_mantem_prioridade_sobre_opcional(self):
        principal = self._produto("pid-principal", codigo="6001", barras=self.EAN_XML)
        opcional = self._produto("pid-opcional", codigo="5218", barras="2300000001470")
        self._overlay_opcional(opcional)

        [item] = casar_produtos_postgres([{"ean": self.EAN_XML, "c_prod": ""}])

        self.assertEqual(item["produto_id"], principal.produto_externo_id)
        self.assertEqual(item["match_tipo"], "ean_pg")

    def test_ean_opcional_ambiguo_nao_escolhe_produto(self):
        primeiro = self._produto("pid-a", codigo="5218", barras="2300000001470")
        segundo = self._produto("pid-b", codigo="5219", barras="2300000001487")
        self._overlay_opcional(primeiro)
        self._overlay_opcional(segundo)

        [item] = casar_produtos_postgres([{"ean": self.EAN_XML, "c_prod": ""}])

        self.assertIsNone(item["produto_id"])
        self.assertIsNone(item["match_tipo"])
