from datetime import datetime
from unittest.mock import patch

from django.http import QueryDict
from django.test import RequestFactory, SimpleTestCase
from django.urls import reverse

from produtos import relatorios_central_views as views
from produtos import relatorios_vendas_util as ru


AGG = [
    {"produto_id_externo": "1", "qtd": 2, "valor": 100},
    {"produto_id_externo": "2", "qtd": 1, "valor": 50},
    {"produto_id_externo": "3", "qtd": 3, "valor": 30},
]
META = {
    "1": {"nome": "Racao A", "codigo": "GM1", "categoria": "Racoes", "subcategoria": "Gato", "subcategoria_2": "Premium", "subcategoria_3": "Adulto", "subcategoria_4": "Frango", "custo": 20},
    "2": {"nome": "Racao B", "codigo": "GM2", "categoria": "Racoes", "subcategoria": "Cachorro", "subcategoria_2": "Premium", "subcategoria_3": "Filhote", "subcategoria_4": "Carne", "custo": 10},
    "3": {"nome": "Vaso", "codigo": "GM3", "categoria": "Jardim", "subcategoria": "Vasos", "subcategoria_2": "Plastico", "subcategoria_3": "Grande", "subcategoria_4": "Preto", "custo": 5},
}


class RelatoriosCatalogoUtilTests(SimpleTestCase):
    def setUp(self):
        self.desde = self.ate = datetime(2026, 8, 1)
        self.patches = [
            patch.object(ru, "_agg_itens_por_produto", return_value=[dict(x) for x in AGG]),
            patch.object(ru, "mapa_produtos_meta", return_value=META),
        ]
        for p in self.patches:
            p.start()
            self.addCleanup(p.stop)

    def test_api_necessaria_existe_e_request_normaliza_multi_casefold(self):
        for nome in ("_as_filtro_lista", "filtros_catalogo_request", "facetas_categoria_sub", "limitar_ranking"):
            self.assertTrue(hasattr(ru, nome), nome)
        req = RequestFactory().get("/", {"categoria": ["Racoes", "", "racoes", "Jardim"]})
        self.assertEqual(ru.filtros_catalogo_request(req)["categoria"], ["Racoes", "Jardim"])

    def test_or_no_campo_e_and_entre_campos(self):
        facetas, rows = ru.facetas_categoria_sub(
            self.desde, self.ate, categoria=["racoes", "JARDIM"], subcategoria=["gato", "cachorro"]
        )
        self.assertEqual([r["produto_id"] for r in rows], ["1", "2"])
        self.assertEqual(facetas["categoria"], ["Racoes", "Jardim"])
        self.assertIn("Vasos", facetas["subcategorias"])

    def test_agrupar_categoria_e_subcategorias_1_a_4(self):
        for campo in ("categoria", "subcategoria", "subcategoria_2", "subcategoria_3", "subcategoria_4"):
            rows, meta = ru.vendas_por_grupo_relatorio(self.desde, self.ate, agrupar=campo)
            self.assertEqual(meta["agrupar"], campo)
            self.assertTrue(rows)

    def test_curva_abc_recalcula_percentuais_no_recorte(self):
        rows, meta = ru.curva_abc(self.desde, self.ate, todos=True, categoria=["racoes"])
        self.assertEqual(meta["total_periodo"], 150)
        self.assertAlmostEqual(sum(r["pct"] for r in rows), 100, places=1)
        self.assertEqual(rows[-1]["pct_acum"], 100)

    def test_margem_reaproveita_linhas_filtradas(self):
        filtradas = ru.ranking_produtos(self.desde, self.ate, categoria=["Jardim"], limite=0)
        with patch.object(ru, "ranking_produtos", side_effect=AssertionError("nao deve consultar de novo")):
            rows = ru.margem_produtos(self.desde, self.ate, rows=filtradas)
        self.assertEqual([r["produto_id"] for r in rows], ["3"])

    def test_contrato_legado_vendas_por_grupo_e_receita(self):
        self.assertIsInstance(ru.vendas_por_grupo(self.desde, self.ate), list)
        receita = ru.receita_categorias_pdv(self.desde.date(), self.ate.date())
        self.assertEqual(receita["total"], 180)


class RelatoriosCentralRoutesTests(SimpleTestCase):
    def setUp(self):
        self.rf = RequestFactory()
        self.agg = patch.object(ru, "_agg_itens_por_produto", return_value=[dict(x) for x in AGG])
        self.meta = patch.object(ru, "mapa_produtos_meta", return_value=META)
        self.agg.start(); self.meta.start()
        self.addCleanup(self.agg.stop); self.addCleanup(self.meta.stop)

    def test_reverse_e_quatro_relatorios_html_multi_e_xlsx(self):
        casos = (
            "relatorios_mais_vendidos",
            "relatorios_vendas_grupo",
            "relatorios_curva_abc",
            "relatorios_margem",
        )
        for nome in casos:
            url = reverse(nome)
            for query in ("", "categoria=Racoes&categoria=Jardim&subcategoria=Gato", "categoria=Racoes&subcategoria=Gato&export=xlsx"):
                response = self.client.get(url + ("?" + query if query else ""))
                self.assertEqual(response.status_code, 200, (nome, query))
                if "export=xlsx" in query:
                    self.assertIn("spreadsheetml", response["Content-Type"])

    def test_todos_ru_usados_pela_view_existem(self):
        import ast
        from pathlib import Path
        tree = ast.parse(Path(views.__file__).read_text(encoding="utf-8"))
        nomes = {n.attr for n in ast.walk(tree) if isinstance(n, ast.Attribute) and isinstance(n.value, ast.Name) and n.value.id == "ru"}
        self.assertFalse(sorted(nome for nome in nomes if not hasattr(ru, nome)))