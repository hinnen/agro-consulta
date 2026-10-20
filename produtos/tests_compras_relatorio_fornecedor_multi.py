"""Relatório/planilha de compras — vários fornecedores na mesma folha."""
from django.test import SimpleTestCase

from produtos.views import (
    _parse_fornecedores_relatorio_compras,
    _resolver_ultima_compra_por_pid_map,
    _rotulo_fornecedores_relatorio,
)


class ParseFornecedoresRelatorioTest(SimpleTestCase):
    def test_unico_campo_legado(self):
        body = {"fornecedor_nome": "ACME", "fornecedor_id": "42"}
        out = _parse_fornecedores_relatorio_compras(body)
        self.assertEqual(out, [("ACME", "42")])

    def test_lista_fornecedores(self):
        body = {
            "fornecedores": [
                {"fornecedor_nome": "A", "fornecedor_id": "1"},
                {"nome": "B", "id": "2"},
            ]
        }
        out = _parse_fornecedores_relatorio_compras(body)
        self.assertEqual(len(out), 2)
        self.assertEqual(out[0][0], "A")
        self.assertEqual(out[1][1], "2")

    def test_deduplica_mesmo_id(self):
        body = {
            "fornecedores": [
                {"fornecedor_nome": "Velho", "fornecedor_id": "9"},
                {"fornecedor_nome": "Novo", "fornecedor_id": "9"},
            ]
        }
        out = _parse_fornecedores_relatorio_compras(body)
        self.assertEqual(len(out), 1)

    def test_rotulo_junta_nomes(self):
        rot = _rotulo_fornecedores_relatorio([("Foo LTDA", None), ("Foo", "1")])
        self.assertIn("Foo LTDA", rot)
        self.assertIn("+", rot)


class ResolverUltimaCompraMapTest(SimpleTestCase):
    def test_resolve_por_chave_alternativa(self):
        por_pid = {"100": {"dt": None, "qtd": 3.0}}
        hit = _resolver_ultima_compra_por_pid_map(por_pid, "999", ["100", "200"])
        self.assertIsNotNone(hit)
        self.assertEqual(hit["qtd"], 3.0)
