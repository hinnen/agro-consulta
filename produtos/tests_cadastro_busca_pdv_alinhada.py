"""Cadastro — filtro e ordenação alinhados ao motor PDV."""
from django.test import SimpleTestCase

from produtos.busca_filtro_pdv_util import filtrar_rows_api_estilo_pdv, ordenar_rows_api_estilo_pdv


class CadastroBuscaPdvAlinhadaTests(SimpleTestCase):
    def test_filtra_palavra_fraca_sem_identificar(self):
        rows = [
            {"id": "1", "nome": "bebedouro grande porte 10l", "marca": "X"},
            {"id": "2", "nome": "milho grande 47 kg", "marca": "PILAR"},
            {"id": "3", "nome": "milho 10kg", "marca": ""},
        ]
        out = filtrar_rows_api_estilo_pdv(rows, "milho grande")
        nomes = [r["nome"] for r in out]
        self.assertIn("milho grande 47 kg", nomes)
        self.assertNotIn("bebedouro grande porte 10l", nomes)

    def test_ordenacao_relevancia_nome_antes_alfabetico(self):
        rows = [
            {"id": "a", "nome": "zinco ração especial", "marca": ""},
            {"id": "b", "nome": "milho 1kg", "marca": ""},
            {"id": "c", "nome": "milho grande 47 kg", "marca": ""},
        ]
        out = ordenar_rows_api_estilo_pdv(rows, "milho")
        self.assertEqual(out[0]["nome"], "milho 1kg")
