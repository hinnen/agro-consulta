"""Cadastro — filtro e ordenação alinhados ao motor PDV."""
from django.test import SimpleTestCase

from produtos.busca_filtro_pdv_util import filtrar_rows_api_estilo_pdv, ordenar_rows_api_estilo_pdv
from produtos.cadastro_busca_codigo_util import tokens_termo_busca_cadastro
from produtos.medicamento_vet_taxonomia import normalizar_palavras_chave


class CadastroBuscaPdvAlinhadaTests(SimpleTestCase):
    def test_tokens_incluem_digito_solo_2_tempo(self):
        self.assertEqual(tokens_termo_busca_cadastro("2 tempo"), ["2", "tempo"])

    def test_palavras_chave_preserva_virgula(self):
        self.assertEqual(normalizar_palavras_chave("oleo, 2 tempo"), "oleo, 2 tempo")

    def test_filtra_oleo_2_tempo_com_busca_texto(self):
        rows = [
            {
                "id": "1",
                "nome": "OLEO CASTROL 2T",
                "busca_texto": "oleo castrol 2t oleo, 2 tempo, 2tempo",
            },
        ]
        out = filtrar_rows_api_estilo_pdv(rows, "2 tempo")
        self.assertEqual(len(out), 1)

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
