"""Solicitações de etiquetas (celular → desktop)."""

import json
from unittest.mock import patch

from django.contrib.auth.models import User
from django.test import Client, TestCase

from produtos.etiquetas_solicitacao_util import (
    criar_pacote_solicitacao,
    normalizar_itens_solicitacao_entrada,
    total_etiquetas_itens,
)
from produtos.models import EtiquetaSolicitacaoPacoteAgro


class EtiquetaSolicitacaoUtilTests(TestCase):
    def test_normalizar_itens_agrupa_por_id(self):
        raw = [
            {"id": "p1", "qtd": 2, "nome": "A"},
            {"id": "p1", "qtd": 3},
            {"id": "p2", "qtd": 1},
        ]
        out = normalizar_itens_solicitacao_entrada(raw)
        self.assertEqual(len(out), 2)
        self.assertEqual(out[0]["id"], "p1")
        self.assertEqual(total_etiquetas_itens(out), 3)

    def test_criar_pacote_vazio_falha(self):
        with self.assertRaises(ValueError):
            criar_pacote_solicitacao(operador_nome="Teste", itens_raw=[])


class EtiquetaSolicitacaoApiTests(TestCase):
    def setUp(self):
        self.user = User.objects.create_user(username="etq_sol", password="x")
        self.client = Client()

    @patch("produtos.etiquetas_solicitacao_util._rows_por_pids")
    def test_importar_marca_pacote_e_retorna_itens(self, rows_mock):
        rows_mock.return_value = {
            "99": {
                "id": "99",
                "nome": "Produto X",
                "codigo_nfe": "GM1",
                "preco_venda": 10.5,
                "peso_etiqueta": "",
                "codigo_barras": "",
            }
        }
        pac = EtiquetaSolicitacaoPacoteAgro.objects.create(
            operador_nome="João",
            itens_json=[{"id": "99", "qtd": 2, "nome": "Produto X"}],
            total_etiquetas=2,
            resumo_nomes="Produto X",
        )
        self.client.force_login(self.user)
        res = self.client.post(
            f"/api/produtos/etiquetas/solicitacoes/{pac.pk}/importar/",
            data="{}",
            content_type="application/json",
        )
        self.assertEqual(res.status_code, 200)
        body = res.json()
        self.assertTrue(body.get("ok"))
        self.assertEqual(len(body.get("itens") or []), 1)
        self.assertEqual(body["itens"][0]["qtd"], 2)
        pac.refresh_from_db()
        self.assertEqual(pac.status, EtiquetaSolicitacaoPacoteAgro.Status.IMPORTADO)

    @patch("produtos.views_etiquetas_solicitacao.criar_pacote_solicitacao")
    def test_mobile_criar_exige_pin(self, criar_mock):
        criar_mock.return_value = EtiquetaSolicitacaoPacoteAgro(pk=1, operador_nome="A")
        session = self.client.session
        session["fotos_produto_operador_nome"] = "Maria"
        session.save()
        res = self.client.post(
            "/api/vendas/lojas/etiquetas/solicitacoes/",
            data=json.dumps({"itens": [{"id": "1", "qtd": 1}]}),
            content_type="application/json",
        )
        self.assertEqual(res.status_code, 200)
        criar_mock.assert_called_once()
