"""Contrato do botão 230 e proteção atômica no salvamento."""
import json
from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from django.test import RequestFactory, SimpleTestCase
from django.urls import reverse

from produtos.agro_codigo_barras_loja_util import (
    ean13_checksum_ok,
    formatar_codigo_barras_loja,
)
from produtos.views import (
    _api_produtos_gestao_overlay_salvar_core,
    api_produtos_cadastro_proximo_cb_loja,
)


class GeradorCbLojaContratoTests(SimpleTestCase):
    def setUp(self):
        self.rf = RequestFactory()
        self.user = SimpleNamespace(is_authenticated=True)

    @patch(
        "produtos.agro_codigo_barras_loja_util.alocar_proximo_codigo_barras_loja_postgres",
        return_value=(None, formatar_codigo_barras_loja(1480)),
    )
    @patch("produtos.agro_fonte_config.agro_catalogo_usa_postgres", return_value=True)
    @patch("produtos.agro_fonte_config.agro_mongo_erp_desligado", return_value=True)
    def test_endpoint_configurado_retorna_ean13_valido(
        self, _mongo_off, _catalogo_pg, _alocar
    ):
        request = self.rf.get(reverse("api_produtos_cadastro_proximo_cb_loja"))
        request.user = self.user

        response = api_produtos_cadastro_proximo_cb_loja(request)
        data = json.loads(response.content)

        self.assertEqual(response.status_code, 200)
        self.assertTrue(data["ok"])
        self.assertTrue(ean13_checksum_ok(data["codigo_barras"]))
        self.assertEqual(
            reverse("api_produtos_cadastro_proximo_cb_loja"),
            "/api/produtos/cadastro/proximo-cb-loja/",
        )

    @patch(
        "produtos.agro_codigo_barras_loja_util.alocar_proximo_codigo_barras_loja_postgres",
        return_value=(None, "2300000014806"),
    )
    @patch("produtos.agro_fonte_config.agro_catalogo_usa_postgres", return_value=True)
    @patch("produtos.agro_fonte_config.agro_mongo_erp_desligado", return_value=True)
    def test_endpoint_nao_entrega_codigo_com_dv_invalido(
        self, _mongo_off, _catalogo_pg, _alocar
    ):
        request = self.rf.get(reverse("api_produtos_cadastro_proximo_cb_loja"))
        request.user = self.user

        response = api_produtos_cadastro_proximo_cb_loja(request)
        data = json.loads(response.content)

        self.assertEqual(response.status_code, 500)
        self.assertFalse(data["ok"])
        self.assertIn("EAN-13 válido", data["erro"])

    def test_template_liga_botao_url_e_campo(self):
        root = Path(__file__).resolve().parents[1]
        page = (root / "produtos/templates/produtos/produtos_cadastro_erp.html").read_text(
            encoding="utf-8"
        )
        modal = (
            root
            / "produtos/templates/produtos/_modal_editar_produto_cadastro_erp.inc.html"
        ).read_text(encoding="utf-8")

        self.assertIn("URL_PROXIMO_CB_LOJA:", page)
        self.assertIn("id=\"btn-gerar-cb-loja\"", modal)
        self.assertIn("fetch(urlCb, { credentials: 'same-origin' })", modal)
        self.assertIn("setVal('edit-cb', String(j.codigo_barras))", modal)


class SalvarCbLojaProtecaoTests(SimpleTestCase):
    def setUp(self):
        self.rf = RequestFactory()
        self.user = SimpleNamespace(is_authenticated=True)

    @patch("produtos.catalogo_agro.try_criar_produto_postgres_somente_agro")
    @patch(
        "produtos.agro_codigo_barras_loja_util.bloquear_alocacao_codigo_barras_loja"
    )
    def test_save_rejeita_dv_invalido_antes_de_criar_produto(
        self, _lock, criar
    ):
        request = self.rf.post(
            "/api/produtos/gestao/overlay/",
            data=json.dumps(
                {
                    "produto_id": "__novo__",
                    "codigo_barras": "2300000014800",
                }
            ),
            content_type="application/json",
        )
        request.user = self.user

        response = _api_produtos_gestao_overlay_salvar_core(request)
        data = json.loads(response.content)

        self.assertEqual(response.status_code, 409)
        self.assertIn("botão 230", data["erro"])
        criar.assert_not_called()

    @patch("produtos.catalogo_agro.Produto.objects.create")
    @patch("django.db.transaction.atomic", return_value=nullcontext())
    @patch(
        "produtos.agro_codigo_barras_loja_util.validar_codigo_barras_loja_para_salvar",
        return_value="Código ocupado. Clique em 230 novamente.",
    )
    @patch(
        "produtos.agro_codigo_barras_loja_util.bloquear_alocacao_codigo_barras_loja"
    )
    def test_criacao_postgres_nao_deixa_produto_parcial_quando_codigo_colide(
        self, _lock, _validar, _atomic, criar
    ):
        from produtos.catalogo_agro import try_criar_produto_postgres_somente_agro

        erro, produto_id = try_criar_produto_postgres_somente_agro(
            {
                "nome": "Produto corrida",
                "codigo": "1480",
                "codigo_nfe": "GM1480",
                "codigo_barras": "2300000014808",
                "preco_venda": "1.00",
                "preco_custo": "0.50",
            }
        )

        self.assertIsNone(produto_id)
        self.assertEqual(erro.status_code, 409)
        criar.assert_not_called()
