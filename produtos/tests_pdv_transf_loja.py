"""PDV — Pedir loja (solicitação de transferência Centro ↔ Vila)."""
from __future__ import annotations

import json
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from django.test import RequestFactory, SimpleTestCase
from django.urls import reverse, NoReverseMatch

from produtos.pdv_transf_loja_util import (
    STATUS_ACEITO,
    STATUS_PENDENTE,
    STATUS_PRONTO,
    loja_oposta,
    pode_agir,
    qtd_decimal,
)


class UrlsPdvTransfTests(SimpleTestCase):
    def test_rotas_existem(self):
        self.assertIn("transf-loja/resumo", reverse("api_pdv_transf_loja_resumo"))
        self.assertIn("transf-loja/saldos", reverse("api_pdv_transf_loja_saldos"))
        self.assertIn("transf-loja/criar", reverse("api_pdv_transf_loja_criar"))
        self.assertIn("transf-loja/ajustar", reverse("api_pdv_transf_loja_ajustar"))
        self.assertIn("/1/", reverse("api_pdv_transf_loja_acao", args=[1]))
        try:
            reverse("sugestao_transferencia")
        except NoReverseMatch:
            self.fail("tela Logística someu — não deveria ser alterada")


class UtilBasicoTests(SimpleTestCase):
    def test_loja_oposta(self):
        self.assertEqual(loja_oposta("centro"), "vila")
        self.assertEqual(loja_oposta("vila"), "centro")
        self.assertEqual(loja_oposta("Vila Elias"), "centro")

    def test_qtd_decimal(self):
        self.assertEqual(qtd_decimal("2"), Decimal("2.000"))
        self.assertEqual(qtd_decimal("1,5"), Decimal("1.500"))
        self.assertIsNone(qtd_decimal("0"))
        self.assertIsNone(qtd_decimal("-1"))
        self.assertIsNone(qtd_decimal("abc"))

    def test_qtd_decimal_ou_zero(self):
        from produtos.pdv_transf_loja_util import qtd_decimal_ou_zero

        self.assertEqual(qtd_decimal_ou_zero(""), Decimal("0.000"))
        self.assertEqual(qtd_decimal_ou_zero("0"), Decimal("0.000"))
        self.assertEqual(qtd_decimal_ou_zero(0), Decimal("0.000"))
        self.assertEqual(qtd_decimal_ou_zero(0.0), Decimal("0.000"))
        self.assertEqual(qtd_decimal_ou_zero("2,5"), Decimal("2.500"))
        self.assertIsNone(qtd_decimal_ou_zero("-1"))
        self.assertIsNone(qtd_decimal_ou_zero("x"))


class PodeAgirTests(SimpleTestCase):
    def _sol(self, status=STATUS_PENDENTE, origem="vila", destino="centro"):
        return SimpleNamespace(status=status, loja_origem=origem, loja_destino=destino)

    def test_origem_aceita_pendente(self):
        ok, err = pode_agir(self._sol(), "vila", "aceitar")
        self.assertTrue(ok)
        self.assertEqual(err, "")

    def test_destino_nao_aceita(self):
        ok, err = pode_agir(self._sol(), "centro", "aceitar")
        self.assertFalse(ok)
        self.assertIn("Vila", err)

    def test_transferir_so_depois_de_aceito(self):
        ok, _ = pode_agir(self._sol(STATUS_PENDENTE), "vila", "transferir")
        self.assertFalse(ok)
        ok, _ = pode_agir(self._sol(STATUS_ACEITO), "vila", "transferir")
        self.assertTrue(ok)
        ok, _ = pode_agir(self._sol(STATUS_PRONTO), "centro", "transferir")
        self.assertTrue(ok)

    def test_cancelar_qualquer_loja_do_pedido(self):
        ok, _ = pode_agir(self._sol(), "centro", "cancelar")
        self.assertTrue(ok)
        ok, _ = pode_agir(self._sol(), "vila", "cancelar")
        self.assertTrue(ok)


class ResolverOperadorTests(SimpleTestCase):
    def test_sessao_sem_pin(self):
        from produtos.pdv_transf_loja_util import resolver_operador_pdv

        req = SimpleNamespace(session={"pdv_operador_nome": "Maria", "pdv_operador_user_id": None})
        with patch("produtos.pdv_transf_loja_util.get_user_model"):
            ok, label, user, err = resolver_operador_pdv(req, "")
        self.assertTrue(ok)
        self.assertEqual(label, "Maria")
        self.assertIsNone(user)
        self.assertEqual(err, "")

    def test_sem_sessao_pede_pin(self):
        from produtos.pdv_transf_loja_util import resolver_operador_pdv

        req = SimpleNamespace(session={})
        ok, label, user, err = resolver_operador_pdv(req, "")
        self.assertFalse(ok)
        self.assertIn("PIN", err)
        self.assertEqual(label, "")
        self.assertIsNone(user)


class CriarItensTests(SimpleTestCase):
    def test_normalizar_itens_ok(self):
        from produtos.pdv_transf_loja_util import _normalizar_itens

        itens, err = _normalizar_itens(
            [
                {"produto_id": "A1", "nome": "Ração", "quantidade": "2"},
                {"id": "A1", "quantidade": 9},
                {"produto_id": "B2", "nome": "Sal", "qtd": "1,5"},
            ]
        )
        self.assertEqual(err, "")
        self.assertEqual(len(itens), 2)
        self.assertEqual(itens[0]["produto_externo_id"], "A1")
        self.assertEqual(itens[1]["quantidade"], Decimal("1.500"))

    def test_normalizar_vazio(self):
        from produtos.pdv_transf_loja_util import _normalizar_itens

        itens, err = _normalizar_itens([])
        self.assertEqual(itens, [])
        self.assertIn("produto", err)


class CriarSolicitacaoMockTests(SimpleTestCase):
    def test_criar_grava_evento(self):
        from produtos.pdv_transf_loja_util import criar_solicitacao

        sol = SimpleNamespace(pk=7, loja_origem="vila")
        ev_mgr = MagicMock()
        with patch("produtos.pdv_transf_loja_util.transaction.atomic"), patch(
            "produtos.pdv_transf_loja_util.SolicitacaoTransferenciaPdv.objects.create",
            return_value=sol,
        ), patch(
            "produtos.pdv_transf_loja_util.SolicitacaoTransferenciaPdvItem",
            MagicMock(),
        ), patch(
            "produtos.pdv_transf_loja_util.SolicitacaoTransferenciaPdvEvento.objects.create",
            ev_mgr,
        ):
            out, err = criar_solicitacao(
                loja_destino="centro",
                itens_raw=[{"produto_id": "X", "nome": "Milho", "quantidade": 2}],
                observacao="cliente na frente",
                operador_label="João",
                usuario=None,
            )
        self.assertEqual(err, "")
        self.assertIs(out, sol)
        ev_mgr.assert_called_once()
        self.assertEqual(ev_mgr.call_args.kwargs["acao"], "pedir")


class ApiViewsTests(SimpleTestCase):
    def setUp(self):
        self.rf = RequestFactory()
        self.user = SimpleNamespace(is_authenticated=True, pk=1)

    def _authed(self, req, session=None):
        req.user = self.user
        req.session = session if session is not None else {}
        return req

    def test_resumo_pede_pin_sem_sessao(self):
        from produtos.views_pdv_transf_loja import api_pdv_transf_loja_resumo

        req = self._authed(self.rf.get("/api/pdv/transf-loja/resumo/"))
        with patch(
            "produtos.views_pdv_transf_loja.bootstrap_deposito",
            return_value={"deposito": "centro"},
        ), patch(
            "produtos.views_pdv_transf_loja.resumo_loja",
            return_value={
                "loja": "centro",
                "loja_label": "Centro",
                "recebidos_abertos": 2,
                "recebidos_pendentes": 1,
                "enviados_abertos": 0,
            },
        ):
            resp = api_pdv_transf_loja_resumo(req)
        self.assertEqual(resp.status_code, 200)
        body = json.loads(resp.content)
        self.assertTrue(body["ok"])
        self.assertTrue(body["precisa_pin"])
        self.assertEqual(body["recebidos_abertos"], 2)

    def test_criar_403_sem_operador(self):
        from produtos.views_pdv_transf_loja import api_pdv_transf_loja_criar

        req = self._authed(
            self.rf.post(
                "/api/pdv/transf-loja/criar/",
                data=b'{"itens":[{"produto_id":"X","quantidade":1}]}',
                content_type="application/json",
            )
        )
        resp = api_pdv_transf_loja_criar(req)
        self.assertEqual(resp.status_code, 403)
        self.assertTrue(json.loads(resp.content).get("precisa_pin"))

    def test_criar_ok_com_sessao(self):
        from produtos.views_pdv_transf_loja import api_pdv_transf_loja_criar

        sol = SimpleNamespace(pk=9, loja_origem="vila")
        req = self._authed(
            self.rf.post(
                "/api/pdv/transf-loja/criar/",
                data=b'{"itens":[{"produto_id":"X","nome":"Milho","quantidade":2}]}',
                content_type="application/json",
            ),
            session={"pdv_operador_nome": "Maria"},
        )
        with patch(
            "produtos.views_pdv_transf_loja.bootstrap_deposito",
            return_value={"deposito": "centro"},
        ), patch(
            "produtos.views_pdv_transf_loja.criar_solicitacao",
            return_value=(sol, ""),
        ), patch(
            "produtos.views_pdv_transf_loja.SolicitacaoTransferenciaPdv.objects.prefetch_related",
        ) as pref, patch(
            "produtos.views_pdv_transf_loja.serializar_solicitacao",
            return_value={"id": 9, "status": "pendente"},
        ), patch(
            "produtos.views_pdv_transf_loja.resumo_loja",
            return_value={
                "loja": "centro",
                "loja_label": "Centro",
                "recebidos_abertos": 0,
                "recebidos_pendentes": 0,
                "enviados_abertos": 1,
            },
        ):
            pref.return_value.filter.return_value.first.return_value = sol
            resp = api_pdv_transf_loja_criar(req)
        self.assertEqual(resp.status_code, 200)
        body = json.loads(resp.content)
        self.assertTrue(body["ok"])
        self.assertIn("Vila", body["mensagem"])

    def test_saldos_vazio(self):
        from produtos.views_pdv_transf_loja import api_pdv_transf_loja_saldos

        req = self._authed(self.rf.get("/api/pdv/transf-loja/saldos/"))
        resp = api_pdv_transf_loja_saldos(req)
        self.assertEqual(resp.status_code, 200)
        body = json.loads(resp.content)
        self.assertTrue(body["ok"])
        self.assertEqual(body["saldos"], {})

    def test_saldos_agro(self):
        from produtos.views_pdv_transf_loja import api_pdv_transf_loja_saldos

        req = self._authed(self.rf.get("/api/pdv/transf-loja/saldos/", {"ids": "P1,P2"}))
        with patch(
            "produtos.views.obter_conexao_mongo",
            return_value=(None, None),
        ), patch(
            "produtos.estoque_saldo_agro_util.mapa_saldos_operacionais_agro",
            return_value={
                "P1": {"saldo_centro": 12.0, "saldo_vila": 3.5},
                "P2": {"saldo_centro": 0.0, "saldo_vila": 8.0},
            },
        ):
            resp = api_pdv_transf_loja_saldos(req)
        self.assertEqual(resp.status_code, 200)
        body = json.loads(resp.content)
        self.assertTrue(body["ok"])
        self.assertEqual(body["saldos"]["P1"]["saldo_centro"], 12.0)
        self.assertEqual(body["saldos"]["P1"]["saldo_vila"], 3.5)
        self.assertEqual(body["saldos"]["P2"]["saldo_vila"], 8.0)

    def test_ajustar_403_sem_operador(self):
        from produtos.views_pdv_transf_loja import api_pdv_transf_loja_ajustar

        req = self._authed(
            self.rf.post(
                "/api/pdv/transf-loja/ajustar/",
                data=b'{"produto_id":"P1","saldo_centro":0,"saldo_vila":0}',
                content_type="application/json",
            )
        )
        resp = api_pdv_transf_loja_ajustar(req)
        self.assertEqual(resp.status_code, 403)
        self.assertTrue(json.loads(resp.content).get("precisa_pin"))

    def test_ajustar_ok(self):
        from produtos.views_pdv_transf_loja import api_pdv_transf_loja_ajustar

        req = self._authed(
            self.rf.post(
                "/api/pdv/transf-loja/ajustar/",
                data=b'{"produto_id":"P1","nome":"Milho","saldo_centro":0,"saldo_vila":2}',
                content_type="application/json",
            ),
            session={"pdv_pin_user_id": 1, "pdv_pin_label": "Op"},
        )
        with patch(
            "produtos.views_pdv_transf_loja._operador",
            return_value=(True, "Op", self.user, ""),
        ), patch(
            "produtos.pdv_transf_loja_util._aplicar_ajuste_absoluto_origem",
            return_value=(True, ""),
        ), patch(
            "produtos.views._invalidar_caches_apos_ajuste_pin",
            return_value=None,
        ):
            resp = api_pdv_transf_loja_ajustar(req)
        self.assertEqual(resp.status_code, 200)
        body = json.loads(resp.content)
        self.assertTrue(body["ok"])
        self.assertEqual(body["saldo_centro"], 0.0)
        self.assertEqual(body["saldo_vila"], 2.0)
