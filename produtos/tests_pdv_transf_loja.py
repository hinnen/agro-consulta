"""PDV — Pedir loja (solicitação de transferência Centro ↔ Vila)."""
from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from django.test import SimpleTestCase
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
        self.assertIn("transf-loja/criar", reverse("api_pdv_transf_loja_criar"))
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
