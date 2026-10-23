"""Sessão PIN única GM Lojas (7 dias)."""

from unittest.mock import patch

from django.test import RequestFactory, TestCase

from produtos.vendas_lojas_hub_sessao_util import (
    PIN_TTL_SEGUNDOS,
    SESSION_DESDE,
    SESSION_OPERADOR,
    SESSION_PIN,
    gravar_operador_sessao,
    limpar_operador_sessao,
    operador_da_sessao,
    pin_da_sessao,
)


class VendasLojasHubSessaoTests(TestCase):
    def setUp(self):
        self.rf = RequestFactory()

    def _req(self):
        req = self.rf.get("/vendas/lojas/")
        from django.contrib.sessions.middleware import SessionMiddleware

        mw = SessionMiddleware(lambda r: None)
        mw.process_request(req)
        req.session.save()
        return req

    @patch("produtos.vendas_lojas_hub_sessao_util.rotulo_operador_pin", return_value="Renan")
    @patch("produtos.vendas_lojas_hub_sessao_util.validar_pin_operador", return_value=(True, ""))
    def test_gravar_e_ler_operador(self, _v, _r):
        req = self._req()
        ok, nome, err = gravar_operador_sessao(req, "9973")
        self.assertTrue(ok, err)
        self.assertEqual(nome, "Renan")
        self.assertEqual(operador_da_sessao(req), "Renan")
        self.assertEqual(pin_da_sessao(req), "9973")
        self.assertIn(SESSION_DESDE, req.session)

    def test_expira_apos_ttl(self):
        req = self._req()
        req.session[SESSION_OPERADOR] = "X"
        req.session[SESSION_PIN] = "1"
        req.session[SESSION_DESDE] = 1.0
        self.assertEqual(operador_da_sessao(req), "")
        limpar_operador_sessao(req)
        self.assertEqual(PIN_TTL_SEGUNDOS, 7 * 24 * 3600)
