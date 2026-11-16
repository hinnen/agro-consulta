"""API GM Vendas da pulseira: token e totais, sem banco."""
import json
from decimal import Decimal
from unittest.mock import patch

from django.test import RequestFactory, SimpleTestCase, override_settings

from produtos.api_gm_vendas_band import api_gm_vendas_band

TOKEN = "a" * 32


class ApiGmVendasBandTests(SimpleTestCase):
    def setUp(self):
        self.factory = RequestFactory()

    def _get(self, auth=None):
        req = self.factory.get("/api/gm-vendas-band/")
        if auth is not None:
            req.META["HTTP_AUTHORIZATION"] = auth
        return api_gm_vendas_band(req)

    @override_settings()
    def test_sem_token_configurado(self):
        with patch.dict("os.environ", {"GM_VENDAS_BAND_TOKEN": ""}, clear=False):
            resp = self._get("Bearer " + TOKEN)
        self.assertEqual(resp.status_code, 503)

    def test_sem_autorizacao(self):
        with patch.dict("os.environ", {"GM_VENDAS_BAND_TOKEN": TOKEN}, clear=False):
            resp = self._get()
        self.assertEqual(resp.status_code, 401)

    def test_token_errado(self):
        with patch.dict("os.environ", {"GM_VENDAS_BAND_TOKEN": TOKEN}, clear=False):
            resp = self._get("Bearer " + ("b" * 32))
        self.assertEqual(resp.status_code, 401)

    def test_totais_e_sem_cache(self):
        with patch.dict("os.environ", {"GM_VENDAS_BAND_TOKEN": TOKEN}, clear=False):
            with patch(
                "produtos.vendas_lojas_util.vendas_lojas_totais",
                return_value=(Decimal("10.00"), Decimal("5.50"), Decimal("15.50")),
            ):
                resp = self._get("Bearer " + TOKEN)
        self.assertEqual(resp.status_code, 200)
        data = json.loads(resp.content)
        self.assertEqual(data["tipo"], "gm_vendas_totais")
        self.assertEqual(data["centro"], 10.0)
        self.assertEqual(data["vila"], 5.5)
        self.assertEqual(data["total"], 15.5)
        self.assertAlmostEqual(data["centro"] + data["vila"], data["total"])
        self.assertIn("data", data)
        self.assertIn("hora", data)
        self.assertIn("no-store", resp["Cache-Control"])
        self.assertEqual(resp["Pragma"], "no-cache")
        self.assertEqual(resp["Vary"], "Authorization")
