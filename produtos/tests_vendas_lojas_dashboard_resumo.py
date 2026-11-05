"""Etapa 1 — resumo do dashboard mobile (sem depender de migrate SQLite completo)."""
from __future__ import annotations

import json
from datetime import date
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

from django.http import JsonResponse
from django.test import RequestFactory, SimpleTestCase
from django.urls import reverse


def _json(resp):
    return json.loads(resp.content.decode("utf-8"))

from produtos.vendas_lojas_dashboard_resumo_util import (
    montar_resumo_dashboard,
    normalizar_loja,
    parse_periodo,
    periodo_equivalente_anterior,
)


class VendasLojasDashboardResumoUtilTests(SimpleTestCase):
    def test_parse_periodo_ok(self):
        di, df, label = parse_periodo(
            "2026-10-01", "2026-10-31", hoje=date(2026, 11, 5)
        )
        self.assertEqual(di, date(2026, 10, 1))
        self.assertEqual(df, date(2026, 10, 31))
        self.assertIn("01/10/2026", label)

    def test_parse_periodo_rejeita_invalido(self):
        with self.assertRaises(ValueError):
            parse_periodo("2026-13-01", "2026-01-02", hoje=date(2026, 11, 5))
        with self.assertRaises(ValueError):
            parse_periodo("2026-01-10", "2026-01-01", hoje=date(2026, 11, 5))
        with self.assertRaises(ValueError):
            parse_periodo("2026-01-01", "2099-01-01", hoje=date(2026, 11, 5))
        with self.assertRaises(ValueError):
            parse_periodo("2025-01-01", "2026-12-31", hoje=date(2026, 11, 5))

    def test_normalizar_loja(self):
        self.assertEqual(normalizar_loja("todas"), ("todas", None))
        self.assertEqual(normalizar_loja("centro"), ("centro", "centro"))
        self.assertEqual(normalizar_loja("vila"), ("vila", "vila"))
        with self.assertRaises(ValueError):
            normalizar_loja("erp")

    def test_periodo_anterior_equivalente(self):
        ini, fim = periodo_equivalente_anterior(date(2026, 11, 1), date(2026, 11, 7))
        self.assertEqual(ini, date(2026, 10, 25))
        self.assertEqual(fim, date(2026, 10, 31))
        self.assertEqual((fim - ini).days, 6)

    @patch("produtos.vendas_lojas_dashboard_resumo_util._serie_diaria")
    @patch("produtos.vendas_lojas_dashboard_resumo_util._contar_vendas")
    @patch("produtos.vendas_lojas_dashboard_resumo_util.vendas_lojas_totais")
    @patch("produtos.vendas_lojas_dashboard_resumo_util.vendas_lojas_total_deposito")
    def test_centro_mais_vila_igual_total(
        self, mock_dep, mock_totais, mock_count, mock_serie
    ):
        mock_totais.return_value = (
            Decimal("100.00"),
            Decimal("40.00"),
            Decimal("140.00"),
        )
        mock_dep.side_effect = lambda i, f, d: {
            None: Decimal("140.00"),
            "centro": Decimal("100.00"),
            "vila": Decimal("40.00"),
        }[d]
        mock_count.return_value = 3
        mock_serie.return_value = [
            {"data": "2026-11-05", "faturamento": "140.00", "numero_vendas": 3}
        ]
        payload = montar_resumo_dashboard(
            loja="todas",
            inicio="2026-11-05",
            fim="2026-11-05",
            hoje=date(2026, 11, 5),
        )
        c = Decimal(payload["por_loja"]["centro"])
        v = Decimal(payload["por_loja"]["vila"])
        t = Decimal(payload["por_loja"]["total"])
        self.assertEqual(c + v, t)
        self.assertEqual(Decimal(payload["indicadores"]["faturamento"]), t)
        self.assertEqual(payload["fonte"], "postgres")
        self.assertEqual(payload["indicadores"]["numero_vendas"], 3)
        self.assertEqual(payload["indicadores"]["ticket_medio"], "46.67")

    @patch("produtos.vendas_lojas_dashboard_resumo_util._serie_diaria")
    @patch("produtos.vendas_lojas_dashboard_resumo_util._contar_vendas")
    @patch("produtos.vendas_lojas_dashboard_resumo_util.vendas_lojas_totais")
    @patch("produtos.vendas_lojas_dashboard_resumo_util.vendas_lojas_total_deposito")
    def test_filtro_loja_isola(
        self, mock_dep, mock_totais, mock_count, mock_serie
    ):
        mock_totais.return_value = (
            Decimal("80.00"),
            Decimal("25.00"),
            Decimal("105.00"),
        )
        mock_dep.side_effect = lambda i, f, d: {
            None: Decimal("105.00"),
            "centro": Decimal("80.00"),
            "vila": Decimal("25.00"),
        }[d]
        mock_count.return_value = 1
        mock_serie.return_value = [
            {"data": "2026-11-05", "faturamento": "80.00", "numero_vendas": 1}
        ]
        centro = montar_resumo_dashboard(
            loja="centro",
            inicio="2026-11-05",
            fim="2026-11-05",
            hoje=date(2026, 11, 5),
        )
        self.assertEqual(Decimal(centro["indicadores"]["faturamento"]), Decimal("80.00"))
        self.assertEqual(centro["loja"], "centro")

    @patch("produtos.vendas_lojas_dashboard_resumo_util._serie_diaria")
    @patch("produtos.vendas_lojas_dashboard_resumo_util._contar_vendas")
    @patch("produtos.vendas_lojas_dashboard_resumo_util.vendas_lojas_totais")
    @patch("produtos.vendas_lojas_dashboard_resumo_util.vendas_lojas_total_deposito")
    def test_sem_vendas_flag(self, mock_dep, mock_totais, mock_count, mock_serie):
        mock_totais.return_value = (
            Decimal("0.00"),
            Decimal("0.00"),
            Decimal("0.00"),
        )
        mock_dep.return_value = Decimal("0.00")
        mock_count.return_value = 0
        mock_serie.return_value = []
        payload = montar_resumo_dashboard(
            loja="todas",
            inicio="2026-11-05",
            fim="2026-11-05",
            hoje=date(2026, 11, 5),
        )
        self.assertTrue(payload["sem_vendas"])
        self.assertIn("produtos", payload["indisponiveis"])
        self.assertIn("metas_servidor", payload["indisponiveis"])


class VendasLojasDashboardApiPermissaoTests(SimpleTestCase):
    def setUp(self):
        self.factory = RequestFactory()

    def _call_api(self, **params):
        from produtos.views import api_vendas_lojas_dashboard_resumo

        q = "&".join(f"{k}={v}" for k, v in params.items())
        request = self.factory.get(f"/api/vendas/lojas/dashboard/resumo/?{q}")
        return api_vendas_lojas_dashboard_resumo(request)

    @patch("produtos.vendas_lojas_pin_util.operador_da_sessao", return_value="")
    def test_sem_pin_retorna_401(self, _op):
        resp = self._call_api(
            loja="todas", inicio="2026-11-05", fim="2026-11-05"
        )
        self.assertEqual(resp.status_code, 401)
        self.assertIsInstance(resp, JsonResponse)
        self.assertFalse(_json(resp).get("ok"))

    @patch("produtos.vendas_lojas_pin_util.pin_liberado_vendas_tarefas", return_value=False)
    @patch("produtos.vendas_lojas_pin_util.pin_da_sessao", return_value="5555")
    @patch("produtos.vendas_lojas_pin_util.operador_da_sessao", return_value="Operador")
    def test_pin_sem_gerencia_retorna_403(self, *_mocks):
        resp = self._call_api(
            loja="todas", inicio="2026-11-05", fim="2026-11-05"
        )
        self.assertEqual(resp.status_code, 403)
        self.assertTrue(_json(resp).get("sem_acesso_gerencia"))

    @patch("produtos.vendas_lojas_pin_util.pin_liberado_vendas_tarefas", return_value=True)
    @patch("produtos.vendas_lojas_pin_util.pin_da_sessao", return_value="9973")
    @patch("produtos.vendas_lojas_pin_util.operador_da_sessao", return_value="Renan")
    def test_datas_invalidas_400(self, *_mocks):
        with patch(
            "produtos.vendas_lojas_dashboard_resumo_util.montar_resumo_dashboard",
            side_effect=ValueError("datas inválidas; use YYYY-MM-DD"),
        ):
            resp = self._call_api(loja="todas", inicio="abc", fim="2026-11-05")
        self.assertEqual(resp.status_code, 400)
        self.assertIn("datas", _json(resp).get("erro", "").lower())

    @patch("produtos.vendas_lojas_pin_util.pin_liberado_vendas_tarefas", return_value=True)
    @patch("produtos.vendas_lojas_pin_util.pin_da_sessao", return_value="9973")
    @patch("produtos.vendas_lojas_pin_util.operador_da_sessao", return_value="Renan")
    def test_ok_com_gerencia(self, *_mocks):
        payload = {
            "ok": True,
            "fonte": "postgres",
            "loja": "todas",
            "indicadores": {"faturamento": "10.00"},
            "serie": [],
            "por_loja": {"centro": "10.00", "vila": "0.00", "total": "10.00"},
            "avisos": [],
            "indisponiveis": [],
        }
        with patch(
            "produtos.vendas_lojas_dashboard_resumo_util.montar_resumo_dashboard",
            return_value=payload,
        ):
            resp = self._call_api(
                loja="todas", inicio="2026-11-05", fim="2026-11-05"
            )
        self.assertEqual(resp.status_code, 200)
        body = _json(resp)
        self.assertTrue(body["ok"])
        self.assertEqual(body["fonte"], "postgres")


class VendasLojasDashboardArquivosTests(SimpleTestCase):
    def test_hub_tem_card_dashboard(self):
        hub = Path("produtos/templates/produtos/vendas_lojas_hub.html").read_text(
            encoding="utf-8"
        )
        self.assertIn("vendas_lojas_dashboard", hub)
        self.assertIn("Indicadores das lojas", hub)
        self.assertIn("vl-hub-dashboard", hub)
        # cinco cards originais preservados
        self.assertIn("vl-hub-vendas", hub)
        self.assertIn("vl-hub-tarefas", hub)
        self.assertIn("vl-hub-fotos", hub)
        self.assertIn("vl-hub-etiquetas", hub)
        self.assertIn("vl-hub-ajuste", hub)

    def test_urls_registradas(self):
        self.assertEqual(
            reverse("vendas_lojas_dashboard"), "/vendas/lojas/dashboard/"
        )
        self.assertEqual(
            reverse("api_vendas_lojas_dashboard_resumo"),
            "/api/vendas/lojas/dashboard/resumo/",
        )

    def test_js_sem_dados_demo(self):
        js = Path(
            "produtos/static/produtos/js/vendas_lojas_dashboard_mobile.js"
        ).read_text(encoding="utf-8")
        self.assertNotIn("gm_mobile_demo_v3", js)
        self.assertNotIn("Protótipo interativo", js)
        self.assertIn("Ainda não integrado", js)
        self.assertIn("Sem permissão", js)
        self.assertIn("AbortController", js)

    def test_referencia_visual_preservada(self):
        ref = Path("docs/referencia/dashboard-mobile-original.html")
        self.assertTrue(ref.is_file())
        text = ref.read_text(encoding="utf-8")
        self.assertIn("Protótipo interativo", text)
