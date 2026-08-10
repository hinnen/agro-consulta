"""Prévia visual DRE — pacote de despesas + query incluir_visual + API."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

from django.test import SimpleTestCase
from rest_framework.test import APIRequestFactory

from financeiro.api.jsonutil import json_safe
from financeiro.api.serializers import ResumoOperacionalQuerySerializer
from financeiro.api.views import ResumoOperacionalAPIView
from financeiro.services.dre_visual_util import montar_dre_visual


class IncluirVisualSerializerTests(SimpleTestCase):
    def test_aceita_1(self):
        s = ResumoOperacionalQuerySerializer(
            data={
                "modo": "empresa",
                "empresa_id": 1,
                "data_inicio": "2026-07-01",
                "data_fim": "2026-07-31",
                "incluir_visual": "1",
            }
        )
        self.assertTrue(s.is_valid(), s.errors)
        self.assertTrue(s.validated_data["incluir_visual"])

    def test_default_false(self):
        s = ResumoOperacionalQuerySerializer(
            data={
                "modo": "empresa",
                "empresa_id": 1,
                "data_inicio": "2026-07-01",
                "data_fim": "2026-07-31",
            }
        )
        self.assertTrue(s.is_valid(), s.errors)
        self.assertFalse(s.validated_data["incluir_visual"])


class MontarDreVisualTests(SimpleTestCase):
    def test_ok_top_e_resumo(self):
        fake = {
            "ok": True,
            "buckets": [{"key": "m1", "label": "Jun"}],
            "resumo_grupos": [{"key": "fixa", "label": "Fixas", "ultimo": 10}],
            "total_ultimo_periodo": 100.0,
            "linhas": [
                {
                    "plano": "Aluguel",
                    "categoria": "Aluguel",
                    "valores": [8, 10],
                    "delta_abs": 2,
                    "tendencia": "up",
                },
            ],
        }
        with patch(
            "financeiro.services.gastos_variacao_pg.gastos_variacao_pg",
            return_value=fake,
        ):
            out = montar_dre_visual(empresa_id=1, por="competencia")
        self.assertTrue(out["ok"])
        self.assertEqual(out["variacao"]["top"][0]["plano"], "Aluguel")
        self.assertEqual(out["variacao"]["top"][0]["ultimo"], 10.0)
        self.assertEqual(out["variacao"]["resumo_grupos"][0]["key"], "fixa")

    def test_erro_var(self):
        with patch(
            "financeiro.services.gastos_variacao_pg.gastos_variacao_pg",
            return_value={"ok": False, "erro": "x"},
        ):
            out = montar_dre_visual(empresa_id=1)
        self.assertFalse(out["ok"])
        self.assertFalse(out["variacao"]["ok"])

    def test_top_max_12_e_fallback_categoria(self):
        linhas = [
            {"categoria": f"C{i}", "valores": [i], "delta_abs": 0, "tendencia": "flat"}
            for i in range(20)
        ]
        fake = {
            "ok": True,
            "buckets": [],
            "resumo_grupos": [],
            "total_ultimo_periodo": 1,
            "linhas": linhas,
        }
        with patch(
            "financeiro.services.gastos_variacao_pg.gastos_variacao_pg",
            return_value=fake,
        ):
            out = montar_dre_visual(empresa_id=1)
        self.assertEqual(len(out["variacao"]["top"]), 12)
        self.assertEqual(out["variacao"]["top"][0]["plano"], "C0")
        safe = json_safe(out)
        self.assertTrue(safe["ok"])
        self.assertIsInstance(safe["variacao"]["top"][0]["ultimo"], float)


class ApiIncluirVisualTests(SimpleTestCase):
    def _call(self, qs, core=None, visual=None):
        factory = APIRequestFactory()
        request = factory.get("/api/financeiro/resumo-operacional", qs)
        request.user = MagicMock(is_authenticated=True)
        view = ResumoOperacionalAPIView.as_view()
        core = core or {"receita_operacional": 100.0, "geracao_caixa": -10.0, "cmv": 40.0}
        visual = visual or {"ok": True, "variacao": {"ok": True, "top": []}}
        with (
            patch("financeiro.api.views._resumo_usa_titulos_pg", return_value=True),
            patch(
                "financeiro.services.resumo_operacional_pg.consolidar_empresa_pg",
                return_value=dict(core),
            ),
            patch(
                "financeiro.services.dre_visual_util.montar_dre_visual",
                return_value=visual,
            ) as mock_v,
        ):
            resp = view(request)
        return resp, mock_v

    def test_empresa_com_flag_anexa(self):
        resp, mock_v = self._call(
            {
                "modo": "empresa",
                "empresa_id": "1",
                "data_inicio": "2026-07-01",
                "data_fim": "2026-07-31",
                "incluir_visual": "1",
                "fonte": "postgres",
            }
        )
        self.assertEqual(resp.status_code, 200)
        self.assertTrue(resp.data["visual"]["ok"])
        self.assertEqual(resp.data["geracao_caixa"], -10.0)
        self.assertTrue(mock_v.called)
        self.assertEqual(mock_v.call_args.kwargs["empresa_id"], 1)

    def test_sem_flag_nao_anexa(self):
        resp, mock_v = self._call(
            {
                "modo": "empresa",
                "empresa_id": "1",
                "data_inicio": "2026-07-01",
                "data_fim": "2026-07-31",
                "fonte": "postgres",
            }
        )
        self.assertEqual(resp.status_code, 200)
        self.assertNotIn("visual", resp.data)
        self.assertFalse(mock_v.called)
