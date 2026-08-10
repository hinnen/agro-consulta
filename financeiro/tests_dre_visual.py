"""Prévia visual DRE — pacote de despesas + query incluir_visual."""
from __future__ import annotations

from unittest.mock import patch

from django.test import SimpleTestCase

from financeiro.api.serializers import ResumoOperacionalQuerySerializer
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
