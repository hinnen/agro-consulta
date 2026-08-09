"""DRE gerencial usa faturamento do PDV como receita operacional."""
from __future__ import annotations

from datetime import date
from decimal import Decimal
from unittest.mock import patch

from django.test import SimpleTestCase

from financeiro.services.receita_pdv_util import (
    aplicar_receita_pdv_no_resumo,
    deposito_pdv_por_empresa_nome,
)


class DepositoPdvEmpresaTests(SimpleTestCase):
    def test_centro_vila_e_grupo(self):
        self.assertEqual(deposito_pdv_por_empresa_nome("Agro Mais Centro"), "centro")
        self.assertEqual(deposito_pdv_por_empresa_nome("Agro Mais Vila Elias"), "vila")
        self.assertIsNone(deposito_pdv_por_empresa_nome("Grupo GM"))


class AplicarReceitaPdvTests(SimpleTestCase):
    def test_recalcula_lucro_e_liquido_com_pdv(self):
        core = {
            "receita_operacional": Decimal("0"),
            "cmv": Decimal("3902.20"),
            "despesas_fixas": Decimal("4321.88"),
            "despesas_variaveis": Decimal("683.80"),
            "despesas_financeiras": Decimal("1463.97"),
            "resultado_liquido_gerencial": Decimal("-10371.85"),
        }
        fake = {
            "ok": True,
            "total": 39125.01,
            "por_dia": {"2026-08-01": 5000},
            "fonte": "pdv",
        }
        with patch(
            "financeiro.services.receita_pdv_util.faturamento_pdv_periodo",
            return_value=fake,
        ):
            out = aplicar_receita_pdv_no_resumo(
                core,
                date(2026, 8, 1),
                date(2026, 8, 9),
                empresa_nome="Agro Mais Centro",
            )
        self.assertEqual(out["receita_fonte"], "pdv")
        self.assertEqual(out["receita_lancamentos"], Decimal("0"))
        self.assertEqual(out["receita_operacional"], Decimal("39125.01"))
        self.assertEqual(out["lucro_bruto"], Decimal("35222.81"))
        self.assertEqual(out["resultado_operacional"], Decimal("30217.13"))
        self.assertEqual(out["resultado_liquido_gerencial"], Decimal("28753.16"))

    def test_fallback_lancamentos_se_pdv_falhar(self):
        core = {
            "receita_operacional": Decimal("15.68"),
            "cmv": Decimal("0"),
            "despesas_fixas": Decimal("0"),
            "despesas_variaveis": Decimal("0"),
            "despesas_financeiras": Decimal("0"),
            "resultado_liquido_gerencial": Decimal("15.68"),
        }
        with patch(
            "financeiro.services.receita_pdv_util.faturamento_pdv_periodo",
            return_value={"ok": False, "total": Decimal("0"), "por_dia": {}},
        ):
            out = aplicar_receita_pdv_no_resumo(
                core, date(2026, 8, 1), date(2026, 8, 9), empresa_nome="Agro Mais Centro"
            )
        self.assertEqual(out["receita_fonte"], "lancamentos")
        self.assertEqual(out["receita_operacional"], Decimal("15.68"))
