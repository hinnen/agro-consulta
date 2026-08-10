"""Card BI Lucro Líquido — vencimento bruto + pago."""
from __future__ import annotations

from datetime import date
from decimal import Decimal
from unittest.mock import patch

from django.test import SimpleTestCase

from financeiro.services.indicadores_gerencial_pg import (
    lucro_liquido_vencimento_bruto_pago,
)


class _Emp:
    def __init__(self, pk: int, nome: str):
        self.pk = pk
        self.nome_fantasia = nome


def _core(*, rec, cmv, df, dv, dfin, rec_nao=0):
    rec = Decimal(str(rec))
    cmv = Decimal(str(cmv))
    df = Decimal(str(df))
    dv = Decimal(str(dv))
    dfin = Decimal(str(dfin))
    rec_nao = Decimal(str(rec_nao))
    op = rec - cmv - df - dv
    return {
        "receita_operacional": rec,
        "receita_nao_operacional": rec_nao,
        "cmv": cmv,
        "despesas_fixas": df,
        "despesas_variaveis": dv,
        "despesas_financeiras": dfin,
        "resultado_operacional": op,
        "resultado_liquido_gerencial": op - dfin,
    }


class LucroLiquidoVencimentoTests(SimpleTestCase):
    def test_bruto_e_pago_com_cmv_vendida(self):
        empresas = [_Emp(1, "Agro Mais Centro")]

        def fake_consol(**kwargs):
            if kwargs.get("valor") == "bruto":
                return _core(rec=1000, cmv=400, df=100, dv=50, dfin=20)
            return _core(rec=1000, cmv=200, df=80, dv=40, dfin=10)

        with (
            patch("base.models.Empresa.objects") as mock_emp,
            patch(
                "financeiro.services.indicadores_gerencial_pg.consolidar_empresa_pg",
                side_effect=fake_consol,
            ),
            patch(
                "produtos.relatorios_vendas_util.custo_mercadoria_vendida",
                return_value={"ok": True, "total": Decimal("300")},
            ),
        ):
            mock_emp.filter.return_value.only.return_value = empresas
            out = lucro_liquido_vencimento_bruto_pago(
                date(2026, 8, 1), date(2026, 8, 9), deposito="centro"
            )

        self.assertTrue(out["ok"])
        self.assertEqual(out["por"], "vencimento")
        self.assertEqual(out["cmv_modo"], "vendida")
        # rec 1000 - cmv vendida 300 - df - dv - dfin
        self.assertEqual(out["bruto"], Decimal("530"))
        self.assertEqual(out["pago"], Decimal("570"))

    def test_total_soma_lojas_e_ignora_grupo(self):
        empresas = [
            _Emp(1, "Agro Mais Centro"),
            _Emp(2, "Agro Mais Vila Elias"),
            _Emp(3, "Grupo GM"),
        ]
        vistos: list[int] = []

        def fake_consol(**kwargs):
            eid = int(kwargs["empresa_id"])
            vistos.append(eid)
            if eid == 1:
                if kwargs.get("valor") == "bruto":
                    return _core(rec=100, cmv=10, df=5, dv=5, dfin=0)
                return _core(rec=100, cmv=10, df=5, dv=5, dfin=0)
            if kwargs.get("valor") == "bruto":
                return _core(rec=50, cmv=5, df=2, dv=3, dfin=0)
            return _core(rec=50, cmv=5, df=2, dv=3, dfin=0)

        with (
            patch("base.models.Empresa.objects") as mock_emp,
            patch(
                "financeiro.services.indicadores_gerencial_pg.consolidar_empresa_pg",
                side_effect=fake_consol,
            ),
            patch(
                "produtos.relatorios_vendas_util.custo_mercadoria_vendida",
                return_value={"ok": False, "total": Decimal("0")},
            ),
        ):
            mock_emp.filter.return_value.only.return_value = empresas
            out = lucro_liquido_vencimento_bruto_pago(
                date(2026, 8, 1), date(2026, 8, 9), deposito=None
            )

        self.assertTrue(out["ok"])
        self.assertNotIn(3, vistos)
        self.assertEqual(set(vistos), {1, 2})
        # CMV paga (títulos): centro 100-10-5-5=80 · vila 50-5-2-3=40 · soma 120
        self.assertEqual(out["bruto"], Decimal("120"))
        self.assertEqual(out["pago"], Decimal("120"))
        self.assertEqual(out["cmv_modo"], "paga")

    def test_sem_empresa_volta_zero(self):
        with patch("base.models.Empresa.objects") as mock_emp:
            mock_emp.filter.return_value.only.return_value = []
            out = lucro_liquido_vencimento_bruto_pago(
                date(2026, 8, 1), date(2026, 8, 9), deposito="vila"
            )
        self.assertFalse(out["ok"])
        self.assertEqual(out["bruto"], Decimal("0"))
        self.assertEqual(out["pago"], Decimal("0"))
