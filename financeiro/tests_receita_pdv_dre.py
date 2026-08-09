"""DRE gerencial usa faturamento do PDV como receita operacional."""
from __future__ import annotations

from datetime import date
from decimal import Decimal
from unittest.mock import patch

from django.test import SimpleTestCase

from financeiro.services.receita_pdv_util import (
    aplicar_receita_pdv_no_resumo,
    deposito_pdv_por_empresa_nome,
    somar_resumos_dre_empresas,
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

    def test_nao_mexe_geracao_caixa(self):
        core = {
            "receita_operacional": Decimal("0"),
            "cmv": Decimal("100"),
            "despesas_fixas": Decimal("50"),
            "despesas_variaveis": Decimal("10"),
            "despesas_financeiras": Decimal("5"),
            "geracao_caixa": Decimal("-200"),
        }
        with patch(
            "financeiro.services.receita_pdv_util.faturamento_pdv_periodo",
            return_value={"ok": True, "total": 1000, "por_dia": {}, "fonte": "pdv"},
        ):
            out = aplicar_receita_pdv_no_resumo(
                core, date(2026, 8, 1), date(2026, 8, 9), empresa_nome="Agro Mais Centro"
            )
        self.assertEqual(out["geracao_caixa"], Decimal("-200"))
        self.assertEqual(out["resultado_liquido_gerencial"], Decimal("835"))

    def test_nao_aplica_se_erro(self):
        core = {"erro": "Empresa não encontrada", "receita_operacional": Decimal("0")}
        out = aplicar_receita_pdv_no_resumo(
            core, date(2026, 8, 1), date(2026, 8, 9), empresa_nome="Agro Mais Centro"
        )
        self.assertEqual(out["erro"], "Empresa não encontrada")
        self.assertNotIn("receita_fonte", out)


class SomarGrupoDreTests(SimpleTestCase):
    def test_soma_pdv_de_cada_loja_nao_todas_unidades(self):
        centro = {
            "receita_operacional": Decimal("30000"),
            "receita_lancamentos": Decimal("0"),
            "cmv": Decimal("2000"),
            "despesas_fixas": Decimal("4000"),
            "despesas_variaveis": Decimal("600"),
            "despesas_financeiras": Decimal("1000"),
            "geracao_caixa": Decimal("-100"),
            "receita_fonte": "pdv",
        }
        vila = {
            "receita_operacional": Decimal("9000"),
            "receita_lancamentos": Decimal("10"),
            "cmv": Decimal("900"),
            "despesas_fixas": Decimal("300"),
            "despesas_variaveis": Decimal("80"),
            "despesas_financeiras": Decimal("50"),
            "geracao_caixa": Decimal("20"),
            "receita_fonte": "pdv",
        }
        out = somar_resumos_dre_empresas([centro, vila, {"erro": "falhou"}])
        self.assertEqual(out["receita_operacional"], Decimal("39000"))
        self.assertEqual(out["receita_lancamentos"], Decimal("10"))
        self.assertEqual(out["cmv"], Decimal("2900"))
        self.assertEqual(out["lucro_bruto"], Decimal("36100"))
        self.assertEqual(out["resultado_operacional"], Decimal("31120"))
        self.assertEqual(out["resultado_liquido_gerencial"], Decimal("30070"))
        self.assertEqual(out["geracao_caixa"], Decimal("-80"))
        self.assertEqual(out["receita_fonte"], "pdv")
        self.assertEqual(out["empresas_ok"], 2)


class IndicadoresDreConsistenciaTests(SimpleTestCase):
    def test_ebitda_menos_financeira_mais_nao_op_igual_liquido(self):
        from financeiro.services.indicadores_gerencial_pg import _indicadores_from_core
        from financeiro.services.resumo_operacional_mongo import natureza_buckets_from_linhas_dre

        core = {
            "receita_operacional": Decimal("39125.01"),
            "receita_nao_operacional": Decimal("100"),
            "cmv": Decimal("3902.20"),
            "despesas_fixas": Decimal("4321.88"),
            "despesas_variaveis": Decimal("683.80"),
            "despesas_financeiras": Decimal("1463.97"),
            "resultado_liquido_gerencial": Decimal("28753.16"),
            "receita_fonte": "pdv",
            "receita_lancamentos": Decimal("0"),
            "aportes_socios": Decimal("0"),
            "retiradas_socios": Decimal("0"),
        }
        ind = _indicadores_from_core(
            core,
            caixa_buckets=natureza_buckets_from_linhas_dre([]),
            dias_janela=9,
        )
        esperado = ind["ebitda"] - ind["desp_fin"] + ind["receita_nao_op"]
        self.assertEqual(ind["resultado_liquido"].quantize(Decimal("0.01")), esperado.quantize(Decimal("0.01")))
        self.assertEqual(ind["receita_op"], Decimal("39125.01"))
        self.assertEqual(ind["receita_fonte"], "pdv")


class CmvVendidaRowsTests(SimpleTestCase):
    def test_soma_custo_qtd_e_ignora_sem_custo(self):
        from produtos.relatorios_vendas_util import cmv_vendida_de_rows

        rows = [
            {"produto_id_externo": "A", "qtd": 10},
            {"produto_id_externo": "B", "qtd": 2},
            {"produto_id_externo": "C", "qtd": 5},
        ]
        meta = {
            "A": {"custo": Decimal("3.50")},
            "B": {"custo": Decimal("0")},
            "C": {"custo": Decimal("12")},
        }
        total, ok, sem = cmv_vendida_de_rows(rows, meta)
        self.assertEqual(total, Decimal("95.00"))
        self.assertEqual(ok, 2)
        self.assertEqual(sem, 1)


class RecalcIndicadoresCmvTests(SimpleTestCase):
    def test_vendida_recalcula_lucro_e_nao_mexe_caixa(self):
        from financeiro.services.indicadores_gerencial_pg import recalc_indicadores_cmv

        ind = {
            "receita_op": Decimal("100000"),
            "receita_nao_op": Decimal("0"),
            "cmv": Decimal("60000"),
            "dv": Decimal("5000"),
            "df": Decimal("8000"),
            "desp_fin": Decimal("2000"),
            "lucro_bruto": Decimal("40000"),
            "ebitda": Decimal("27000"),
            "resultado_liquido": Decimal("25000"),
            "geracao_caixa": Decimal("-1234.56"),
            "entradas_caixa": Decimal("10"),
            "saidas_caixa": Decimal("20"),
        }
        out = recalc_indicadores_cmv(ind, Decimal("24663"), 31)
        self.assertEqual(out["cmv"], Decimal("24663"))
        self.assertEqual(out["lucro_bruto"], Decimal("75337"))
        self.assertEqual(out["margem_contrib"], Decimal("70337"))
        self.assertEqual(out["ebitda"], Decimal("62337"))
        self.assertEqual(out["resultado_liquido"], Decimal("60337"))
        self.assertEqual(out["geracao_caixa"], Decimal("-1234.56"))
        self.assertEqual(out["entradas_caixa"], Decimal("10"))

    def test_paga_volta_aos_numeros_originais(self):
        from financeiro.services.indicadores_gerencial_pg import recalc_indicadores_cmv

        ind = {
            "receita_op": Decimal("100455"),
            "receita_nao_op": Decimal("0"),
            "cmv": Decimal("24663"),
            "dv": Decimal("683.80"),
            "df": Decimal("4321.88"),
            "desp_fin": Decimal("1463.97"),
            "geracao_caixa": Decimal("50"),
        }
        paga = Decimal("60339")
        out = recalc_indicadores_cmv(ind, paga, 31)
        self.assertEqual(out["lucro_bruto"], Decimal("40116"))
        self.assertEqual(out["geracao_caixa"], Decimal("50"))
