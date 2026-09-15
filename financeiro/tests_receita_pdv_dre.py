"""DRE gerencial usa faturamento do PDV como receita operacional."""
from __future__ import annotations

from datetime import date
from decimal import Decimal
from unittest.mock import patch

from django.test import SimpleTestCase

from financeiro.services.receita_pdv_util import (
    aplicar_cmv_modos_no_resumo,
    aplicar_receita_pdv_no_resumo,
    deposito_pdv_por_empresa_nome,
    fundir_cmv_modos_grupo,
    recalc_resumo_cmv,
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


class GetIndicadoresCmvToggleTests(SimpleTestCase):
    def _ind(self, rec: Decimal, cmv: Decimal, *, dias: int = 9):
        from financeiro.models import LancamentoFinanceiro as NF
        from financeiro.services.indicadores_gerencial_pg import _indicadores_from_core
        from financeiro.services.resumo_operacional_mongo import natureza_buckets_from_linhas_dre

        buckets = natureza_buckets_from_linhas_dre([])
        buckets[NF.NATUREZA_RECEITA_OPERACIONAL] = Decimal("80")
        buckets[NF.NATUREZA_CMV] = Decimal("180")
        return _indicadores_from_core(
            {
                "receita_operacional": rec,
                "receita_nao_operacional": Decimal("0"),
                "cmv": cmv,
                "despesas_fixas": Decimal("1000"),
                "despesas_variaveis": Decimal("200"),
                "despesas_financeiras": Decimal("50"),
                "resultado_liquido_gerencial": rec - cmv - Decimal("1250"),
                "aportes_socios": Decimal("0"),
                "retiradas_socios": Decimal("0"),
                "receita_fonte": "pdv",
                "receita_lancamentos": Decimal("0"),
            },
            caixa_buckets=buckets,
            dias_janela=dias,
        )

    @patch("financeiro.services.indicadores_gerencial_pg.gastos_variacao_pg")
    @patch("produtos.relatorios_vendas_util.custo_mercadoria_vendida")
    @patch("financeiro.services.receita_pdv_util.deposito_pdv_por_empresa_id")
    @patch("financeiro.services.indicadores_gerencial_pg._faturamento_pdv_periodo")
    @patch("financeiro.services.indicadores_gerencial_pg._bloco_periodo")
    def test_ssr_vendida_json_dois_modos_caixa_intacto(
        self, mock_bloco, mock_fat, mock_dep, mock_cmv, mock_var
    ):
        from financeiro.services.indicadores_gerencial_pg import get_indicadores_gerencial_pg

        paga_atual = self._ind(Decimal("100000"), Decimal("60000"))
        paga_60 = self._ind(Decimal("200000"), Decimal("120000"), dias=60)
        mock_bloco.side_effect = [(paga_atual, None), (paga_60, None)]
        mock_fat.return_value = {"ok": True, "total": Decimal("100000"), "por_dia": {}}
        mock_dep.return_value = "centro"
        mock_cmv.side_effect = [
            {"ok": True, "total": Decimal("24663"), "skus_com_custo": 80, "skus_sem_custo": 3},
            {"ok": True, "total": Decimal("50000"), "skus_com_custo": 90, "skus_sem_custo": 1},
        ]
        mock_var.return_value = {"ok": True, "chart": {}, "buckets": []}

        out = get_indicadores_gerencial_pg(1, date(2026, 8, 1), date(2026, 8, 9))
        self.assertEqual(out["atual"]["cmv_modo"], "vendida")
        self.assertEqual(out["atual"]["cmv"], Decimal("24663"))
        self.assertEqual(out["atual"]["cmv_paga"], Decimal("60000"))
        self.assertEqual(out["atual"]["lucro_bruto"], Decimal("75337"))
        self.assertEqual(out["atual"]["geracao_caixa"], paga_atual["geracao_caixa"])
        self.assertEqual(out["cmv_modos"]["vendida"]["atual"]["cmv"], 24663.0)
        self.assertEqual(out["cmv_modos"]["paga"]["atual"]["cmv"], 60000.0)
        self.assertEqual(out["cmv_modos"]["paga"]["atual"]["lucro_bruto"], float(paga_atual["lucro_bruto"]))
        self.assertNotIn("geracao_caixa", out["cmv_modos"]["vendida"]["atual"])
        self.assertEqual(out["atual"]["cmv_skus_sem_custo"], 3)
        self.assertEqual(mock_cmv.call_args_list[0].kwargs.get("deposito"), "centro")

    @patch("financeiro.services.indicadores_gerencial_pg.gastos_variacao_pg")
    @patch("produtos.relatorios_vendas_util.custo_mercadoria_vendida")
    @patch("financeiro.services.receita_pdv_util.deposito_pdv_por_empresa_id")
    @patch("financeiro.services.indicadores_gerencial_pg._faturamento_pdv_periodo")
    @patch("financeiro.services.indicadores_gerencial_pg._bloco_periodo")
    def test_se_cmv_vendida_falha_fica_paga_e_caixa_igual(
        self, mock_bloco, mock_fat, mock_dep, mock_cmv, mock_var
    ):
        from financeiro.services.indicadores_gerencial_pg import get_indicadores_gerencial_pg

        paga_atual = self._ind(Decimal("100000"), Decimal("60000"))
        paga_60 = self._ind(Decimal("200000"), Decimal("120000"), dias=60)
        mock_bloco.side_effect = [(paga_atual, None), (paga_60, None)]
        mock_fat.return_value = {"ok": True, "total": Decimal("100000"), "por_dia": {}}
        mock_dep.return_value = "centro"
        mock_cmv.side_effect = RuntimeError("mongo down")
        mock_var.return_value = {"ok": True, "chart": {}, "buckets": []}

        out = get_indicadores_gerencial_pg(1, date(2026, 8, 1), date(2026, 8, 9))
        self.assertEqual(out["atual"]["cmv_modo"], "paga")
        self.assertEqual(out["atual"]["cmv"], Decimal("60000"))
        self.assertEqual(out["atual"]["lucro_bruto"], paga_atual["lucro_bruto"])
        self.assertEqual(out["atual"]["geracao_caixa"], paga_atual["geracao_caixa"])
        self.assertTrue(any("CMV vendida" in str(a) for a in out["meta"]["avisos"]))


class ResumoCmvModosTests(SimpleTestCase):
    def _core(self):
        return {
            "receita_operacional": Decimal("100000"),
            "cmv": Decimal("60000"),
            "despesas_fixas": Decimal("8000"),
            "despesas_variaveis": Decimal("5000"),
            "despesas_financeiras": Decimal("2000"),
            "lucro_bruto": Decimal("40000"),
            "resultado_operacional": Decimal("27000"),
            "resultado_liquido_gerencial": Decimal("25000"),
            "geracao_caixa": Decimal("-1234.56"),
            "receita_fonte": "pdv",
        }

    def test_recalc_nao_mexe_caixa(self):
        out = recalc_resumo_cmv(self._core(), Decimal("24663"), dias_periodo=31)
        self.assertEqual(out["cmv"], Decimal("24663"))
        self.assertEqual(out["lucro_bruto"], Decimal("75337"))
        self.assertEqual(out["geracao_caixa"], Decimal("-1234.56"))

    @patch("produtos.relatorios_vendas_util.custo_mercadoria_vendida")
    def test_anexa_modos_sem_trocar_cmv_raiz(self, mock_cmv):
        mock_cmv.return_value = {
            "ok": True,
            "total": Decimal("24663"),
            "skus_com_custo": 80,
            "skus_sem_custo": 3,
        }
        out = aplicar_cmv_modos_no_resumo(
            self._core(),
            date(2026, 7, 1),
            date(2026, 7, 31),
            empresa_nome="Agro Mais Centro",
        )
        self.assertEqual(out["cmv"], Decimal("60000"))
        self.assertEqual(out["lucro_bruto"], Decimal("40000"))
        self.assertEqual(out["geracao_caixa"], Decimal("-1234.56"))
        self.assertEqual(out["cmv_modo"], "vendida")
        self.assertEqual(out["cmv_modos"]["vendida"]["cmv"], 24663.0)
        self.assertEqual(out["cmv_modos"]["vendida"]["lucro_bruto"], 75337.0)
        self.assertEqual(out["cmv_modos"]["paga"]["lucro_bruto"], 40000.0)
        self.assertEqual(out["cmv_skus_sem_custo"], 3)
        self.assertEqual(mock_cmv.call_args.kwargs.get("deposito"), "centro")
        self.assertNotIn("geracao_caixa", out["cmv_modos"]["vendida"])

    @patch("produtos.relatorios_vendas_util.custo_mercadoria_vendida")
    def test_fallback_paga_se_vendida_falha(self, mock_cmv):
        mock_cmv.side_effect = RuntimeError("mongo down")
        out = aplicar_cmv_modos_no_resumo(
            self._core(),
            date(2026, 7, 1),
            date(2026, 7, 31),
            empresa_nome="Agro Mais Centro",
        )
        self.assertEqual(out["cmv_modo"], "paga")
        self.assertFalse(out["cmv_modos"]["ok_vendida"])
        self.assertEqual(out["cmv"], Decimal("60000"))
        self.assertEqual(out["geracao_caixa"], Decimal("-1234.56"))

    def test_fundir_grupo_soma_modos(self):
        consolidado = {
            "receita_operacional": Decimal("39000"),
            "cmv": Decimal("2900"),
            "despesas_fixas": Decimal("4300"),
            "despesas_variaveis": Decimal("680"),
            "despesas_financeiras": Decimal("1050"),
            "geracao_caixa": Decimal("-80"),
        }
        subs = [
            {
                "cmv_paga": Decimal("2000"),
                "cmv_vendida": Decimal("1500"),
                "cmv_modos": {"ok_vendida": True},
                "cmv_skus_sem_custo": 2,
                "cmv_skus_com_custo": 10,
            },
            {
                "cmv_paga": Decimal("900"),
                "cmv_vendida": Decimal("700"),
                "cmv_modos": {"ok_vendida": True},
                "cmv_skus_sem_custo": 1,
                "cmv_skus_com_custo": 5,
            },
        ]
        out = fundir_cmv_modos_grupo(consolidado, subs, dias_periodo=31)
        self.assertEqual(out["cmv"], Decimal("2900"))
        self.assertEqual(out["cmv_paga"], Decimal("2900"))
        self.assertEqual(out["cmv_vendida"], Decimal("2200"))
        self.assertEqual(out["cmv_modos"]["vendida"]["cmv"], 2200.0)
        self.assertEqual(out["cmv_modos"]["paga"]["cmv"], 2900.0)
        self.assertEqual(out["geracao_caixa"], Decimal("-80"))
        self.assertEqual(out["cmv_skus_sem_custo"], 3)
