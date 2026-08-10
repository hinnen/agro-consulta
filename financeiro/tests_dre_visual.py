"""Prévia visual DRE — pacote de despesas + query incluir_visual + API."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

from django.test import SimpleTestCase
from rest_framework.test import APIRequestFactory

from financeiro.api.jsonutil import json_safe
from financeiro.api.serializers import ResumoOperacionalQuerySerializer
from financeiro.api.views import ResumoOperacionalAPIView
from financeiro.services.dre_emprestimos_util import (
    eh_entrada_emprestimo,
    eh_juros_emprestimo,
    eh_pagamento_principal_emprestimo,
)
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
        self.assertFalse(out["emprestimos"]["ok"])
        self.assertFalse(out["receita_categorias"]["ok"])

    def test_anexa_emprestimos(self):
        from datetime import date

        fake_emp = {
            "ok": True,
            "valor_devido": 1000.0,
            "valor_pago": 200.0,
            "juros": 50.0,
            "valor_emprestado": 5000.0,
            "entrada_por": "competencia",
        }
        with (
            patch(
                "financeiro.services.gastos_variacao_pg.gastos_variacao_pg",
                return_value={
                    "ok": True,
                    "linhas": [],
                    "resumo_grupos": [],
                    "buckets": [],
                    "total_ultimo_periodo": 0,
                },
            ),
            patch(
                "financeiro.services.dre_emprestimos_util.resumo_emprestimos_pg",
                return_value=fake_emp,
            ) as mock_e,
            patch(
                "produtos.relatorios_vendas_util.receita_categorias_pdv",
                return_value={
                    "ok": True,
                    "total": 100.0,
                    "fatias": [{"nome": "Rações", "valor": 80.0, "pct": 80.0}],
                },
            ),
            patch(
                "produtos.lancamentos_financeiro_pg_analytics_util.dre_resumo_simples_pg",
                return_value={
                    "ok": True,
                    "linhas": [
                        {"plano": "Aluguel", "despesa": 18356.83, "receita": 0},
                        {"plano": "Comissão", "despesa": 2894.58, "receita": 0},
                        {"plano": "IOF", "despesa": 3096.78, "receita": 0},
                    ],
                },
            ),
            patch(
                "financeiro.services.resumo_operacional_pg.consolidar_empresa_pg",
                return_value={
                    "receita_operacional": 90000,
                    "cmv": 60000,
                    "despesas_fixas": 10000,
                    "despesas_variaveis": 2000,
                    "despesas_financeiras": 1000,
                    "margem_bruta_pct": 33.33,
                    "markup_pct": 50.0,
                    "cmv_modos": {
                        "ok_vendida": True,
                        "vendida": {"cmv": 60000, "margem_bruta_pct": 33.33, "markup_pct": 50.0},
                        "paga": {"cmv": 55000, "margem_bruta_pct": 38.89, "markup_pct": 63.64},
                    },
                },
            ),
        ):
            out = montar_dre_visual(
                empresa_id=1,
                por="vencimento",
                data_inicio=date(2026, 7, 1),
                data_fim=date(2026, 7, 31),
                empresa_nome="Agro Mais Centro",
                valor="bruto",
            )
        self.assertTrue(out["emprestimos"]["ok"])
        self.assertEqual(out["emprestimos"]["valor_emprestado"], 5000.0)
        self.assertEqual(mock_e.call_args.kwargs["por"], "vencimento")
        self.assertEqual(mock_e.call_args.kwargs["empresa_nome"], "Agro Mais Centro")
        self.assertTrue(out["receita_categorias"]["ok"])
        self.assertEqual(out["receita_categorias"]["fatias"][0]["nome"], "Rações")
        self.assertTrue(out["despesas_categorias"]["ok"])
        self.assertAlmostEqual(out["despesas_categorias"]["total"], 24348.19, places=2)
        self.assertEqual(out["despesas_categorias"]["grupos"][0]["key"], "fixa")
        self.assertTrue(out["comparativo"]["ok"])
        self.assertEqual(out["comparativo"]["mes"]["despesas"], 13000.0)
        self.assertEqual(out["comparativo"]["d90"]["receita"], 90000.0)

    def test_janelas_comparativo(self):
        from datetime import date

        from financeiro.services.dre_visual_util import janela_90d_antes, janela_mes_passado

        self.assertEqual(janela_mes_passado(date(2026, 7, 1)), (date(2026, 6, 1), date(2026, 6, 30)))
        self.assertEqual(janela_mes_passado(date(2026, 7, 12)), (date(2026, 6, 1), date(2026, 6, 30)))
        self.assertEqual(janela_90d_antes(date(2026, 7, 1)), (date(2026, 4, 2), date(2026, 6, 30)))

    def test_snapshot_k_projeta_dias(self):
        from datetime import date

        from financeiro.services.dre_visual_util import (
            _dias_periodo,
            _snapshot_kpis_dre,
            janela_mes_passado,
        )

        dias_atual = _dias_periodo(date(2026, 7, 1), date(2026, 7, 31))
        mes_i, mes_f = janela_mes_passado(date(2026, 7, 1))
        dias_ref = _dias_periodo(mes_i, mes_f)
        snap = _snapshot_kpis_dre(
            {
                "receita_operacional": 31000,
                "cmv": 10000,
                "despesas_fixas": 3000,
                "despesas_variaveis": 0,
                "despesas_financeiras": 0,
            },
            dias_ref=dias_ref,
            dias_atual=dias_atual,
        )
        self.assertEqual(dias_atual, 31)
        self.assertEqual(dias_ref, 30)
        self.assertAlmostEqual(snap["k"], 31 / 30, places=5)
        self.assertEqual(snap["despesas"], 3000.0)
        self.assertAlmostEqual(snap["despesas"] * snap["k"], 3100.0, places=1)

    def test_despesas_categorias_so_periodo(self):
        from datetime import date

        from financeiro.services.dre_visual_util import despesas_categorias_dre_pg

        fake_linhas = {
            "ok": True,
            "linhas": [
                {"plano": "Aluguel", "despesa": 100.0, "receita": 0},
                {"plano": "Comissão", "despesa": 20.0, "receita": 0},
                {"plano": "IOF", "despesa": 5.0, "receita": 0},
                {"plano": "Pagamento de Empréstimos", "despesa": 999.0, "receita": 0},
            ],
        }
        with patch(
            "produtos.lancamentos_financeiro_pg_analytics_util.dre_resumo_simples_pg",
            return_value=fake_linhas,
        ):
            out = despesas_categorias_dre_pg(
                empresa_nome="Agro Mais Centro",
                data_inicio=date(2026, 7, 1),
                data_fim=date(2026, 7, 31),
                por="competencia",
                valor="bruto",
            )
        self.assertTrue(out["ok"])
        self.assertEqual(out["total"], 125.0)
        planos = [r["plano"] for r in out["top"]]
        self.assertNotIn("Pagamento de Empréstimos", planos)
        self.assertEqual(out["grupos"][0]["total"], 100.0)
        self.assertEqual(out["grupos"][2]["key"], "financeira")
        self.assertEqual(out["grupos"][2]["total"], 5.0)


class EmprestimosCardTests(SimpleTestCase):
    def test_classifica(self):
        self.assertTrue(eh_entrada_emprestimo("Entrada de Empréstimo", despesa=False))
        self.assertFalse(eh_entrada_emprestimo("Entrada de Empréstimo", despesa=True))
        self.assertTrue(eh_juros_emprestimo("Juros de Empréstimos", despesa=True))
        self.assertFalse(eh_juros_emprestimo("Juros de Empréstimos", despesa=False))
        self.assertTrue(eh_pagamento_principal_emprestimo("Pagamento de Empréstimos", despesa=True))
        self.assertFalse(eh_pagamento_principal_emprestimo("Juros de Empréstimos", despesa=True))

    def test_resumo_entrada_sempre_competencia(self):
        from datetime import date
        from decimal import Decimal
        from types import SimpleNamespace

        from financeiro.services.dre_emprestimos_util import resumo_emprestimos_pg

        def T(**kw):
            return SimpleNamespace(
                despesa=kw.get("despesa", True),
                quitado=kw.get("quitado", False),
                plano_conta=kw["plano"],
                valor_restante=Decimal(str(kw.get("restante", 0))),
                valor_bruto=Decimal(str(kw.get("bruto", 0))),
                valor_pago=Decimal(str(kw.get("pago", 0))),
                data_competencia=kw.get("comp"),
                data_vencimento=kw.get("venc"),
                data_pagamento=kw.get("pagto"),
            )

        titulos = [
            T(
                plano="Pagamento de Empréstimos",
                restante=1000,
                bruto=1000,
                quitado=False,
                comp=date(2026, 6, 1),
                venc=date(2026, 6, 1),
            ),
            T(
                plano="Pagamento de Empréstimos",
                restante=300,
                bruto=300,
                quitado=False,
                comp=date(2026, 7, 20),
                venc=date(2026, 7, 20),
            ),
            T(
                plano="Pagamento de Empréstimos",
                restante=0,
                bruto=200,
                pago=200,
                quitado=True,
                comp=date(2026, 7, 10),
                venc=date(2026, 7, 10),
                pagto=date(2026, 7, 10),
            ),
            T(
                plano="Juros de Empréstimos",
                restante=0,
                bruto=50,
                pago=50,
                quitado=True,
                comp=date(2026, 7, 10),
                venc=date(2026, 7, 10),
                pagto=date(2026, 7, 10),
            ),
            T(
                plano="Entrada de Empréstimo",
                despesa=False,
                restante=0,
                bruto=5000,
                pago=5000,
                quitado=True,
                comp=date(2026, 7, 5),
                venc=None,
                pagto=date(2026, 7, 5),
            ),
            T(
                plano="Entrada de Empréstimo",
                despesa=False,
                restante=0,
                bruto=9999,
                pago=9999,
                quitado=True,
                comp=date(2026, 6, 1),
                venc=date(2026, 7, 15),
                pagto=date(2026, 7, 15),
            ),
        ]

        class FakeQS:
            def __init__(self, items):
                self.items = list(items)

            def filter(self, *a, **k):
                return FakeQS(self.items)

            def __iter__(self):
                return iter(self.items)

        with (
            patch(
                "financeiro.services.dre_emprestimos_util._qs_empresa",
                return_value=FakeQS(titulos),
            ),
            patch(
                "financeiro.services.dre_emprestimos_util.dedup_titulos",
                side_effect=lambda xs: list(xs),
            ),
        ):
            out = resumo_emprestimos_pg(
                empresa_nome="Agro Mais Centro",
                data_inicio=date(2026, 7, 1),
                data_fim=date(2026, 7, 31),
                por="vencimento",
                valor="bruto",
            )
        self.assertTrue(out["ok"])
        self.assertEqual(out["valor_devido"], 300.0)
        self.assertEqual(out["valor_pago"], 500.0)
        self.assertEqual(out["juros"], 50.0)
        self.assertEqual(out["valor_emprestado"], 5000.0)
        self.assertEqual(out["entrada_por"], "competencia")


class ApiIncluirVisualTests(SimpleTestCase):
    def _call(self, qs, core=None, visual=None):
        factory = APIRequestFactory()
        request = factory.get("/api/financeiro/resumo-operacional", qs)
        request.user = MagicMock(is_authenticated=True)
        view = ResumoOperacionalAPIView.as_view()
        core = core or {
            "receita_operacional": 100.0,
            "geracao_caixa": -10.0,
            "cmv": 40.0,
            "empresa_nome_filtro": "Agro Mais Centro",
        }
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
        self.assertEqual(mock_v.call_args.kwargs.get("empresa_nome"), "Agro Mais Centro")
        self.assertTrue(mock_v.call_args.kwargs.get("data_inicio"))

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


class ReceitaCategoriasPdvTests(SimpleTestCase):
    def test_top_e_outros(self):
        from datetime import date

        from produtos.relatorios_vendas_util import receita_categorias_pdv

        fake = [
            {"grupo": "Rações", "valor": 80},
            {"grupo": "Pet", "valor": 15},
            {"grupo": "A", "valor": 2},
            {"grupo": "B", "valor": 2},
            {"grupo": "C", "valor": 1},
        ]
        with patch(
            "produtos.relatorios_vendas_util.vendas_por_grupo",
            return_value=fake,
        ):
            out = receita_categorias_pdv(date(2026, 7, 1), date(2026, 7, 31), top=2)
        self.assertTrue(out["ok"])
        self.assertEqual(out["total"], 100.0)
        self.assertEqual(out["fatias"][0]["nome"], "Rações")
        self.assertEqual(out["fatias"][-1]["nome"], "Outros")
        self.assertEqual(out["fatias"][-1]["valor"], 5.0)
