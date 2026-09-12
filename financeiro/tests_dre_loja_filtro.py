"""Filtro de loja DRE/BI: Centro + Vila (padrão), Centro, Vila."""
from __future__ import annotations

from datetime import date
from decimal import Decimal
from unittest.mock import patch

from django.test import SimpleTestCase

from financeiro.services.receita_pdv_util import (
    aplicar_receita_pdv_no_resumo,
    deposito_de_loja,
    deposito_pdv_efetivo,
    label_loja_filtro,
    normalizar_loja_filtro,
    resolver_deposito_pdv,
)


class NormalizarLojaFiltroTests(SimpleTestCase):
    def test_padrao_todas(self):
        self.assertEqual(normalizar_loja_filtro(None), "todas")
        self.assertEqual(normalizar_loja_filtro(""), "todas")
        self.assertEqual(normalizar_loja_filtro("ambas"), "todas")

    def test_centro_vila(self):
        self.assertEqual(normalizar_loja_filtro("centro"), "centro")
        self.assertEqual(normalizar_loja_filtro("1"), "centro")
        self.assertEqual(normalizar_loja_filtro("vila"), "vila")
        self.assertEqual(normalizar_loja_filtro("2"), "vila")

    def test_deposito_e_label(self):
        self.assertIsNone(deposito_de_loja("todas"))
        self.assertEqual(deposito_de_loja("centro"), "centro")
        self.assertEqual(deposito_de_loja("vila"), "vila")
        self.assertEqual(label_loja_filtro("todas"), "Centro + Vila")
        self.assertEqual(label_loja_filtro("centro"), "Centro")
        self.assertEqual(label_loja_filtro("vila"), "Vila Elias")


class ResolverDepositoPdvTests(SimpleTestCase):
    def test_nome_centro_sozinho_vira_centro(self):
        self.assertEqual(resolver_deposito_pdv(None, "Agro Mais Centro"), "centro")

    def test_todas_ignora_nome_centro(self):
        self.assertIsNone(resolver_deposito_pdv("todas", "Agro Mais Centro"))

    def test_vila_explicito(self):
        self.assertEqual(resolver_deposito_pdv("vila", "Agro Mais Centro"), "vila")

    def test_efetivo_uma_empresa_todas(self):
        self.assertEqual(
            deposito_pdv_efetivo(empresa_id=1, deposito_filtro=None, n_empresas=1),
            "todas",
        )

    def test_efetivo_filtro_centro(self):
        self.assertEqual(
            deposito_pdv_efetivo(empresa_id=1, deposito_filtro="centro", n_empresas=1),
            "centro",
        )


class AplicarReceitaTodasLojasTests(SimpleTestCase):
    def test_todas_chama_pdv_sem_deposito(self):
        core = {
            "receita_operacional": Decimal("0"),
            "cmv": Decimal("10"),
            "despesas_fixas": Decimal("0"),
            "despesas_variaveis": Decimal("0"),
            "despesas_financeiras": Decimal("0"),
        }
        with patch(
            "financeiro.services.receita_pdv_util.faturamento_pdv_periodo",
            return_value={"ok": True, "total": Decimal("100"), "por_dia": {}},
        ) as mock_fat:
            out = aplicar_receita_pdv_no_resumo(
                core,
                date(2026, 7, 1),
                date(2026, 7, 31),
                empresa_nome="Agro Mais Centro",
                deposito="todas",
            )
        self.assertEqual(out["receita_operacional"], Decimal("100"))
        self.assertIsNone(mock_fat.call_args.kwargs.get("deposito"))


class SerializerLojaTests(SimpleTestCase):
    def test_loja_todas_sem_modo(self):
        from financeiro.api.serializers import ResumoOperacionalQuerySerializer

        s = ResumoOperacionalQuerySerializer(
            data={
                "loja": "todas",
                "data_inicio": "2026-07-01",
                "data_fim": "2026-07-31",
            }
        )
        self.assertTrue(s.is_valid(), s.errors)
        self.assertEqual(s.validated_data["modo"], "lojas")
        self.assertEqual(s.validated_data["loja"], "todas")

    def test_modo_empresa_ainda_vale(self):
        from financeiro.api.serializers import ResumoOperacionalQuerySerializer

        s = ResumoOperacionalQuerySerializer(
            data={
                "modo": "empresa",
                "empresa_id": 1,
                "data_inicio": "2026-07-01",
                "data_fim": "2026-07-31",
            }
        )
        self.assertTrue(s.is_valid(), s.errors)


class ConsolidarPorLojaTests(SimpleTestCase):
    def test_uma_empresa_todas_passa_deposito_todas(self):
        from financeiro.services.resumo_operacional_pg import consolidar_por_loja_pg

        vistos = []

        def fake_consol(**kwargs):
            vistos.append(kwargs.get("deposito"))
            return {
                "receita_operacional": Decimal("10"),
                "cmv": Decimal("1"),
                "despesas_fixas": Decimal("0"),
                "despesas_variaveis": Decimal("0"),
                "despesas_financeiras": Decimal("0"),
                "resultado_operacional": Decimal("9"),
                "resultado_liquido_gerencial": Decimal("9"),
                "empresa_id": kwargs.get("empresa_id"),
                "empresa_nome_filtro": "Agro Mais Centro",
            }

        with (
            patch(
                "financeiro.services.receita_pdv_util.empresas_ids_para_deposito",
                return_value=[1],
            ),
            patch(
                "financeiro.services.resumo_operacional_pg.consolidar_empresa_pg",
                side_effect=fake_consol,
            ),
        ):
            out = consolidar_por_loja_pg(
                loja="todas",
                data_inicio=date(2026, 7, 1),
                data_fim=date(2026, 7, 31),
                anexar_cmv_modos=False,
            )
        self.assertEqual(out.get("loja"), "todas")
        self.assertEqual(out.get("loja_label"), "Centro + Vila")
        self.assertEqual(vistos, ["todas"])
