"""Venda hora a hora — grade pura, sem banco."""
from datetime import date, datetime
from unittest.mock import patch

from django.test import RequestFactory, SimpleTestCase
from django.urls import reverse

from produtos import relatorios_hora_util as hu


def _v(quando, total, deposito="centro", entrega=False, operador="Ana", cliente="João", vid=1):
    return {
        "id": vid,
        "quando": quando,
        "deposito": deposito,
        "total": total,
        "operador": operador,
        "cliente": cliente,
        "entrega": entrega,
    }


QUI = date(2026, 10, 8)


class HoraGradeTests(SimpleTestCase):
    def test_um_dia_separa_loja_entrega_e_pico(self):
        vendas = [
            _v(datetime(2026, 10, 8, 10, 15), 100, "centro", False, "Ana", "João", 1),
            _v(datetime(2026, 10, 8, 10, 40), 50, "vila", True, "Bia", "Maria", 2),
            _v(datetime(2026, 10, 8, 22, 5), 10, "centro", False, "Ana", "Pedro", 3),
        ]
        grade = hu.agregar_hora(
            vendas, [], dias=[QUI], dias_base=[], loja="ambos", canal="todos", visao="soma"
        )
        self.assertEqual(grade["visao"], "soma")
        self.assertEqual(grade["total_bruto"], 160.0)
        h10 = next(ln for ln in grade["expediente"] if ln["hora"] == 10)
        self.assertEqual(h10["centro"], 100.0)
        self.assertEqual(h10["vila"], 50.0)
        self.assertEqual(h10["entrega"], 50.0)
        self.assertEqual(h10["balcao"], 100.0)
        self.assertEqual(h10["quem"], "Ana 1 · Bia 1")
        self.assertEqual(grade["pico"]["hora"], 10)
        self.assertEqual(len(grade["expediente"]), 12)
        self.assertEqual(grade["expediente"][0]["hora"], 7)
        self.assertEqual([ln["hora"] for ln in grade["fora"]], [22])
        h7 = grade["expediente"][0]
        self.assertEqual(h7["total"], 0.0)
        self.assertEqual(h7["quem"], "—")
        self.assertEqual(h7["var_label"], "sem costume")

    def test_so_vila_e_so_balcao(self):
        vendas = [
            _v(datetime(2026, 10, 8, 11, 0), 80, "centro", False, vid=1),
            _v(datetime(2026, 10, 8, 11, 10), 40, "vila", True, vid=2),
            _v(datetime(2026, 10, 8, 11, 20), 20, "vila", False, vid=3),
        ]
        so_vila = hu.agregar_hora(
            vendas, [], dias=[QUI], dias_base=[], loja="vila", canal="todos", visao="soma"
        )
        self.assertEqual(so_vila["total_bruto"], 60.0)
        self.assertFalse(so_vila["duas_lojas"])
        so_balcao = hu.agregar_hora(
            vendas, [], dias=[QUI], dias_base=[], loja="ambos", canal="balcao", visao="soma"
        )
        self.assertEqual(so_balcao["total_bruto"], 100.0)

    def test_media_e_costume_mesmo_dia_da_semana(self):
        dias = [date(2026, 10, 7), date(2026, 10, 8)]
        base_dias = [date(2026, 10, 1), date(2026, 9, 24), date(2026, 9, 17), date(2026, 9, 10)]
        vendas = [
            _v(datetime(2026, 10, 7, 10, 0), 100, vid=1),
            _v(datetime(2026, 10, 8, 10, 0), 300, vid=2),
        ]
        baseline = [_v(datetime(2026, 10, 1, 10, 0), 80, vid=9)]
        media = hu.agregar_hora(
            vendas, baseline, dias=dias, dias_base=base_dias, visao="media"
        )
        h10 = next(ln for ln in media["expediente"] if ln["hora"] == 10)
        self.assertEqual(h10["total"], 200.0)
        self.assertEqual(h10["base"], 20.0)
        self.assertEqual(h10["var_tom"], "acima")
        soma = hu.agregar_hora(
            vendas, baseline, dias=dias, dias_base=base_dias, visao="soma"
        )
        h10s = next(ln for ln in soma["expediente"] if ln["hora"] == 10)
        self.assertEqual(h10s["total"], 400.0)
        self.assertEqual(h10s["base"], 40.0)

    def test_mapa_e_detalhe_da_hora(self):
        vendas = [_v(datetime(2026, 10, 8, 9, 30), 70, "vila", False, "Caio", "Lúcia", 15)]
        grade = hu.agregar_hora(
            vendas,
            [],
            dias=[QUI],
            dias_base=[],
            visao="soma",
            hora_detalhe=9,
            wd_detalhe=3,
        )
        qui = next(linha for linha in grade["mapa"] if linha["wd"] == 3)
        cel = next(c for c in qui["celulas"] if c["hora"] == 9)
        self.assertEqual(cel["valor"], 70.0)
        self.assertGreater(cel["alpha"], 0)
        self.assertEqual(grade["detalhe"]["n"], 1)
        self.assertEqual(grade["detalhe"]["vendas"][0]["cliente"], "Lúcia")
        self.assertEqual(grade["detalhe"]["vendas"][0]["loja"], "Vila")
        self.assertEqual(grade["detalhe"]["vendas"][0]["canal"], "Balcão")
        vazio = hu.agregar_hora(
            vendas, [], dias=[QUI], dias_base=[], hora_detalhe=15, wd_detalhe=3
        )
        self.assertEqual(vazio["detalhe"]["n"], 0)

    def test_costume_quatro_semanas_e_periodo(self):
        dias = hu.listar_dias(QUI, QUI)
        base = hu.dias_costume(dias)
        self.assertEqual(len(base), 4)
        self.assertTrue(all(d.weekday() == QUI.weekday() for d in base))
        self.assertEqual(base[-1], date(2026, 10, 1))
        req = RequestFactory().get("/", {"periodo": "ontem", "deposito": "vila", "canal": "entrega"})
        with patch("produtos.relatorios_hora_util.timezone.localdate", return_value=QUI):
            filtros = hu.parse_periodo_hora(req)
        self.assertEqual(filtros["periodo"], "ontem")
        self.assertEqual(filtros["d0"], date(2026, 10, 7))
        self.assertEqual(hu.parse_loja_hora(req), "vila")
        self.assertEqual(hu.parse_canal_hora(req), "entrega")
        self.assertEqual(hu.parse_visao_hora(req, 1), "soma")
        self.assertEqual(hu.parse_visao_hora(RequestFactory().get("/", {"visao": "soma"}), 5), "soma")

    def test_devolucao_nao_conta_cupom(self):
        vendas = [
            _v(datetime(2026, 10, 8, 10, 0), 100, vid=1),
            {
                "id": 1,
                "quando": datetime(2026, 10, 8, 16, 0),
                "deposito": "centro",
                "total": -30,
                "operador": "Ana",
                "cliente": "João",
                "entrega": False,
                "devolucao": True,
            },
        ]
        grade = hu.agregar_hora(vendas, [], dias=[QUI], dias_base=[], visao="soma")
        h10 = next(ln for ln in grade["expediente"] if ln["hora"] == 10)
        h16 = next(ln for ln in grade["expediente"] if ln["hora"] == 16)
        self.assertEqual(h10["total"], 100.0)
        self.assertEqual(h10["n_bruto"], 1)
        self.assertEqual(h16["total"], -30.0)
        self.assertEqual(h16["n_bruto"], 0)
        self.assertEqual(grade["total_bruto"], 70.0)
        self.assertEqual(grade["n_vendas"], 1)
        req = RequestFactory().get("/", {"periodo": "custom", "de": "2020-01-01", "ate": "2026-10-08"})
        filtros = hu.parse_periodo_hora(req)
        self.assertLessEqual((filtros["d1"] - filtros["d0"]).days + 1, hu.PERIODO_MAX_DIAS)
        self.assertTrue(filtros["aviso"])


class RelatoriosHoraViewTests(SimpleTestCase):
    def test_pagina_e_excel(self):
        with patch("produtos.relatorios_hora_util.carregar_vendas_intervalo", return_value=[]), patch(
            "produtos.relatorios_hora_util.meta_card_hora", return_value=None
        ):
            html = self.client.get(reverse("relatorios_hora"))
            self.assertEqual(html.status_code, 200)
            body = html.content.decode("utf-8", errors="ignore")
            self.assertIn("Venda hora a hora", body)
            self.assertIn("Centro + Vila", body)
            self.assertIn("Só Vila", body)
            self.assertIn("Mapa da semana", body)
            xlsx = self.client.get(reverse("relatorios_hora") + "?export=xlsx&deposito=centro")
            self.assertEqual(xlsx.status_code, 200)
            self.assertIn("spreadsheetml", xlsx["Content-Type"])
            self.assertTrue(xlsx.content[:2] == b"PK")
