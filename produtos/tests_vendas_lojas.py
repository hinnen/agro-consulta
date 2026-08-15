"""Provas do painel Vendas das lojas (Centro × Vila + soma)."""
from __future__ import annotations

from datetime import date, datetime, time, timedelta
from decimal import Decimal

from django.contrib.auth import get_user_model
from django.test import SimpleTestCase, TestCase, override_settings
from django.urls import reverse
from django.utils import timezone

from produtos.models import VendaAgro
from produtos.vendas_lojas_util import payload_vendas_lojas, resolver_periodo_vendas_lojas


def _marcar_dia(venda: VendaAgro, dia: date) -> None:
    dt = timezone.make_aware(datetime.combine(dia, time(12, 0)))
    VendaAgro.objects.filter(pk=venda.pk).update(criado_em=dt)


class PeriodoVendasLojasTests(SimpleTestCase):
    def test_padrao_e_dia_atual(self):
        hoje = date(2026, 8, 15)
        p = resolver_periodo_vendas_lojas(modo=None, ref=None, hoje=hoje)
        self.assertEqual(p["modo"], "dia")
        self.assertEqual(p["data_ini"], hoje)
        self.assertEqual(p["data_fim"], hoje)
        self.assertFalse(p["pode_avancar"])

    def test_semana_mes_ano(self):
        hoje = date(2026, 8, 15)
        sem = resolver_periodo_vendas_lojas(modo="semana", ref=hoje, hoje=hoje)
        self.assertEqual(sem["data_ini"], date(2026, 8, 10))
        self.assertEqual(sem["data_fim"], date(2026, 8, 16))
        mes = resolver_periodo_vendas_lojas(modo="mes", ref=hoje, hoje=hoje)
        self.assertEqual(mes["data_ini"], date(2026, 8, 1))
        self.assertEqual(mes["data_fim"], date(2026, 8, 31))
        ano = resolver_periodo_vendas_lojas(modo="ano", ref=hoje, hoje=hoje)
        self.assertEqual(ano["data_ini"], date(2026, 1, 1))
        self.assertEqual(ano["data_fim"], date(2026, 12, 31))


@override_settings(
    CACHES={"default": {"BACKEND": "django.core.cache.backends.locmem.LocMemCache"}}
)
class TotaisVendasLojasTests(TestCase):
    def setUp(self):
        self.hoje = date(2026, 8, 15)
        c = VendaAgro.objects.create(total=Decimal("100.50"), deposito="centro")
        v = VendaAgro.objects.create(total=Decimal("40.25"), deposito="vila")
        legado = VendaAgro.objects.create(total=Decimal("10.00"), deposito="")
        dev = VendaAgro.objects.create(
            total=Decimal("999.00"),
            deposito="centro",
            devolvida_em=timezone.now(),
        )
        ontem = VendaAgro.objects.create(total=Decimal("7.00"), deposito="vila")
        _marcar_dia(c, self.hoje)
        _marcar_dia(v, self.hoje)
        _marcar_dia(legado, self.hoje)
        _marcar_dia(dev, self.hoje)
        _marcar_dia(ontem, self.hoje - timedelta(days=1))

    def test_soma_hoje_exclui_devolvida_e_ontem(self):
        p = payload_vendas_lojas(modo="dia", ref=self.hoje, hoje=self.hoje)
        self.assertEqual(p["centro"], 110.5)
        self.assertEqual(p["vila"], 40.25)
        self.assertEqual(p["soma"], 150.75)
        self.assertEqual(p["periodo"], "dia")

    def test_semana_inclui_ontem(self):
        p = payload_vendas_lojas(modo="semana", ref=self.hoje, hoje=self.hoje)
        self.assertEqual(p["vila"], 47.25)
        self.assertEqual(p["soma"], 157.75)


@override_settings(
    CACHES={"default": {"BACKEND": "django.core.cache.backends.locmem.LocMemCache"}}
)
class VendasLojasViewTests(TestCase):
    def setUp(self):
        User = get_user_model()
        self.user = User.objects.create_user("renan-vl", password="x")
        self.client.force_login(self.user)

    def test_abre_no_dia_e_json(self):
        r = self.client.get(reverse("vendas_lojas"))
        self.assertEqual(r.status_code, 200)
        self.assertContains(r, "Vendas das lojas")
        self.assertContains(r, "Soma das duas")
        body = r.content.decode("utf-8")
        self.assertIn("periodo=dia", body)
        j = self.client.get(reverse("vendas_lojas") + "?fmt=json")
        self.assertEqual(j.status_code, 200)
        data = j.json()
        self.assertTrue(data["ok"])
        self.assertEqual(data["periodo"], "dia")
        self.assertIn("centro_fmt", data)
        self.assertIn("soma_fmt", data)
