"""BI: devolução desconta no dia do evento, não some a venda original."""
from __future__ import annotations

from datetime import date, datetime, timedelta
from decimal import Decimal

from django.core.cache import cache
from django.test import SimpleTestCase, TestCase, override_settings
from django.utils import timezone

from produtos.models import DevolucaoVendaAgro, VendaAgro


@override_settings(
    CACHES={"default": {"BACKEND": "django.core.cache.backends.locmem.LocMemCache"}}
)
class BiDevolucaoDiaEventoTests(TestCase):
    def setUp(self):
        cache.clear()
        self.hoje = timezone.localdate()
        self.ontem = self.hoje - timedelta(days=1)

    def _venda(self, total, *, dia: date, deposito="centro", devolvida=False):
        tz = timezone.get_current_timezone()
        v = VendaAgro.objects.create(
            cliente_nome="T",
            total=Decimal(str(total)),
            deposito=deposito,
        )
        dt = timezone.make_aware(datetime.combine(dia, datetime.min.time().replace(hour=12)), tz)
        v.criado_em = dt
        campos = ["criado_em"]
        if devolvida:
            v.devolvida_em = timezone.make_aware(
                datetime.combine(self.hoje, datetime.min.time().replace(hour=15)), tz
            )
            campos.append("devolvida_em")
        v.save(update_fields=campos)
        return v

    def _dev(self, venda, total, *, dia: date):
        ev = DevolucaoVendaAgro.objects.create(venda=venda, total=Decimal(str(total)))
        tz = timezone.get_current_timezone()
        ev.criado_em = timezone.make_aware(
            datetime.combine(dia, datetime.min.time().replace(hour=16)), tz
        )
        ev.save(update_fields=["criado_em"])
        return ev

    def test_devolucao_hoje_de_venda_ontem_cai_hoje(self):
        from produtos.views import _dashboard_vendas_serie_pdv

        self._venda(100, dia=self.ontem)
        v = self._venda(40, dia=self.ontem)
        self._dev(v, 40, dia=self.hoje)
        self._venda(25, dia=self.hoje)

        ser_hoje = _dashboard_vendas_serie_pdv(self.hoje, self.hoje)
        self.assertAlmostEqual(ser_hoje["total"], -15.0, places=2)

        ser_ontem = _dashboard_vendas_serie_pdv(self.ontem, self.ontem)
        self.assertAlmostEqual(ser_ontem["total"], 140.0, places=2)

    def test_devolucao_total_mesmo_dia_zera_venda_e_soma_nova(self):
        from produtos.views import _dashboard_vendas_serie_pdv

        v = self._venda(80, dia=self.hoje, devolvida=True)
        self._dev(v, 80, dia=self.hoje)
        self._venda(50, dia=self.hoje)
        ser = _dashboard_vendas_serie_pdv(self.hoje, self.hoje)
        self.assertAlmostEqual(ser["total"], 50.0, places=2)


class BiDevolucaoMathTests(SimpleTestCase):
    def test_aplicar_abatimento(self):
        from produtos.dashboard_pdv_devolucao_util import aplicar_abatimento_por_dia

        out = aplicar_abatimento_por_dia(
            {"2026-09-01": 25.0}, {"2026-09-01": Decimal("40.00")}
        )
        self.assertEqual(out["2026-09-01"], -15.0)
