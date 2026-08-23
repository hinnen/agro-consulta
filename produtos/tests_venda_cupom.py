"""Cupom 80mm — número da venda visível e data com segundos."""

from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from django.test import SimpleTestCase
from django.utils import timezone

from produtos.venda_cupom_util import _formatar_data_venda


class FormatacaoDataCupomTests(SimpleTestCase):
    def test_data_cupom_inclui_segundos(self):
        dt = timezone.make_aware(
            datetime(2026, 7, 16, 20, 3, 53),
            ZoneInfo("America/Sao_Paulo"),
        )
        txt = _formatar_data_venda(dt)
        self.assertEqual(txt, "16/07/2026 20:03:53")
