"""Títulos fiado incluem todas as linhas (frete / 2ª fatia), não só a primeira."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from types import SimpleNamespace
from zoneinfo import ZoneInfo

from django.test import SimpleTestCase

from produtos.fiado_credito_util import montar_cronograma_fiado, valor_fiado_venda_local
from produtos.fiado_gestao_util import _dec, _parcelas_fiado_para_titulos


def _venda_duas_linhas(*, produtos="399.50", frete="10.00", cron_so_primeira=True):
    p = Decimal(produtos)
    f = Decimal(frete)
    cron_p = montar_cronograma_fiado(p, 1, 30)
    cron_f = montar_cronograma_fiado(f, 1, 30)
    return SimpleNamespace(
        pagamentos_json=[
            {
                "forma": "Fiado",
                "valor": float(p),
                "fiado_parcelas": 1,
                "fiado_dias_primeiro": 30,
                "fiado_cronograma": cron_p,
            },
            {
                "forma": "Fiado",
                "valor": float(f),
                "fiado_parcelas": 1,
                "fiado_dias_primeiro": 30,
                "fiado_cronograma": cron_f,
            },
        ],
        fiado_cronograma_json=cron_p if cron_so_primeira else cron_p + cron_f,
        criado_em=datetime(2026, 7, 16, 17, 4, 32, tzinfo=ZoneInfo("America/Sao_Paulo")),
        forma_pagamento="Fiado + Fiado",
        total=p + f,
        cliente_id_erp="",
    )


class ParcelasFiadoFreteTests(SimpleTestCase):
    def test_valor_fiado_soma_as_duas_linhas(self):
        v = _venda_duas_linhas()
        self.assertEqual(valor_fiado_venda_local(v), Decimal("409.50"))

    def test_parcelas_incluem_frete_mesmo_com_cronograma_so_da_primeira(self):
        v = _venda_duas_linhas()
        self.assertEqual(sum((_dec(p["valor"]) for p in (v.fiado_cronograma_json or [])), Decimal("0")), Decimal("399.50"))
        parcelas = _parcelas_fiado_para_titulos(v)
        self.assertEqual(len(parcelas), 2)
        soma = sum((_dec(p["valor"]) for p in parcelas), Decimal("0"))
        self.assertEqual(soma, Decimal("409.50"))
        valores = sorted(_dec(p["valor"]) for p in parcelas)
        self.assertEqual(valores, [Decimal("10.00"), Decimal("399.50")])

    def test_complementa_se_pagamentos_somam_mais_que_o_cronograma(self):
        cron = montar_cronograma_fiado(Decimal("399.50"), 1, 30)
        v = SimpleNamespace(
            pagamentos_json=[
                {"forma": "Fiado", "valor": 399.50},
                {"forma": "Fiado", "valor": 10.00},
            ],
            fiado_cronograma_json=cron,
            criado_em=datetime(2026, 7, 16, 17, 4, 32, tzinfo=ZoneInfo("America/Sao_Paulo")),
            forma_pagamento="Fiado + Fiado",
            total=Decimal("409.50"),
            cliente_id_erp="",
        )
        parcelas = _parcelas_fiado_para_titulos(v)
        soma = sum((_dec(p["valor"]) for p in parcelas), Decimal("0"))
        self.assertEqual(soma, Decimal("409.50"))

    def test_uma_linha_ja_com_frete_no_valor(self):
        cron = montar_cronograma_fiado(Decimal("409.50"), 1, 30)
        v = SimpleNamespace(
            pagamentos_json=[{"forma": "Fiado", "valor": 409.50, "fiado_cronograma": cron}],
            fiado_cronograma_json=cron,
            criado_em=datetime(2026, 7, 16, 17, 4, 32, tzinfo=ZoneInfo("America/Sao_Paulo")),
            forma_pagamento="Fiado",
            total=Decimal("409.50"),
            cliente_id_erp="",
        )
        parcelas = _parcelas_fiado_para_titulos(v)
        self.assertEqual(len(parcelas), 1)
        self.assertEqual(_dec(parcelas[0]["valor"]), Decimal("409.50"))
