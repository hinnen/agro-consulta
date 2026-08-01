"""
Prova isolada (sem DB loja): card Validade BI por loja.
  .venv\\Scripts\\python.exe scripts/verify_validade_bi.py
"""
from __future__ import annotations

import os
import sys
from datetime import date
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
import django

django.setup()

from produtos.views import (
    _bounds_mes_atual,
    _contagem_validade_dashboard_por_loja,
)

HOJE = date(2026, 8, 1)
fails: list[str] = []
oks: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    if cond:
        oks.append(name)
        print(f"  OK  {name}" + (f" — {detail}" if detail else ""))
    else:
        fails.append(name)
        print(f"  FAIL {name}" + (f" — {detail}" if detail else ""))


def _qs_values(rows: list[dict]):
    m = MagicMock()
    m.filter.return_value = m
    m.values.return_value = rows
    m.values_list.return_value = [r["overlay_id"] for r in rows]
    m.distinct.return_value = m
    return m


def run_por_loja(
    *,
    lotes_qtd: list[dict],
    lotes_zero: list[dict] | None = None,
    overlays_extras: list | None = None,
    overlays_com_lote_ids: set[int] | None = None,
    saldos: dict,
    deposito: str,
):
    """Simula queries do path por loja."""
    lotes_zero = lotes_zero or []
    overlays_extras = overlays_extras or []
    if overlays_com_lote_ids is None:
        overlays_com_lote_ids = {r["overlay_id"] for r in lotes_qtd + lotes_zero}

    base_qtd = MagicMock()
    base_qtd.values.return_value = lotes_qtd

    qs_all = MagicMock()

    def filter_side(**kw):
        if kw.get("quantidade_atual__gt") == 0 or (
            "quantidade_atual__gt" in kw and kw["quantidade_atual__gt"] == 0
        ):
            # not used that way
            pass
        if "quantidade_atual__gt" in kw:
            m = MagicMock()
            m.values.return_value = lotes_qtd
            return m
        if "quantidade_atual__lte" in kw:
            m = MagicMock()
            m.values.return_value = lotes_zero
            return m
        return qs_all

    estoque_m = MagicMock()
    estoque_m.filter.side_effect = filter_side
    estoque_m.values_list.return_value = MagicMock(
        distinct=MagicMock(return_value=list(overlays_com_lote_ids))
    )
    # filter(quantidade_atual__gt=0) used as base_qtd = EstoqueLote.objects.filter(...)
    def objects_filter(**kw):
        if kw == {"quantidade_atual__gt": 0}:
            m = MagicMock()
            m.values.return_value = lotes_qtd
            return m
        if kw == {"quantidade_atual__lte": 0}:
            m = MagicMock()
            m.values.return_value = lotes_zero
            return m
        return MagicMock()

    estoque_m.filter.side_effect = objects_filter
    estoque_m.values_list.return_value.distinct.return_value = list(
        overlays_com_lote_ids
    )

    ov_m = MagicMock()
    chain = MagicMock()
    chain.only.return_value = overlays_extras
    ov_m.filter.return_value = chain

    with (
        patch("produtos.views.EstoqueLote.objects", estoque_m),
        patch("produtos.views.ProdutoGestaoOverlayAgro.objects", ov_m),
        patch(
            "produtos.estoque_saldo_agro_util.mapa_saldos_operacionais_agro",
            return_value=saldos,
        ),
    ):
        return _contagem_validade_dashboard_por_loja(HOJE, deposito)


def main() -> int:
    print("=== Datas (print Renan 01/08) ===")
    a, b = _bounds_mes_atual(HOJE)
    check("mes_agosto", a == date(2026, 8, 1) and b == date(2026, 8, 31))
    check("30/07_vencido", (date(2026, 7, 30) - HOJE).days == -2)
    check("30/08_em_29d", (date(2026, 8, 30) - HOJE).days == 29)

    print("\n=== Bug card 0/0: lote qtd + C+V operacional 0 ===")
    rows = [
        {
            "overlay_id": 1,
            "data_validade": date(2026, 7, 30),
            "overlay__produto_externo_id": "P1",
        },
        {
            "overlay_id": 2,
            "data_validade": date(2026, 8, 30),
            "overlay__produto_externo_id": "P2",
        },
    ]
    saldos0 = {
        "P1": {"saldo_centro": 0.0, "saldo_vila": 0.0},
        "P2": {"saldo_centro": 0.0, "saldo_vila": 0.0},
    }
    c = run_por_loja(lotes_qtd=rows, saldos=saldos0, deposito="centro")
    v = run_por_loja(lotes_qtd=rows, saldos=saldos0, deposito="vila")
    check("centro_vencidos_1", c["vencidos"] == 1, str(c))
    check("centro_mes_1", c["vencendo_mes"] == 1, str(c))
    check("vila_vencidos_1", v["vencidos"] == 1, str(v))
    check("vila_mes_1", v["vencendo_mes"] == 1, str(v))

    print("\n=== Duas datas mesmo overlay ===")
    multi = [
        {
            "overlay_id": 9,
            "data_validade": date(2027, 1, 1),
            "overlay__produto_externo_id": "PM",
        },
        {
            "overlay_id": 9,
            "data_validade": date(2026, 7, 30),
            "overlay__produto_externo_id": "PM",
        },
        {
            "overlay_id": 9,
            "data_validade": date(2026, 8, 15),
            "overlay__produto_externo_id": "PM",
        },
    ]
    cm = run_por_loja(
        lotes_qtd=multi,
        saldos={"PM": {"saldo_centro": 0.0, "saldo_vila": 0.0}},
        deposito="centro",
    )
    check("multi_vencido_e_mes", cm["vencidos"] == 1 and cm["vencendo_mes"] == 1, str(cm))

    print("\n=== Saldo só Vila: Centro não conta ===")
    row_v = [
        {
            "overlay_id": 3,
            "data_validade": date(2026, 7, 30),
            "overlay__produto_externo_id": "PV",
        }
    ]
    saldos_v = {"PV": {"saldo_centro": 0.0, "saldo_vila": 5.0}}
    cc = run_por_loja(lotes_qtd=row_v, saldos=saldos_v, deposito="centro")
    vv = run_por_loja(lotes_qtd=row_v, saldos=saldos_v, deposito="vila")
    check("centro_ignora_estoque_vila", cc["vencidos"] == 0, str(cc))
    check("vila_conta_estoque_vila", vv["vencidos"] == 1, str(vv))

    print("\n=== Saldo só Centro ===")
    saldos_c = {"PV": {"saldo_centro": 2.0, "saldo_vila": 0.0}}
    cc2 = run_por_loja(lotes_qtd=row_v, saldos=saldos_c, deposito="centro")
    vv2 = run_por_loja(lotes_qtd=row_v, saldos=saldos_c, deposito="vila")
    check("centro_com_saldo", cc2["vencidos"] == 1, str(cc2))
    check("vila_sem_saldo", vv2["vencidos"] == 0, str(vv2))

    print("\n=== Mapa vazio: ainda conta lote ===")
    cm2 = run_por_loja(lotes_qtd=row_v, saldos={}, deposito="centro")
    check("mapa_vazio", cm2["vencidos"] == 1, str(cm2))

    print("\n=== Lote qtd 0 não entra em lotes_qtd ===")
    cz = run_por_loja(
        lotes_qtd=[],
        lotes_zero=[
            {
                "overlay_id": 4,
                "data_validade": date(2026, 7, 30),
                "overlay__produto_externo_id": "PZ",
            }
        ],
        saldos={"PZ": {"saldo_centro": 0.0, "saldo_vila": 0.0}},
        deposito="centro",
    )
    check("lote_zero_nao_conta", cz["vencidos"] == 0 and cz["vencendo_mes"] == 0, str(cz))

    print("\n=== Extras validade sem lote + saldo centro ===")
    ex = SimpleNamespace(
        pk=50,
        produto_externo_id="PEX",
        cadastro_extras={"validade": "2026-08-20"},
    )
    cx = run_por_loja(
        lotes_qtd=[],
        lotes_zero=[],
        overlays_com_lote_ids=set(),
        overlays_extras=[ex],
        saldos={"PEX": {"saldo_centro": 1.0, "saldo_vila": 0.0}},
        deposito="centro",
    )
    check("extras_com_saldo_centro", cx["vencendo_mes"] == 1, str(cx))

    print(f"\n=== Resultado: {len(oks)} OK · {len(fails)} FAIL ===")
    if fails:
        print("FALHAS:", ", ".join(fails))
        return 1
    print("VERIFY_OK validade BI")
    return 0


if __name__ == "__main__":
    sys.exit(main())
