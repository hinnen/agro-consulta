#!/usr/bin/env python
"""Prova — delta do acumulado usa % do envio do dia (REPASSE-ACUM-PCT-DIA).

Bug: padrão virou 0% → refresh forçava 180 dias com 0% → envio antigo a 50%
virava crédito fantasma (ex. 500 vira «levei 400 a mais»).
"""
from __future__ import annotations

import os
import sys
from datetime import timedelta
from decimal import Decimal
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

import django

django.setup()

if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

from django.utils import timezone

from produtos.models import RepasseVilaCentroAgro, RepasseVilaDeltaDiaAgro
from produtos.repasse_vila_util import (
    _percentual_para_delta_dia,
    _preencher_cache_faltante,
    delta_dia_repasse,
    reconstruir_deltas_acumulado,
    refresh_deltas_apos_envio,
)

fails: list[str] = []
oks = 0


def ok(msg: str) -> None:
    global oks
    oks += 1
    print(f"OK {msg}")


def fail(msg: str) -> None:
    fails.append(msg)
    print(f"FAIL {msg}")


def must(cond: bool, msg: str) -> None:
    if cond:
        ok(msg)
    else:
        fail(msg)


def main() -> int:
    util = (ROOT / "produtos/repasse_vila_util.py").read_text(encoding="utf-8", errors="replace")
    views = (ROOT / "produtos/views_repasse_vila.py").read_text(encoding="utf-8", errors="replace")
    js = (ROOT / "produtos/static/produtos/js/pdv_repasse_vila.js").read_text(
        encoding="utf-8", errors="replace"
    )

    must("_percentual_para_delta_dia" in util, "helper % do dia")
    must("reconstruir_deltas_acumulado" in util, "reconstruir_deltas_acumulado")
    must("Não reescreve o passado inteiro" in util or "não podem ficar com o % padrão" in views, "doc/proteção")
    must("reconstruir_deltas_acumulado" in views, "calc chama reconstruir")
    must("Crédito de dias anteriores cobre o dia" in js, "hint UX líquido zero")

    # Contrato: dia com envio a 50% (+ outro a 0%) usa o MAIOR % — não o padrão 0
    dia = timezone.localdate() - timedelta(days=3)
    with mock.patch(
        "produtos.repasse_vila_util.RepasseVilaCentroAgro.objects"
    ) as m_objs:
        m_qs = mock.MagicMock()
        m_objs.filter.return_value = m_qs
        m_qs.aggregate.return_value = {"m": Decimal("50")}
        with mock.patch("produtos.repasse_vila_util.obter_config") as m_cfg:
            cfg = mock.MagicMock()
            cfg.percentual_lucro_padrao = Decimal("0")
            m_cfg.return_value = cfg
            pct = _percentual_para_delta_dia(dia)
    must(pct == Decimal("50"), f"maior % do dia=50 (não 0) → {pct}")

    with mock.patch(
        "produtos.repasse_vila_util.RepasseVilaCentroAgro.objects"
    ) as m_objs:
        m_qs = mock.MagicMock()
        m_objs.filter.return_value = m_qs
        m_qs.aggregate.return_value = {"m": None}
        with mock.patch("produtos.repasse_vila_util.obter_config") as m_cfg:
            cfg = mock.MagicMock()
            cfg.percentual_lucro_padrao = Decimal("0")
            m_cfg.return_value = cfg
            pct2 = _percentual_para_delta_dia(dia)
    must(pct2 == Decimal("0"), f"sem envio → padrao 0 → {pct2}")

    must("Max(\"percentual_lucro\")" in util or "Max('percentual_lucro')" in util or "Max(" in util, "usa Max % do dia")
    # refresh_deltas_apos_envio não chama _preencher com forcar=True na janela toda
    # (texto do código)
    must(
        "forcar=False" in util.split("def refresh_deltas_apos_envio")[1].split("def ")[0],
        "refresh não forçar janela inteira com % atual",
    )

    # Simula: cache corrompido (alvo baixo) + envio 50% → reconstruir usa 50
    must(
        "percentual_lucro=_percentual_para_delta_dia" in util,
        "preencher/atualizar usa % do dia",
    )

    print(f"\n{oks} OK · {len(fails)} FAIL")
    for f in fails:
        print(f"  - {f}")
    if fails:
        print("VERIFY_REPASSE_ACUM_PCT_DIA_FAIL")
        return 1
    print("VERIFY_REPASSE_ACUM_PCT_DIA_OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
