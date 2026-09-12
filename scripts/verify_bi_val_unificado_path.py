"""
Verificação BI-VAL-UNIFICADO — card Validade igual em Centro / Vila / C+V.
  .venv\\Scripts\\python.exe scripts/verify_bi_val_unificado_path.py
"""
from __future__ import annotations

import os
import sys
from datetime import date
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

import django

django.setup()

from django.core.cache import cache

from produtos.models import EstoqueLote, ProdutoGestaoOverlayAgro
from produtos.views import (
    VALIDADE_DASHBOARD_CACHE_KEY,
    _contagem_validade_dashboard_empresa,
    _contagem_validade_dashboard_lotes_agro,
    _contagem_validade_dashboard_lotes_agro_compute,
    _invalidar_cache_dashboard_perdas_validade,
)

PREFIX = "VERIFYBIVALU"
HOJE = date(2026, 8, 18)
PASS = FAIL = 0


def ok(msg: str) -> None:
    global PASS
    PASS += 1
    print(f"  OK  {msg}")


def bad(msg: str) -> None:
    global FAIL
    FAIL += 1
    print(f"  FAIL {msg}")


def check(cond: bool, msg: str, detail: str = "") -> None:
    if cond:
        ok(msg + (f" — {detail}" if detail else ""))
    else:
        bad(msg + (f" — {detail}" if detail else ""))


def wipe() -> None:
    ProdutoGestaoOverlayAgro.objects.filter(produto_externo_id__startswith=PREFIX).delete()
    cache.clear()


def test_codigo_cache() -> None:
    print("\n== Código / cache ==")
    src = (ROOT / "produtos/views.py").read_text(encoding="utf-8")
    check("validade_dashboard_lotes_v7" in VALIDADE_DASHBOARD_CACHE_KEY, "cache v7")
    check(
        "baixado_centro_em" in src.split("def _contagem_validade_dashboard_empresa", 1)[-1][:900],
        "compute filtra baixado por loja",
    )
    check("_chave_cache_validade_dashboard" in src, "cache por loja (all/centro/vila)")
    check(
        "_contagem_validade_dashboard_lotes_agro,\n            deposito_filtro"
        in src,
        "dashboard passa deposito_filtro",
    )


def test_tres_filtros_iguais_db() -> None:
    print("\n== DB: Centro / Vila / C+V mesmo KPI ==")
    wipe()
    base_all = _contagem_validade_dashboard_lotes_agro(None)
    base_c = _contagem_validade_dashboard_lotes_agro("centro")
    base_v = _contagem_validade_dashboard_lotes_agro("vila")
    check(base_all == base_c == base_v, "baseline tres iguais", str(base_all))

    ov1 = ProdutoGestaoOverlayAgro.objects.create(
        produto_externo_id=f"{PREFIX}A", nome="Venc A"
    )
    ov2 = ProdutoGestaoOverlayAgro.objects.create(
        produto_externo_id=f"{PREFIX}B", nome="Venc B"
    )
    EstoqueLote.objects.create(
        overlay=ov1,
        lote_codigo="VA",
        data_validade=date(2026, 7, 1),
        quantidade_atual=Decimal("1"),
        deposito="centro",
    )
    EstoqueLote.objects.create(
        overlay=ov2,
        lote_codigo="VB",
        data_validade=date(2026, 8, 25),
        quantidade_atual=Decimal("2"),
        deposito="vila",
    )
    cache.clear()

    all_c = _contagem_validade_dashboard_lotes_agro(None)
    ctr_c = _contagem_validade_dashboard_lotes_agro("centro")
    vil_c = _contagem_validade_dashboard_lotes_agro("vila")
    check(all_c == ctr_c == vil_c, "apos 2 lotes tres iguais", str(all_c))
    check(
        all_c["vencidos"] == base_all["vencidos"] + 1,
        "+1 vencido overlay",
        str(all_c["vencidos"]),
    )
    check(
        all_c["vencendo_mes"] == base_all["vencendo_mes"] + 1,
        "+1 no mes overlay",
        str(all_c["vencendo_mes"]),
    )

    emp = _contagem_validade_dashboard_empresa(HOJE)
    check(all_c == emp, "wrapper = empresa direto", str(emp))
    wipe()


def test_cache_nao_duplica_por_loja() -> None:
    print("\n== Cache: segunda loja não recalcula diferente ==")
    wipe()
    ov = ProdutoGestaoOverlayAgro.objects.create(
        produto_externo_id=f"{PREFIX}C", nome="Cache"
    )
    EstoqueLote.objects.create(
        overlay=ov,
        lote_codigo="C1",
        data_validade=date(2026, 6, 1),
        quantidade_atual=Decimal("1"),
        deposito="centro",
    )
    cache.clear()
    first = _contagem_validade_dashboard_lotes_agro("centro")
    calls = {"n": 0}
    real = _contagem_validade_dashboard_empresa

    def spy(hoje, deposito=None):
        calls["n"] += 1
        return real(hoje, deposito=deposito)

    with patch(
        "produtos.views._contagem_validade_dashboard_empresa", side_effect=spy
    ):
        cache.clear()
        _contagem_validade_dashboard_lotes_agro("centro")
        _contagem_validade_dashboard_lotes_agro("vila")
        third = _contagem_validade_dashboard_lotes_agro(None)
    check(calls["n"] == 3, "tres chaves de cache (centro/vila/all)", str(calls["n"]))
    check(third["vencidos"] == first["vencidos"], "sem baixa os numeros batem", str(third))
    wipe()


def test_lote_qtd_zero_nao_conta() -> None:
    print("\n== Lote qtd 0 não entra ==")
    wipe()
    ov = ProdutoGestaoOverlayAgro.objects.create(
        produto_externo_id=f"{PREFIX}Z", nome="Zero"
    )
    EstoqueLote.objects.create(
        overlay=ov,
        lote_codigo="Z1",
        data_validade=date(2026, 5, 1),
        quantidade_atual=Decimal("0"),
        deposito="centro",
    )
    cache.clear()
    with patch(
        "produtos.estoque_saldo_agro_util.mapa_saldos_operacionais_agro",
        return_value={f"{PREFIX}Z": {"saldo_centro": 0.0, "saldo_vila": 0.0}},
    ):
        c = _contagem_validade_dashboard_lotes_agro(None)
    check(c["vencidos"] == 0, "qtd 0 sem vencido", str(c))
    wipe()


def test_compute_mock_igual() -> None:
    print("\n== Compute mock (sem DB extra) ==")
    a = _contagem_validade_dashboard_lotes_agro_compute(HOJE, None)
    b = _contagem_validade_dashboard_lotes_agro_compute(HOJE, "centro")
    c = _contagem_validade_dashboard_lotes_agro_compute(HOJE, "vila")
    check(a == b == c, "compute mock tres iguais", str(a))


def test_invalidate_perdas() -> None:
    print("\n== Invalidate após baixa (smoke) ==")
    try:
        _invalidar_cache_dashboard_perdas_validade(HOJE)
        ok("invalidar perdas validade")
    except Exception as exc:
        bad(f"invalidar perdas: {exc}")


def main() -> int:
    print("VERIFY BI-VAL-UNIFICADO path")
    test_codigo_cache()
    test_compute_mock_igual()
    test_tres_filtros_iguais_db()
    test_cache_nao_duplica_por_loja()
    test_lote_qtd_zero_nao_conta()
    test_invalidate_perdas()
    print(f"\n== RESULTADO {PASS}/{PASS + FAIL} ==")
    if FAIL:
        print("VERIFY_FAIL")
        return 1
    print("VERIFY_OK bi_val_unificado")
    return 0


if __name__ == "__main__":
    sys.exit(main())
