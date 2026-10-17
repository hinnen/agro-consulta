"""
Verificacao BI-VAL-BAIXA-LOJA — baixa no Centro nao zera a Vila.
  .venv\\Scripts\\python.exe scripts/verify_bi_val_baixa_loja_path.py
"""
from __future__ import annotations

import json
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

from django.contrib.auth.models import User
from django.core.cache import cache
from django.test import RequestFactory
from django.utils import timezone

from produtos.models import EstoqueLote, ProdutoGestaoOverlayAgro
from produtos.views import (
    VALIDADE_DASHBOARD_CACHE_KEY,
    _contagem_validade_dashboard_lotes_agro,
    api_relatorio_validade_baixa,
)

PREFIX = "VERIFYBIVALBX"
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


def test_fonte() -> None:
    print("\n== Codigo ==")
    src = (ROOT / "produtos/views.py").read_text(encoding="utf-8")
    check("v7" in VALIDADE_DASHBOARD_CACHE_KEY, "cache v7")
    check("somente_deposito" in src, "baixa estoque so da loja")
    check("baixado_centro_em" in src and "baixado_vila_em" in src, "flags por loja")
    check("A outra loja continua vendo" in src, "mensagem outra loja")
    tpl = (ROOT / "produtos/templates/produtos/relatorios_validade.html").read_text(
        encoding="utf-8"
    )
    check('deposito: "{{ deposito_baixa|escapejs }}"' in tpl, "JS envia deposito")
    check("Dar baixa só nesta loja" in tpl, "confirm so nesta loja")
    mig = ROOT / "produtos/migrations/0096_estoque_lote_baixa_por_loja.py"
    check(mig.is_file(), "migrate 0096")


def test_contagem_apos_baixa_centro() -> None:
    print("\n== Contagem: Centro baixou, Vila permanece ==")
    wipe()
    ov = ProdutoGestaoOverlayAgro.objects.create(
        produto_externo_id=f"{PREFIX}A", nome="Venc A"
    )
    el = EstoqueLote.objects.create(
        overlay=ov,
        lote_codigo="BXA",
        data_validade=date(2026, 7, 1),
        quantidade_atual=Decimal("2"),
    )
    cache.clear()
    a_all = _contagem_validade_dashboard_lotes_agro(None)
    a_c = _contagem_validade_dashboard_lotes_agro("centro")
    a_v = _contagem_validade_dashboard_lotes_agro("vila")
    check(a_all == a_c == a_v, "antes da baixa tres iguais", str(a_all))
    check(a_v["vencidos"] >= 1, "vila via vencido antes")

    el.baixado_centro_em = timezone.now()
    el.save(update_fields=["baixado_centro_em"])
    cache.clear()
    d_all = _contagem_validade_dashboard_lotes_agro(None)
    d_c = _contagem_validade_dashboard_lotes_agro("centro")
    d_v = _contagem_validade_dashboard_lotes_agro("vila")
    check(d_c["vencidos"] == a_c["vencidos"] - 1, "centro caiu 1", str(d_c))
    check(d_v["vencidos"] == a_v["vencidos"], "vila inalterada", str(d_v))
    check(d_all["vencidos"] == a_all["vencidos"], "C+V inalterado", str(d_all))
    check(EstoqueLote.objects.filter(pk=el.pk).exists(), "lote nao apagado")

    el.baixado_vila_em = timezone.now()
    el.save(update_fields=["baixado_vila_em"])
    # API apaga quando as duas baixam; aqui so conferimos a regra de contagem
    cache.clear()
    e_c = _contagem_validade_dashboard_lotes_agro("centro")
    e_v = _contagem_validade_dashboard_lotes_agro("vila")
    check(e_c["vencidos"] == a_c["vencidos"] - 1, "centro segue 0 neste overlay")
    check(e_v["vencidos"] == a_v["vencidos"] - 1, "vila caiu apos a 2a baixa")
    wipe()


def test_api_baixa_centro_preserva_lote() -> None:
    print("\n== API: baixa Centro nao apaga lote ==")
    wipe()
    ov = ProdutoGestaoOverlayAgro.objects.create(
        produto_externo_id=f"{PREFIX}API", nome="API"
    )
    el = EstoqueLote.objects.create(
        overlay=ov,
        lote_codigo="API1",
        data_validade=date(2026, 6, 1),
        quantidade_atual=Decimal("3"),
    )
    user, _ = User.objects.get_or_create(username="verify_bi_val_bx")
    rf = RequestFactory()
    body = json.dumps({"lote_id": el.pk, "deposito": "centro"})
    req = rf.post(
        "/api/relatorio-validade-baixa/",
        data=body,
        content_type="application/json",
        HTTP_HOST="127.0.0.1",
    )
    req.user = user

    with patch("produtos.views.obter_conexao_mongo", return_value=(object(), object())):
        with patch("produtos.views._saldo_erp_produto_deposito_mongo", return_value=Decimal("0")):
            with patch("produtos.views._saldo_final_agro_com_pin", return_value=Decimal("0")):
                with patch("produtos.views._produto_mongo_por_id_externo", return_value=None):
                    with patch(
                        "produtos.views._aplicar_baixa_operacional_vencimento_loja",
                        return_value=(True, None),
                    ) as mock_adj:
                        resp = api_relatorio_validade_baixa(req)

    check(resp.status_code == 200, f"HTTP {resp.status_code}")
    data = json.loads(resp.content.decode("utf-8"))
    check(data.get("ok") is True, "ok True", str(data)[:180])
    check(data.get("loja") == "centro", "loja=centro")
    check(data.get("apagou_lote") is False, "nao apagou lote")
    check(mock_adj.call_count == 0, "sem estoque nesta loja: nao mexe C+V")
    el.refresh_from_db()
    check(el.baixado_centro_em is not None, "flag centro marcada")
    check(el.baixado_vila_em is None, "flag vila vazia")
    check(el.quantidade_atual == Decimal("3.00") or el.quantidade_atual == Decimal("3"), "qtd preservada")

    body2 = json.dumps({"lote_id": el.pk, "deposito": "vila"})
    req2 = rf.post(
        "/api/relatorio-validade-baixa/",
        data=body2,
        content_type="application/json",
        HTTP_HOST="127.0.0.1",
    )
    req2.user = user
    with patch("produtos.views.obter_conexao_mongo", return_value=(object(), object())):
        with patch("produtos.views._saldo_erp_produto_deposito_mongo", return_value=Decimal("0")):
            with patch("produtos.views._saldo_final_agro_com_pin", return_value=Decimal("0")):
                with patch("produtos.views._produto_mongo_por_id_externo", return_value=None):
                    with patch(
                        "produtos.views._aplicar_baixa_operacional_vencimento_loja",
                        return_value=(True, None),
                    ):
                        resp2 = api_relatorio_validade_baixa(req2)
    data2 = json.loads(resp2.content.decode("utf-8"))
    check(resp2.status_code == 200 and data2.get("ok") is True, "2a baixa Vila ok")
    check(data2.get("apagou_lote") is True, "apagou so quando as duas conferiram")
    check(not EstoqueLote.objects.filter(pk=el.pk).exists(), "lote removido apos as duas")
    wipe()


def main() -> int:
    print("VERIFY BI-VAL-BAIXA-LOJA path")
    test_fonte()
    test_contagem_apos_baixa_centro()
    test_api_baixa_centro_preserva_lote()
    print(f"\n== RESULTADO {PASS}/{PASS + FAIL} ==")
    if FAIL:
        print("VERIFY_FAIL")
        return 1
    print("VERIFY_OK bi_val_baixa_loja")
    return 0


if __name__ == "__main__":
    sys.exit(main())
