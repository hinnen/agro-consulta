"""
Verificacao detalhada: Validade Salvar (VAL-SALVAR) + BI card por loja (BI-VAL-LOJA).
Roda: python scripts/verify_bi_val_salvar_path.py
"""
from __future__ import annotations

import os
import sys
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

import django

django.setup()

from django.contrib.auth import get_user_model
from django.core.cache import cache
from django.test import RequestFactory

from produtos.models import EstoqueLote, ProdutoGestaoOverlayAgro
from produtos.views import (
    _contagem_validade_dashboard_empresa,
    _contagem_validade_dashboard_por_loja,
    api_overlay_lote_adicionar,
)

PASS = 0
FAIL = 0
PREFIX = "VERIFYBIVAL"


def ok(msg: str) -> None:
    global PASS
    PASS += 1
    print(f"  OK  {msg}")


def bad(msg: str) -> None:
    global FAIL
    FAIL += 1
    print(f"  FAIL {msg}")


def check(cond: bool, msg: str) -> None:
    if cond:
        ok(msg)
    else:
        bad(msg)


def wipe() -> None:
    ProdutoGestaoOverlayAgro.objects.filter(produto_externo_id__startswith=PREFIX).delete()
    cache.clear()


def test_template_salvar_sempre() -> None:
    print("\n== VAL-SALVAR template ==")
    html = (ROOT / "produtos/templates/produtos/relatorios_validade.html").read_text(
        encoding="utf-8"
    )
    check("Usar cadastro" not in html and "Usar<br/>cadastro" not in html, "sem texto Usar cadastro")
    check("class=\"rv-salvar" in html or "class='rv-salvar" in html, "botao rv-salvar presente")
    check("if (isDraft || loteId)" in html, "JS grava lote quando tem lote_id")
    check("body.lote_id" in html, "JS envia lote_id")
    check("mongoOk" not in html, "mongoOk nao trava mais Salvar")


def test_api_lote_id_atualiza_data() -> None:
    print("\n== VAL-SALVAR api lote_id ==")
    wipe()
    pid = f"{PREFIX}API0001"
    ov = ProdutoGestaoOverlayAgro.objects.create(produto_externo_id=pid, nome="API")
    el = EstoqueLote.objects.create(
        overlay=ov,
        lote_codigo="L-OLD",
        data_validade=date(2228, 5, 9),
        quantidade_atual=Decimal("4"),
        deposito="centro",
    )
    User = get_user_model()
    u = User.objects.filter(is_superuser=True).first() or User.objects.first()
    rf = RequestFactory()
    req = rf.post(
        "/api/",
        data={},
        content_type="application/json",
        HTTP_HOST="127.0.0.1",
    )
    req.user = u
    import json

    req._body = json.dumps(
        {
            "produto_id": pid,
            "lote_id": el.pk,
            "lote_codigo": "L-OLD",
            "data_validade": "2028-05-10",
            "quantidade": "4",
            "deposito": "centro",
        }
    ).encode("utf-8")
    # RequestFactory POST with body: set manually
    from django.http import HttpRequest

    req2 = RequestFactory().generic(
        "POST",
        "/api/overlay/lote/",
        data=json.dumps(
            {
                "produto_id": pid,
                "lote_id": el.pk,
                "lote_codigo": "L-OLD",
                "data_validade": "2028-05-10",
                "quantidade": "4",
                "deposito": "centro",
            }
        ),
        content_type="application/json",
        HTTP_HOST="127.0.0.1",
    )
    req2.user = u
    resp = api_overlay_lote_adicionar(req2)
    body = json.loads(resp.content.decode("utf-8"))
    check(body.get("ok") is True, "API ok")
    el.refresh_from_db()
    check(el.data_validade == date(2028, 5, 10), "data corrigida 2028-05-10")
    check(float(el.quantidade_atual) == 4.0, "qtd preservada = 4")
    check(EstoqueLote.objects.filter(overlay=ov).count() == 1, "nao duplicou lote")
    wipe()


def test_bi_por_loja() -> None:
    print("\n== BI-VAL-LOJA contagem ==")
    wipe()
    hoje = date(2026, 8, 1)
    venc = date(2026, 7, 15)
    mes = date(2026, 8, 20)

    with patch(
        "produtos.estoque_saldo_agro_util.mapa_saldos_operacionais_agro", return_value={}
    ):
        base_c = _contagem_validade_dashboard_por_loja(hoje, "centro")
        base_v = _contagem_validade_dashboard_por_loja(hoje, "vila")

    ov_c = ProdutoGestaoOverlayAgro.objects.create(
        produto_externo_id=f"{PREFIX}CTR", nome="CTR"
    )
    EstoqueLote.objects.create(
        overlay=ov_c,
        lote_codigo="C1",
        data_validade=venc,
        quantidade_atual=Decimal("2"),
        deposito="centro",
    )
    saldos_c = {f"{PREFIX}CTR": {"saldo_centro": 0.0, "saldo_vila": 0.0}}
    with patch(
        "produtos.estoque_saldo_agro_util.mapa_saldos_operacionais_agro",
        return_value=saldos_c,
    ):
        c = _contagem_validade_dashboard_por_loja(hoje, "centro")
        v = _contagem_validade_dashboard_por_loja(hoje, "vila")
    check(
        c["vencidos"] == base_c["vencidos"] + 1 and v["vencidos"] == base_v["vencidos"],
        "lote centro: +1 Centro, Vila inalterada",
    )

    wipe()
    ov_v = ProdutoGestaoOverlayAgro.objects.create(
        produto_externo_id=f"{PREFIX}VILA", nome="VILA"
    )
    EstoqueLote.objects.create(
        overlay=ov_v,
        lote_codigo="V1",
        data_validade=venc,
        quantidade_atual=Decimal("3"),
        deposito="vila",
    )
    saldos_v = {f"{PREFIX}VILA": {"saldo_centro": 0.0, "saldo_vila": 3.0}}
    with patch(
        "produtos.estoque_saldo_agro_util.mapa_saldos_operacionais_agro",
        return_value=saldos_v,
    ):
        c = _contagem_validade_dashboard_por_loja(hoje, "centro")
        v = _contagem_validade_dashboard_por_loja(hoje, "vila")
    check(
        c["vencidos"] == base_c["vencidos"] and v["vencidos"] == base_v["vencidos"] + 1,
        "lote vila: +1 Vila, Centro inalterado",
    )

    wipe()
    ov_s = ProdutoGestaoOverlayAgro.objects.create(
        produto_externo_id=f"{PREFIX}SEM", nome="SEM"
    )
    EstoqueLote.objects.create(
        overlay=ov_s,
        lote_codigo="S1",
        data_validade=venc,
        quantidade_atual=Decimal("1"),
        deposito="",
    )
    with patch(
        "produtos.estoque_saldo_agro_util.mapa_saldos_operacionais_agro",
        return_value={f"{PREFIX}SEM": {"saldo_centro": 0.0, "saldo_vila": 0.0}},
    ):
        c = _contagem_validade_dashboard_por_loja(hoje, "centro")
        v = _contagem_validade_dashboard_por_loja(hoje, "vila")
    check(
        c["vencidos"] == base_c["vencidos"] and v["vencidos"] == base_v["vencidos"],
        "lote sem deposito + C+V0: nao inventa loja",
    )

    wipe()
    ov_m = ProdutoGestaoOverlayAgro.objects.create(
        produto_externo_id=f"{PREFIX}MES", nome="MES"
    )
    EstoqueLote.objects.create(
        overlay=ov_m,
        lote_codigo="M1",
        data_validade=mes,
        quantidade_atual=Decimal("1"),
        deposito="centro",
    )
    with patch(
        "produtos.estoque_saldo_agro_util.mapa_saldos_operacionais_agro",
        return_value={f"{PREFIX}MES": {"saldo_centro": 0.0, "saldo_vila": 0.0}},
    ):
        c = _contagem_validade_dashboard_por_loja(hoje, "centro")
    check(c["vencendo_mes"] == base_c["vencendo_mes"] + 1, "no mes conta no Centro")

    # cache key v6 (BI unificado)
    from produtos.views import VALIDADE_DASHBOARD_CACHE_KEY

    check("v6" in VALIDADE_DASHBOARD_CACHE_KEY, "cache key v6")

    from produtos.views import _contagem_validade_dashboard_lotes_agro_compute

    u = _contagem_validade_dashboard_lotes_agro_compute(hoje, None)
    uc = _contagem_validade_dashboard_lotes_agro_compute(hoje, "centro")
    uv = _contagem_validade_dashboard_lotes_agro_compute(hoje, "vila")
    check(u == uc == uv, "BI card compute: Centro = Vila = C+V")

    # empresa ainda soma lotes com qtd
    wipe()
    ov_e = ProdutoGestaoOverlayAgro.objects.create(
        produto_externo_id=f"{PREFIX}EMP", nome="EMP"
    )
    EstoqueLote.objects.create(
        overlay=ov_e,
        lote_codigo="E1",
        data_validade=venc,
        quantidade_atual=Decimal("1"),
        deposito="centro",
    )
    emp = _contagem_validade_dashboard_empresa(hoje)
    check(emp["vencidos"] >= 1, "visao empresa ainda conta vencidos")
    wipe()


def test_bi_url_loja() -> None:
    print("\n== BI link Relatorio ==")
    src = (ROOT / "produtos/views.py").read_text(encoding="utf-8")
    check(
        'f"?loja={deposito_filtro}"' in src or "?loja=" in src,
        "relatorios_validade_url passa loja",
    )
    check(
        "_contagem_validade_dashboard_lotes_agro,\n            deposito_filtro" in src
        or "deposito_filtro," in src,
        "dashboard passa deposito_filtro ao card",
    )


def main() -> int:
    print("VERIFY BI-VAL-LOJA + VAL-SALVAR")
    test_template_salvar_sempre()
    test_api_lote_id_atualiza_data()
    test_bi_por_loja()
    test_bi_url_loja()
    print(f"\n== RESULTADO {PASS}/{PASS + FAIL} ==")
    if FAIL:
        print("VERIFY_FAIL")
        return 1
    print("VERIFY_OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
