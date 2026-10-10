#!/usr/bin/env python
"""Prova: CP só DINHEIRO/BANCO canônicos + extravio = depósito − BANCO (com sinal)."""
from __future__ import annotations

import os
import sys
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

import django

django.setup()

from produtos.conferencia_deposito_extravio_util import (
    aplicar_extravio_auto_no_resumo,
    extravio_auto_periodo,
)
from produtos.extravio_deposito_util import (
    FORMA_CP_BANCO,
    FORMA_CP_DINHEIRO,
    filtrar_formas_baixa_cp,
    forma_eh_banco,
    forma_eh_dinheiro,
    forma_permitida_baixa_cp,
)


def check(name: str, cond: bool, detail: str = "") -> None:
    if not cond:
        raise SystemExit(f"FAIL {name}: {detail or 'assertion'}")
    print(f"OK {name}")


def main() -> None:
    check("banco_ok", forma_eh_banco("BANCO"))
    check("banco_ok2", forma_eh_banco("Banco"))
    check("deposito_ok", forma_eh_banco("DEPÓSITO"))
    check("deposito_ok2", forma_eh_banco("Deposito"))
    check("banco_nao_pix", not forma_eh_banco("Pix"))
    check("banco_nao_cartao", not forma_eh_banco("Cartão crédito"))
    check("banco_nao_adicionar", not forma_eh_banco("ADICIONAR BANCO"))
    check("dinheiro_ok", forma_eh_dinheiro("Dinheiro"))
    check("perm_banco", forma_permitida_baixa_cp("BANCO"))
    check("perm_dep", forma_permitida_baixa_cp("DEPÓSITO"))
    check("perm_din", forma_permitida_baixa_cp("Dinheiro"))
    check("perm_nao_pix", not forma_permitida_baixa_cp("Pix"))

    formas = [
        {"id": "1", "nome": "Dinheiro"},
        {"id": "1b", "nome": "À vista - Dinheiro"},
        {"id": "2", "nome": "BANCO"},
        {"id": "3", "nome": "Pix"},
        {"id": "4", "nome": "Cartão"},
    ]
    f2 = filtrar_formas_baixa_cp(formas)
    check("filtro_2", len(f2) == 2, str(f2))
    nomes = [x["nome"] for x in f2]
    check("filtro_nomes", nomes == [FORMA_CP_DINHEIRO, FORMA_CP_BANCO], str(nomes))
    check("filtro_id_din", f2[0].get("id") == "1", str(f2[0]))
    check("filtro_id_ban", f2[1].get("id") == "2", str(f2[1]))

    # Sem BANCO no ERP → injeta
    f3 = filtrar_formas_baixa_cp(
        [{"id": "9", "nome": "À vista - Dinheiro"}, {"id": "3", "nome": "Pix"}]
    )
    check("injeta_2", len(f3) == 2, str(f3))
    check("injeta_din", f3[0]["nome"] == FORMA_CP_DINHEIRO)
    check("injeta_ban", f3[1]["nome"] == FORMA_CP_BANCO and f3[1].get("id") == "")

    # fórmula: depósito 1000 − BANCO 700 = 300
    core = {
        "extravio_apos_deposito": Decimal("0"),
        "geracao_caixa": Decimal("5000.00"),
        "retiradas_socios": Decimal("0"),
    }

    from produtos import conferencia_deposito_extravio_util as u

    orig = u.extravio_auto_periodo

    def fake_periodo(*_a, **_k):
        return {
            "depositos_caixa": Decimal("1000.00"),
            "baixas_banco": Decimal("700.00"),
            "extravio_auto": Decimal("300.00"),
        }

    u.extravio_auto_periodo = fake_periodo  # type: ignore
    try:
        from datetime import date

        aplicar_extravio_auto_no_resumo(
            core, date(2026, 8, 1), date(2026, 8, 31), deposito="centro"
        )
    finally:
        u.extravio_auto_periodo = orig  # type: ignore

    check("auto_300", Decimal(core["extravio_apos_deposito"]) == Decimal("300.00"))
    check("dep_key", Decimal(core["depositos_caixa"]) == Decimal("1000.00"))
    check("ban_key", Decimal(core["baixas_banco"]) == Decimal("700.00"))
    check(
        "geracao_cortou",
        Decimal(core["geracao_caixa"]) == Decimal("4700.00"),
        str(core["geracao_caixa"]),
    )

    # manual + auto
    core2 = {
        "extravio_apos_deposito": Decimal("50.00"),
        "geracao_caixa": Decimal("1000.00"),
    }
    u.extravio_auto_periodo = fake_periodo  # type: ignore
    try:
        from datetime import date

        aplicar_extravio_auto_no_resumo(
            core2, date(2026, 8, 1), date(2026, 8, 31)
        )
    finally:
        u.extravio_auto_periodo = orig  # type: ignore
    check(
        "manual_mais_auto",
        Decimal(core2["extravio_apos_deposito"]) == Decimal("350.00"),
    )

    # extravio negativo (BANCO > depósito)
    def fake_neg(*_a, **_k):
        return {
            "depositos_caixa": Decimal("100.00"),
            "baixas_banco": Decimal("250.00"),
            "extravio_auto": Decimal("-150.00"),
        }

    core3 = {
        "extravio_apos_deposito": Decimal("0"),
        "geracao_caixa": Decimal("1000.00"),
    }
    u.extravio_auto_periodo = fake_neg  # type: ignore
    try:
        from datetime import date

        aplicar_extravio_auto_no_resumo(core3, date(2026, 8, 1), date(2026, 8, 31))
    finally:
        u.extravio_auto_periodo = orig  # type: ignore
    check(
        "auto_neg",
        Decimal(core3["extravio_apos_deposito"]) == Decimal("-150.00"),
        str(core3["extravio_apos_deposito"]),
    )
    check(
        "geracao_sobe_neg",
        Decimal(core3["geracao_caixa"]) == Decimal("1150.00"),
        str(core3["geracao_caixa"]),
    )

    views = (ROOT / "produtos" / "views.py").read_text(encoding="utf-8")
    check("views_reject", "_forma_pagamento_permitida_baixa_cp" in views)
    check("views_filter_qs", "somente_dinheiro_banco" in views)
    check("views_filtrar_call", "filtrar_formas_baixa_cp" in views)

    conf_src = (
        ROOT / "produtos" / "conferencia_deposito_extravio_util.py"
    ).read_text(encoding="utf-8")
    check("sem_max0", "if auto < 0" not in conf_src)

    html_cp = (
        ROOT / "produtos" / "templates" / "produtos" / "lancamentos_contas_pagar_teste.html"
    ).read_text(encoding="utf-8")
    check("cp_param", "somente_dinheiro_banco=1" in html_cp)
    check("cp_checkbox_dia", "caixa do dia" in html_cp.lower())

    html_cl = (
        ROOT / "produtos" / "templates" / "produtos" / "lancamentos_financeiros.html"
    ).read_text(encoding="utf-8")
    check("cl_param", "somente_dinheiro_banco" in html_cl)

    js = (ROOT / "static" / "js" / "agro_resumo_gerencial.js").read_text(encoding="utf-8")
    check(
        "js_auto_hint",
        "depósitos do caixa" in js or "depositos do caixa" in js.lower() or "baixas CP" in js,
    )
    check("js_sinal_abs", "Math.abs(extravioDep)" in js)

    pg = (
        ROOT / "financeiro" / "services" / "resumo_operacional_pg.py"
    ).read_text(encoding="utf-8")
    check("pg_hook", "aplicar_extravio_auto_no_resumo" in pg)

    from datetime import date

    pack = extravio_auto_periodo(date(2026, 8, 1), date(2026, 8, 31), deposito="centro")
    check("smoke_keys", "extravio_auto" in pack and "depositos_caixa" in pack)

    # PIN loja + API real (RequestFactory)
    from base.models import PerfilUsuario
    from django.contrib.auth import get_user_model
    from django.test import RequestFactory
    from produtos import views as vviews

    check("pin_9973", PerfilUsuario.objects.filter(senha_rapida="9973").exists())
    user = get_user_model().objects.filter(is_superuser=True).first()
    if user is None:
        user = get_user_model().objects.first()
    check("user_db", user is not None)
    rf = RequestFactory()
    req = rf.get(
        "/api/lancamentos/opcoes-baixa/",
        {"modo": "erp", "apenas_cadastro_erp": "1", "somente_dinheiro_banco": "1"},
    )
    req.user = user
    resp = vviews.api_lancamentos_opcoes_baixa(req)
    check("api_200", resp.status_code == 200, str(resp.status_code))
    import json

    data = json.loads(resp.content.decode())
    nomes_api = [x.get("nome") for x in data.get("formas") or []]
    check("api_so_2", len(nomes_api) == 2, str(nomes_api))
    check(
        "api_nomes",
        set(nomes_api) == {FORMA_CP_DINHEIRO, FORMA_CP_BANCO},
        str(nomes_api),
    )
    check("api_flag", data.get("somente_dinheiro_banco") is True)

    print("ALL OK")



if __name__ == "__main__":
    main()
