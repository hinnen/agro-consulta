#!/usr/bin/env python
"""Prova detalhada — REPASSE-PLANOS-CENTRO (config PG + rateio lucro)."""
from __future__ import annotations

import json
import os
import sys
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

import django

django.setup()

from django.contrib.auth import get_user_model
from django.test import Client
from django.urls import reverse
from django.utils import timezone

from produtos.models import RepasseVilaConfigAgro
from produtos.repasse_vila_util import (
    calcular_disponivel,
    despesas_caixa_vila_por_plano,
    historico_mes,
    listar_planos_repasse_config,
    nomes_planos_desconto_centro,
    obter_config,
    partir_despesas_centro_vila,
    salvar_percentual_padrao,
    salvar_planos_desconto_centro,
)

User = get_user_model()
fails: list[str] = []
oks = 0
ZERO = Decimal("0.00")


def ok(msg: str) -> None:
    global oks
    oks += 1
    print("OK", msg)


def fail(msg: str) -> None:
    fails.append(msg)
    print("FAIL", msg)


def check_file(rel: str, *needles: str) -> None:
    p = ROOT / rel
    if not p.exists():
        fail(f"MISSING {rel}")
        return
    text = p.read_text(encoding="utf-8", errors="replace")
    for n in needles:
        if n not in text:
            fail(f"{rel} missing {n!r}")
        else:
            ok(f"file {rel} · {n}")


def main() -> int:
    check_file(
        "produtos/models.py",
        "planos_desconto_centro",
        "RepasseVilaConfigAgro",
    )
    check_file(
        "produtos/migrations/0091_repasse_planos_desconto_centro.py",
        "planos_desconto_centro",
        "0090_caixa_conferencia_rascunho_agro",
    )
    check_file(
        "produtos/repasse_vila_util.py",
        "salvar_planos_desconto_centro",
        "despesas_caixa_vila_por_plano",
        "partir_despesas_centro_vila",
        "apos_planos = max(ZERO, (lucro - desp_centro)",
        "lucro_bruto_mes - lucro_enviado_mes - desp_vila",
        'deposito="vila"',
    )
    check_file(
        "produtos/views_repasse_vila.py",
        "salvar_planos_desconto_centro",
        "planos_desconto_centro",
        "percentual_lucro_padrao",
    )
    check_file(
        "produtos/templates/produtos/repasse_vila.html",
        "rv-btn-planos",
        "rv-planos-modal",
        "planos_desconto_centro",
        "Marcado = desconta",
    )
    check_file(
        "produtos/templates/produtos/repasse_vila.html",
        "percentual_lucro_padrao: pctEl.value",
    )
    html = (ROOT / "produtos/templates/produtos/repasse_vila.html").read_text(
        encoding="utf-8", errors="replace"
    )
    if "localStorage" in html and "planos" in html.lower():
        # localStorage exists for other prefs? this page shouldn't persist planos there
        if "planos_desconto" in html and "localStorage" in html:
            fail("planos não podem ir para localStorage")
        else:
            ok("planos não usam localStorage")
    else:
        ok("tela sem localStorage de planos")
    check_file(
        "produtos/templates/produtos/partials/pdv/repasse_vila_overlay.html",
        "pdv-rp-desp-hint",
    )
    check_file(
        "produtos/static/produtos/js/pdv_repasse_vila.js",
        "despesas_centro_dia",
    )
    check_file("produtos/pg_backup_registry.py", "RepasseVilaConfigAgro")

    c_cent, c_vila = partir_despesas_centro_vila(
        {"Alimentação": Decimal("80.00"), "Combustível Strada": Decimal("20.00")},
        ["Alimentação"],
    )
    if c_cent == Decimal("80.00") and c_vila == Decimal("20.00"):
        ok("rateio marcado Centro / resto Vila")
    else:
        fail(f"rateio {c_cent}/{c_vila}")

    z_cent, z_vila = partir_despesas_centro_vila(
        {"Alimentação": Decimal("80.00")}, []
    )
    if z_cent == ZERO and z_vila == Decimal("80.00"):
        ok("sem marca = tudo Vila (envio intacto)")
    else:
        fail(f"vazio {z_cent}/{z_vila}")

    t_cent, t_vila = partir_despesas_centro_vila(
        {"Alimentação": Decimal("80.00"), "Combustível Strada": Decimal("20.00")},
        ["Alimentação", "Combustível Strada"],
    )
    if t_cent == Decimal("100.00") and t_vila == ZERO:
        ok("todos marcados = tudo Centro")
    else:
        fail(f"todos {t_cent}/{t_vila}")

    n_cent, n_vila = partir_despesas_centro_vila(
        {"Alimentação": Decimal("999.00")}, ["Alimentação"]
    )
    if n_cent == Decimal("999.00"):
        ok("despesa maior que lucro ainda rateia o valor cheio")
    else:
        fail(f"overflow split {n_cent}")

    hoje = timezone.localdate()
    fake_linhas = {
        "linhas": [
            {"plano": "Depósito (caixa → banco)", "valor": "100.00"},
            {"plano": "Alimentação", "valor": "50.00"},
            {"plano": "-", "valor": "9.00"},
        ]
    }
    with patch(
        "produtos.caixa_retiradas_util.listar_retiradas_historico",
        return_value=fake_linhas,
    ):
        por = despesas_caixa_vila_por_plano(hoje, hoje)
    if "Alimentação" in por and por["Alimentação"] == Decimal("50.00"):
        ok("agrupa Alimentação da Vila")
    else:
        fail(f"agrupa {por}")
    if any(_k.casefold().startswith("dep") for _k in por):
        fail(f"depósito não deveria entrar {por}")
    else:
        ok("depósito excluído do bolo")
    if "-" in por:
        fail("traço não é plano")
    else:
        ok("plano vazio ignorado")

    cfg = obter_config()
    pct_antes = cfg.percentual_lucro_padrao
    planos_antes = list(cfg.planos_desconto_centro or [])
    try:
        salvar_percentual_padrao(Decimal("50"), operador="planos-verify")
        salvar_planos_desconto_centro(["Alimentação", "alimentação", ""], operador="planos-verify")
        cfg_pg = RepasseVilaConfigAgro.objects.order_by("pk").first()
        saved = list((cfg_pg.planos_desconto_centro if cfg_pg else []) or [])
        if saved == ["Alimentação"]:
            ok("Postgres persiste planos (dedup)")
        else:
            fail(f"PG saved {saved}")
        if nomes_planos_desconto_centro(cfg_pg) == ["Alimentação"]:
            ok("nomes_planos lê PG")
        else:
            fail("nomes_planos divergente")

        lista = listar_planos_repasse_config(cfg_pg)
        marked = [p["nome"] for p in lista if p.get("marcado")]
        if "Alimentação" in marked:
            ok("lista marca Alimentação")
        else:
            fail(f"lista marked {marked[:8]}")

        fake_base = {
            "receita": Decimal("1000.00"),
            "cmv": Decimal("400.00"),
            "lucro_bruto": Decimal("600.00"),
            "skus_com_custo": 1,
            "skus_sem_custo": 0,
            "n_vendas": 1,
        }
        ja_zero = {"cmv": ZERO, "lucro": ZERO, "fiado": ZERO, "total": ZERO}
        fake_desp = {
            "Alimentação": Decimal("30.00"),
            "Combustível Strada": Decimal("10.00"),
        }

        def _calc(planos):
            salvar_planos_desconto_centro(planos, operador="planos-verify")
            with (
                patch(
                    "produtos.repasse_vila_util._receita_e_cmv_vila",
                    return_value=fake_base,
                ),
                patch("produtos.repasse_vila_util._fiado_pago_vila", return_value=ZERO),
                patch("produtos.repasse_vila_util._ja_enviado_dia", return_value=ja_zero),
                patch("produtos.repasse_vila_util._ja_eletronico_vila", return_value=ZERO),
                patch(
                    "produtos.repasse_vila_util.despesas_caixa_vila_por_plano",
                    return_value=fake_desp,
                ),
                patch(
                    "produtos.repasse_vila_util.reserva_aplicada_no_dia",
                    return_value=ZERO,
                ),
            ):
                return calcular_disponivel(
                    hoje, percentual_lucro=Decimal("50"), modo_dia_cheio=True
                )

        calc_on = _calc(["Alimentação"])
        calc_off = _calc([])

        # Planos antes do 50/50: off = 50% de 600 = 300; on = 50% de (600-30) = 285
        lucro_on = Decimal(str(calc_on["alvos"]["lucro"]))
        lucro_off = Decimal(str(calc_off["alvos"]["lucro"]))
        if lucro_off == Decimal("300.00") and lucro_on == Decimal("285.00"):
            ok("50pct: off=300; com Alimentacao 30 antes do split = 285")
        else:
            fail(f"lucro on={lucro_on} off={lucro_off}")
        if calc_on["alvos"]["cmv"] == calc_off["alvos"]["cmv"] == 400.0:
            ok("CMV intacto (400)")
        else:
            fail(f"CMV {calc_on['alvos']['cmv']} / {calc_off['alvos']['cmv']}")
        if calc_on["alvos"]["fiado"] == calc_off["alvos"]["fiado"] == 0.0:
            ok("fiado intacto")
        else:
            fail("fiado mudou")
        if Decimal(str(calc_on["despesas_centro_dia"])) == Decimal("30.00"):
            ok("despesas_centro_dia=30")
        else:
            fail(f"desp centro {calc_on.get('despesas_centro_dia')}")
        if Decimal(str(calc_off["despesas_centro_dia"])) == ZERO:
            ok("sem marca despesas_centro=0")
        else:
            fail("off deveria mandar 0 ao Centro")
        if Decimal(str(calc_off["despesas_vila_dia"])) == Decimal("40.00"):
            ok("sem marca gastos ficam na Vila (40)")
        else:
            fail(f"desp vila off {calc_off.get('despesas_vila_dia')}")
        if lucro_on >= ZERO:
            ok("lucro enviado não fica negativo")
        else:
            fail("lucro negativo")

        with (
            patch(
                "produtos.repasse_vila_util._receita_e_cmv_vila",
                return_value=fake_base,
            ),
            patch("produtos.repasse_vila_util._fiado_pago_vila", return_value=ZERO),
            patch("produtos.repasse_vila_util._ja_enviado_dia", return_value=ja_zero),
            patch("produtos.repasse_vila_util._ja_eletronico_vila", return_value=ZERO),
            patch(
                "produtos.repasse_vila_util.despesas_caixa_vila_por_plano",
                return_value={"Alimentação": Decimal("99999.00")},
            ),
        ):
            salvar_planos_desconto_centro(["Alimentação"], operador="planos-verify")
            calc_cap = calcular_disponivel(
                hoje, percentual_lucro=Decimal("50"), modo_dia_cheio=True
            )
        if Decimal(str(calc_cap["alvos"]["lucro"])) == ZERO:
            ok("teto: lucro enviado não desce de zero")
        else:
            fail(f"teto lucro {calc_cap['alvos']['lucro']}")

        fake_base_mes = {
            "receita": Decimal("1000.00"),
            "cmv": Decimal("400.00"),
            "lucro_bruto": Decimal("600.00"),
            "n_vendas": 1,
        }
        salvar_planos_desconto_centro(["Alimentação"], operador="planos-verify")
        with (
            patch(
                "produtos.repasse_vila_util._receita_e_cmv_vila_periodo",
                return_value=fake_base_mes,
            ),
            patch(
                "produtos.repasse_vila_util.despesas_caixa_vila_por_plano",
                return_value=fake_desp,
            ),
        ):
            hist_on = historico_mes(hoje.year, hoje.month)
        enviado = Decimal(str(hist_on.get("lucro_enviado_mes") or 0))
        ficou = Decimal(str(hist_on.get("lucro_ficou_vila") or 0))
        esperado_ficou = max(ZERO, (Decimal("600.00") - enviado - Decimal("10.00")).quantize(Decimal("0.01")))
        if Decimal(str(hist_on.get("despesas_centro_mes") or 0)) == Decimal("30.00"):
            ok("hist: marcado vai para Centro (30)")
        else:
            fail(f"hist desp centro {hist_on.get('despesas_centro_mes')}")
        if Decimal(str(hist_on.get("despesas_vila_mes") or 0)) == Decimal("10.00"):
            ok("hist: nao marcado fica na Vila (10)")
        else:
            fail(f"hist desp vila {hist_on.get('despesas_vila_mes')}")
        if ficou == esperado_ficou:
            ok("hist: ficou = bruto - enviado - gastos Vila")
        else:
            fail(f"hist ficou {ficou} esperado {esperado_ficou} enviado {enviado}")

        user, _ = User.objects.get_or_create(
            username="repasse_planos_verify_bot", defaults={"is_staff": True}
        )
        client = Client(HTTP_HOST="127.0.0.1")
        client.force_login(user)

        r = client.get(reverse("repasse_vila"))
        if r.status_code == 200 and b"rv-btn-planos" in r.content and b"rv-planos-modal" in r.content:
            ok("tela tem botão Planos")
        else:
            fail(f"tela planos status={r.status_code}")

        r = client.post(
            reverse("api_repasse_vila_config"),
            data=json.dumps({"planos_desconto_centro": ["Alimentação"]}),
            content_type="application/json",
        )
        j = r.json()
        if r.status_code == 200 and j.get("ok") and "Alimentação" in (j.get("planos_desconto_centro") or []):
            ok("POST só planos grava")
        else:
            fail(f"POST planos {r.status_code} {j}")

        r = client.post(
            reverse("api_repasse_vila_config"),
            data=json.dumps({"percentual_lucro_padrao": 50}),
            content_type="application/json",
        )
        j = r.json()
        if "Alimentação" in (j.get("planos_desconto_centro") or []):
            ok("POST só % não apaga planos")
        else:
            fail(f"POST % limpou planos {j.get('planos_desconto_centro')}")

        r = client.get(reverse("api_repasse_vila_config"))
        j = r.json()
        if r.status_code == 200 and j.get("ok") and isinstance(j.get("planos"), list):
            ok("GET config devolve lista")
        else:
            fail("GET config")

        r = client.get(reverse("api_repasse_vila_calc") + f"?data={hoje.isoformat()}&pct=50")
        j = r.json()
        if r.status_code == 200 and j.get("ok") and "despesas_centro_dia" in j and "planos_desconto_centro" in j:
            ok("calc expõe despesas/planos")
        else:
            fail(f"calc keys {list((j or {}).keys())[:12]}")

        r = client.get(reverse("api_repasse_vila_historico"))
        j = r.json()
        if r.status_code == 200 and "despesas_vila_mes" in j and "lucro_ficou_vila" in j:
            ok("hist expõe gastos Vila")
        else:
            fail(f"hist keys {list((j or {}).keys())[:12]}")
    finally:
        salvar_planos_desconto_centro(planos_antes, operador="planos-verify")
        salvar_percentual_padrao(pct_antes, operador="planos-verify")

    print(f"---\noks={oks} fails={len(fails)}")
    for f in fails:
        print("FAIL", f)
    if fails:
        print("VERIFY_PLANOS_FAIL")
        return 1
    print("VERIFY_PLANOS_OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
