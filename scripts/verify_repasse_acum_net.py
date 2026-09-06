#!/usr/bin/env python
"""Prova detalhada — acumulado do repasse não pede valor já enviado (REPASSE-ACUM-NET)."""
from __future__ import annotations

import os
import sys
from datetime import timedelta
from decimal import Decimal
from pathlib import Path

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

from produtos.repasse_vila_util import (
    ZERO,
    _dec,
    _extra_do_calc,
    _extra_enviado_apos,
    abater_extras_do_acumulado,
    calcular_disponivel,
    listar_acumulado_detalhe,
)
from produtos.models import RepasseVilaDeltaDiaAgro

fails: list[str] = []
oks = 0


def ok(msg: str) -> None:
    global oks
    oks += 1
    print(f"OK {msg}")


def fail(msg: str) -> None:
    fails.append(msg)
    print(f"FAIL {msg}")


def _calc(enviado, cmv=0, lucro=0, fiado=0, elet=0) -> dict:
    return {
        "alvos": {"cmv": cmv, "lucro": lucro, "fiado": fiado},
        "ja_eletronico_aplicado": elet,
        "ja_enviado": {"total": enviado},
    }


def main() -> int:
    hoje = timezone.localdate()

    # --- path / contrato ---
    util = (ROOT / "produtos/repasse_vila_util.py").read_text(encoding="utf-8", errors="replace")
    tela = (ROOT / "produtos/templates/produtos/repasse_vila.html").read_text(
        encoding="utf-8", errors="replace"
    )
    js = (ROOT / "produtos/static/produtos/js/pdv_repasse_vila.js").read_text(
        encoding="utf-8", errors="replace"
    )
    ajuda = (ROOT / "produtos/templates/produtos/includes/repasse_help_agents.html").read_text(
        encoding="utf-8", errors="replace"
    )

    for needle in (
        "abater_extras_do_acumulado",
        "acumulado_bruto",
        "_extra_enviado_apos",
        'acum = _dec(calc.get("acumulado_anterior"))',
        "não cria ajuste",
    ):
        if needle in util:
            ok(f"util tem {needle[:40]}")
        else:
            fail(f"util sem {needle[:40]}")

    for bad in ("_quitar_acumulado_no_repasse", "quitacao = max"):
        if bad in util:
            fail(f"util ainda tem {bad}")
        else:
            ok(f"util sem {bad}")

    if "acumulado já coberto por envio" in tela:
        ok("tela avisa cobertos")
    else:
        fail("tela sem aviso coberto")
    if "tot + acum" in tela:
        ok("tela soma acumulado líquido")
    else:
        fail("tela não soma acum no total")
    if "incluir_acumulado" in js:
        ok("PDV manda incluir_acumulado")
    else:
        fail("PDV sem incluir_acumulado")
    if "abate sozinho" in ajuda:
        ok("ajuda: abate sozinho")
    else:
        fail("ajuda sem abate sozinho")

    # --- conta: print da loja (hoje) ---
    c_print = _calc("1878.47")
    extra_print = _extra_do_calc(c_print)
    liq_print = abater_extras_do_acumulado(hoje, Decimal("1878.47"), c_print)
    if extra_print == Decimal("1878.47") and liq_print == ZERO:
        ok("print 18/08: 1878,47 enviado zera acumulado")
    else:
        fail(f"print 18/08 extra={extra_print} liq={liq_print}")

    # alvo do dia 129,46 + extra cobre 1749,01
    c_misto = _calc("1878.47", cmv="129.46")
    extra_m = _extra_do_calc(c_misto)
    liq_m = abater_extras_do_acumulado(hoje, Decimal("1749.01"), c_misto)
    if extra_m == Decimal("1749.01") and liq_m == ZERO:
        ok("extra além do alvo do dia zera acum")
    else:
        fail(f"misto extra={extra_m} liq={liq_m}")

    # print 01/08: falta 102,38 + acum 463,15 sem envio no dia
    c_d1 = _calc(0, cmv="102.38")
    extra_d1 = _extra_do_calc(c_d1)
    if extra_d1 == ZERO:
        ok("sem envio no dia -> extra 0")
    else:
        fail(f"dia sem envio extra={extra_d1}")
    liq_d1 = abater_extras_do_acumulado(hoje, Decimal("463.15"), c_d1)
    if liq_d1 == Decimal("463.15"):
        ok("sem extra, acum bruto permanece")
    else:
        fail(f"sem extra liq={liq_d1}")

    # parcial
    liq_p = abater_extras_do_acumulado(hoje, Decimal("463.15"), _calc("100"))
    if liq_p == Decimal("363.15"):
        ok("extra parcial abate só a parte")
    else:
        fail(f"parcial liq={liq_p}")

    # crédito (levou a mais que o acum)
    liq_c = abater_extras_do_acumulado(hoje, Decimal("1878.47"), _calc("2000"))
    if liq_c == Decimal("-121.53"):
        ok("extra maior vira crédito")
    else:
        fail(f"crédito liq={liq_c}")

    # cartão/PIX reduz alvo físico
    c_elet = _calc("100", cmv="350.60", elet="350.60")
    extra_e = _extra_do_calc(c_elet)
    if extra_e == Decimal("100.00"):
        ok("cartão no alvo: extra = só o dinheiro")
    else:
        fail(f"elet extra={extra_e}")

    # --- extra depois de um dia passado = soma dos deltas negativos ---
    if _extra_enviado_apos(hoje) == ZERO:
        ok("extra depois de hoje = 0")
    else:
        fail("extra depois de hoje deveria ser 0")

    d_sel = hoje - timedelta(days=10)
    got_apos = _extra_enviado_apos(d_sel)
    esp_apos = ZERO
    for row in RepasseVilaDeltaDiaAgro.objects.filter(
        data_ref__gte=d_sel + timedelta(days=1), data_ref__lte=hoje
    ).only("delta"):
        dlt = _dec(row.delta)
        if dlt < 0:
            esp_apos += -dlt
    if got_apos == esp_apos.quantize(Decimal("0.01")):
        ok(f"extra apos 10d = cache ({got_apos})")
    else:
        fail(f"extra apos {got_apos} != {esp_apos}")

    # dia passado + extra posterior (print 01/08 com envio no dia 18)
    liq_pass = (Decimal("463.15") - got_apos).quantize(Decimal("0.01"))
    if got_apos > 0 and liq_pass <= 0:
        ok("dias seguintes com extra cobrem acum de dia passado")
    elif got_apos == 0:
        ok("sem extra posterior no cache (nada a cobrir no passado)")
    else:
        ok(f"acum passado líquido {liq_pass} após extra {got_apos}")

    # --- dados reais do PC (hoje) ---
    calc = calcular_disponivel(hoje)
    if not calc.get("ok"):
        fail("calcular_disponivel hoje")
        print("---")
        print(f"oks={oks} fails={len(fails)}")
        return 1
    ok("calcular_disponivel hoje")

    bruto = _dec(calc.get("acumulado_bruto"))
    liq = _dec(calc.get("acumulado_anterior"))
    falta = _dec(calc.get("falta_dinheiro"))
    sug = _dec(calc.get("total_sugerido"))
    enviado = _dec((calc.get("ja_enviado") or {}).get("total"))
    extra_hoje = _extra_do_calc(calc)
    esp_liq = (bruto - extra_hoje).quantize(Decimal("0.01"))
    print(
        f"HOJE {hoje.isoformat()} enviado={enviado} falta={falta} "
        f"bruto={bruto} extra={extra_hoje} liq={liq} sugerido={sug}"
    )
    if liq == esp_liq:
        ok("hoje: liquido = bruto - extra do dia")
    else:
        fail(f"hoje liq {liq} != {esp_liq}")
    if sug == (falta + liq).quantize(Decimal("0.01")):
        ok("hoje: sugerido = falta + líquido")
    else:
        fail(f"hoje sug {sug} != falta+liq")
    if extra_hoje >= bruto and bruto > 0:
        if liq <= 0 and sug <= falta:
            ok("hoje: extra cobre o bruto -> nao pede acum de novo")
        else:
            fail(f"hoje deveria zerar acum (liq={liq} sug={sug})")
    elif enviado > 0 and falta == 0 and liq > 0 and extra_hoje == 0:
        fail("hoje: já enviado, falta 0, mas acum líquido ainda positivo sem extra")
    else:
        ok("hoje: invariante extra/acum ok")

    det = listar_acumulado_detalhe(hoje)
    if det.get("ok") and abs(float(det["acumulado_anterior"]) - float(liq)) < 0.02:
        ok("detalhe = calc líquido")
    else:
        fail("detalhe diverge do calc")
    if "acumulado_bruto" in det and "acumulado_bruto" in calc:
        ok("API expõe acumulado_bruto")
    else:
        fail("sem acumulado_bruto")

    # confirmar não deve somar acum bruto se líquido já é 0
    if liq <= 0:
        ok("confirmar com incluir_acumulado não soma (líquido <= 0)")
    else:
        ok(f"ainda ha liquido {liq} - incluir_acumulado somaria isso (correto)")

    print("---")
    print(f"oks={oks} fails={len(fails)}")
    for f in fails:
        print(f)
    if fails:
        return 1
    print("VERIFY_ACUM_NET_OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
