#!/usr/bin/env python
"""Prova path — REPASSE-ACUM-EXTRA-BUG (overpay no dia + cartão/PIX).

Caso loja 11/09: acumulado 522,40 → levou 500 → tela 445 (bug).
Com fix: bruto 688,54 − extra 434,03 → ~254,51.
"""
from __future__ import annotations

import json
import os
import sys
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

from django.test import Client
from django.utils import timezone

from produtos import caixa_util as cu
from produtos.repasse_vila_util import (
    _alvo_fisico_de_calc,
    _dec,
    _extra_do_calc,
    abater_extras_do_acumulado,
    calcular_disponivel,
)

fails: list[str] = []
oks = 0
PIN_LOJA = "9973"
SNAP = ROOT / "scripts/data/snapshot_repasse_acumulado_pre_fix_20260911.json"


def ok(msg: str) -> None:
    global oks
    oks += 1
    print(f"OK {msg}")


def fail(msg: str) -> None:
    fails.append(msg)
    print(f"FAIL {msg}")


def main() -> int:
    util = (ROOT / "produtos/repasse_vila_util.py").read_text(encoding="utf-8", errors="replace")

    # --- contrato fonte ---
    if '"ja_eletronico": ja_elet' in util or "'ja_eletronico': ja_elet" in util:
        ok("mini de calcular_disponivel passa ja_eletronico")
    else:
        fail("mini sem ja_eletronico")

    if 'if "ja_eletronico" in calc' in util:
        ok("_alvo_fisico prefere ja_eletronico (não or falsy)")
    else:
        fail("_alvo_fisico sem preferência ja_eletronico")

    bad = 'calc.get("ja_eletronico_aplicado") or calc.get("ja_eletronico")'
    if bad in util:
        fail("ainda usa or falsy no alvo físico")
    else:
        ok("sem or falsy no alvo físico")

    if SNAP.is_file():
        ok("snapshot pré-fix presente")
        snap = json.loads(SNAP.read_text(encoding="utf-8"))
    else:
        fail("snapshot pré-fix ausente")
        snap = {}

    meta = (snap.get("meta") or {}).get("contas_derivadas_agora") or {}
    tela = (snap.get("meta") or {}).get("tela_observada_renan") or {}
    envio = (snap.get("meta") or {}).get("no_momento_do_envio_id19") or {}

    if float(tela.get("acumulado_dias_anteriores_tela") or 0) == 445.02:
        ok("snapshot: tela bugada 445,02")
    else:
        fail(f"snapshot tela={tela.get('acumulado_dias_anteriores_tela')}")

    if float(envio.get("acumulado_anterior_snapshot") or 0) == 522.40:
        ok("snapshot: no envio acumulado era 522,40")
    else:
        fail("snapshot sem 522,40 no envio")

    # --- regressão matemática (números da loja) ---
    # alvo dia = 252,27 + 4,21 = 256,48 · PIX/cartão 190,51 · físico 65,97
    calc_loja = {
        "alvos": {"cmv": "252.27", "lucro": "4.21", "fiado": 0},
        "ja_eletronico": "190.51",
        "ja_eletronico_aplicado": 0,  # after overpay o aplicado zera
        "ja_enviado": {"total": "500", "cmv": "491.81", "lucro": "8.19", "fiado": 0},
    }
    alvo = _alvo_fisico_de_calc(calc_loja)
    extra = _extra_do_calc(calc_loja)
    bruto = Decimal("688.54")
    liq = abater_extras_do_acumulado(timezone.localdate(), bruto, calc_loja)

    if alvo == Decimal("65.97"):
        ok(f"alvo físico loja = {alvo}")
    else:
        fail(f"alvo físico={alvo} (esp 65,97)")

    if extra == Decimal("434.03"):
        ok(f"extra loja = {extra}")
    else:
        fail(f"extra={extra} (esp 434,03)")

    if liq == Decimal("254.51"):
        ok(f"acumulado líquido = {liq} (não 445,02)")
    else:
        fail(f"líquido={liq} (esp 254,51)")

    # bug antigo: sem ja_eletronico + elet_aplicado 0 → alvo 256,48 → liq 445,02
    calc_bug = {
        "alvos": {"cmv": "252.27", "lucro": "4.21", "fiado": 0},
        "ja_eletronico_aplicado": 0,
        "ja_enviado": {"total": "500"},
    }
    liq_bug = abater_extras_do_acumulado(timezone.localdate(), bruto, calc_bug)
    if liq_bug == Decimal("445.02"):
        ok("reproduz bug antigo 445,02 sem ja_eletronico (baseline)")
    else:
        fail(f"baseline bug={liq_bug}")

    if abs(float(meta.get("acumulado_tela_esperada_com_abate_correto") or 0) - 254.51) < 0.02:
        ok("snapshot espera ~254,51 com abate certo")
    else:
        fail("snapshot conta esperada diverge")

    # elet_aplicado parcial ainda ok se ja_eletronico presente
    calc_parcial = {
        "alvos": {"cmv": "300", "lucro": 0, "fiado": 0},
        "ja_eletronico": "200",
        "ja_eletronico_aplicado": "50",
        "ja_enviado": {"total": "0"},
    }
    if _alvo_fisico_de_calc(calc_parcial) == Decimal("100.00"):
        ok("com ja_eletronico, alvo = total − eletrônico (100)")
    else:
        fail(f"parcial alvo={_alvo_fisico_de_calc(calc_parcial)}")

    # --- PIN loja ---
    ok_pin, label_pin, err_pin = cu.operador_label_de_pin(PIN_LOJA)
    if ok_pin and label_pin and not err_pin:
        ok(f"PIN 9973 → operador {label_pin}")
    else:
        fail(f"PIN 9973 falhou ({err_pin})")

    # API acumulado (Client) — não grava nada
    c = Client(HTTP_HOST="127.0.0.1")
    # login staff se necessário: endpoint pode exigir sessão; tenta anônimo/autenticado
    from django.contrib.auth import get_user_model

    User = get_user_model()
    u = User.objects.filter(is_superuser=True).order_by("id").first()
    if u:
        c.force_login(u)
        ok(f"login force {u.username}")
    else:
        fail("sem superuser local para API")

    hoje = timezone.localdate().isoformat()
    r = c.get(f"/api/repasse-vila/acumulado/?data={hoje}")
    if r.status_code == 200:
        body = r.json()
        if body.get("ok") and "acumulado_anterior" in body and "acumulado_bruto" in body:
            ok("GET api acumulado 200 com bruto/líquido")
        else:
            fail(f"api acumulado body incompleto {list(body)[:8]}")
    else:
        fail(f"GET api acumulado {r.status_code}")

    # calcular_disponivel inclui ja_eletronico no retorno e líquido coerente
    calc = calcular_disponivel(timezone.localdate())
    if calc.get("ok") and "ja_eletronico" in calc:
        ok("calcular_disponivel expõe ja_eletronico")
    else:
        fail("calc sem ja_eletronico")

    bruto_c = _dec(calc.get("acumulado_bruto"))
    liq_c = _dec(calc.get("acumulado_anterior"))
    extra_c = _extra_do_calc(calc)
    if liq_c == (bruto_c - extra_c).quantize(Decimal("0.01")):
        ok("invariante local: líquido = bruto − extra")
    else:
        fail(f"invariante local liq={liq_c} bruto={bruto_c} extra={extra_c}")

    # snapshot deltas do dia 11
    deltas = snap.get("deltas_cache") or []
    d11 = next((d for d in deltas if d.get("data_ref") == "2026-09-11"), None)
    if d11 and float(d11.get("delta") or 0) == -434.03 and float(d11.get("enviado") or 0) == 500:
        ok("snapshot delta 11/09 = −434,03 com envio 500")
    else:
        fail(f"snapshot delta 11={d11}")

    print("---")
    print(f"oks={oks} fails={len(fails)}")
    if fails:
        for f in fails:
            print(f"  · {f}")
        return 1
    print("VERIFY_REPASSE_ACUM_EXTRA_BUG_PATH_OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
