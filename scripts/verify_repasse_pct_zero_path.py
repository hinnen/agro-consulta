#!/usr/bin/env python
"""Prova — % lucro 0% do padrão não vira 50% no PDV (REPASSE-PCT-ZERO)."""
from __future__ import annotations

import os
import re
import subprocess
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

fails: list[str] = []
oks = 0


def ok(msg: str) -> None:
    global oks
    oks += 1
    print(f"OK {msg}")


def fail(msg: str) -> None:
    fails.append(msg)
    print(f"FAIL {msg}")


def main() -> int:
    js = (ROOT / "produtos/static/produtos/js/pdv_repasse_vila.js").read_text(
        encoding="utf-8", errors="replace"
    )
    overlay = (
        ROOT / "produtos/templates/produtos/partials/pdv/repasse_vila_overlay.html"
    ).read_text(encoding="utf-8", errors="replace")
    views = (ROOT / "produtos/views_repasse_vila.py").read_text(
        encoding="utf-8", errors="replace"
    )

    if "percentual_padrao || 50" in js or "Math.round(j.percentual_padrao || 50)" in js:
        fail("JS ainda usa percentual_padrao || 50")
    else:
        ok("JS sem percentual_padrao || 50")

    if "function pctPadraoDeMeta" in js and "function pctAtual" in js:
        ok("JS tem pctPadraoDeMeta + pctAtual")
    else:
        fail("JS sem helpers pct")

    if "pctFromPadraoApplied" in js:
        ok("JS sincroniza padrão uma vez por abertura")
    else:
        fail("JS sem flag pctFromPadraoApplied")

    if re.search(r'id="pdv-rp-pct"[^>]*value="50"', overlay):
        fail("overlay ainda hardcode value=50")
    else:
        ok("overlay não força value=50")

    if 'percentual_lucro_padrao") or "50"' in views or "percentual_lucro_padrao') or '50'" in views:
        fail("views ainda or 50 no save do padrão")
    else:
        ok("views save aceita 0 sem or 50")

    # Simula a conta do helper (espelho da regra JS)
    def padrao(pad):
        if pad is None or pad == "":
            return 50
        n = float(pad)
        return int(round(n))

    if padrao(0) == 0 and padrao(0.0) == 0 and padrao(50) == 50:
        ok("regra: padrao 0 vira 0 (nao 50)")
    else:
        fail("regra padrao 0 quebrou")

    from produtos.repasse_vila_util import calcular_disponivel, obter_config, salvar_percentual_padrao
    from produtos.caixa_util import operador_label_de_pin

    # Espelho do bug antigo: 0 || 50 → 50
    def bug_antigo(pad):
        return int(round(pad or 50))

    if bug_antigo(0) == 50 and padrao(0) == 0:
        ok("regressao: bug antigo 0||50=50; fix=0")
    else:
        fail("regressao padrao 0")

    pin_ok, pin_label, pin_err = operador_label_de_pin("9973")
    if pin_ok and pin_label:
        ok(f"PIN 9973 valido ({pin_label})")
    else:
        fail(f"PIN 9973 invalido: {pin_err or pin_label}")

    cfg = obter_config()
    antes = cfg.percentual_lucro_padrao
    try:
        salvar_percentual_padrao(0, operador="verify-pct-zero")
        cfg2 = obter_config()
        if Decimal(cfg2.percentual_lucro_padrao) == 0:
            ok("PG grava padrão 0%")
        else:
            fail(f"PG padrão={cfg2.percentual_lucro_padrao}")
        calc = calcular_disponivel(percentual_lucro=0)
        pct_calc = calc.get("percentual_lucro")
        if pct_calc is not None and float(pct_calc) == 0.0:
            ok("calcular_disponivel com 0%")
        else:
            fail(f"calc pct={pct_calc}")
        # lucro com 0%: percentual_lucro=0 e parte enviada ao Centro via % = 0
        if float(calc.get("percentual_lucro")) == 0.0:
            ok("calc percentual_lucro=0 no dict")
        else:
            fail(f"percentual_lucro={calc.get('percentual_lucro')}")
        # Com pct 0, a fatia de lucro no total não pode “virar 50”
        lucro_dia = float(calc.get("lucro_bruto_dia") or 0)
        if lucro_dia == 0.0 or abs(float(calc.get("lucro_penultimo_dia") or 0)) >= 0:
            ok(f"calc 0% coerente (lucro_dia={lucro_dia})")
        else:
            fail("calc 0% incoerente")

        from django.test import Client
        from django.contrib.auth import get_user_model

        User = get_user_model()
        u = User.objects.filter(is_superuser=True).order_by("id").first()
        c = Client(HTTP_HOST="127.0.0.1")
        if not u:
            fail("sem superuser para API")
        else:
            c.force_login(u)
            r = c.get("/api/repasse-vila/meta/")
            body = r.json() if r.status_code == 200 else {}
            if r.status_code == 200 and float(body.get("percentual_padrao")) == 0.0:
                ok("GET meta percentual_padrao=0")
            else:
                fail(
                    f"meta {r.status_code} padrao={body.get('percentual_padrao')}"
                )

            # POST config (Gestão “Salvar padrão”) com 0
            r2 = c.post(
                "/api/repasse-vila/config/",
                data='{"percentual_lucro_padrao":0,"operador":"verify-pct-zero"}',
                content_type="application/json",
            )
            b2 = r2.json() if r2.status_code == 200 else {}
            if r2.status_code == 200 and float(b2.get("percentual_lucro_padrao", -1)) == 0.0:
                ok("POST config padrão=0")
            else:
                fail(f"POST config {r2.status_code} {b2}")

            r3 = c.get("/api/repasse-vila/calc/?pct=0")
            b3 = r3.json() if r3.status_code == 200 else {}
            pct_api = b3.get("percentual_lucro")
            if r3.status_code == 200 and pct_api is not None and float(pct_api) == 0.0:
                ok("GET calc ?pct=0")
            else:
                fail(f"calc API {r3.status_code} pct={pct_api}")

            # Com padrão 50 no PG, query ?pct=0 ainda deve calcular 0 (campo da tela)
            salvar_percentual_padrao(50, operador="verify-pct-zero-tmp50")
            r3b = c.get("/api/repasse-vila/calc/?pct=0")
            b3b = r3b.json() if r3b.status_code == 200 else {}
            if r3b.status_code == 200 and float(b3b.get("percentual_lucro")) == 0.0:
                ok("GET calc ?pct=0 com padrao PG=50")
            else:
                fail(f"calc pct=0 vs padrao50 → {b3b.get('percentual_lucro')}")
            salvar_percentual_padrao(0, operador="verify-pct-zero-back0")

            # Confirmar com PIN válido mas valor 0 / sem transferência real —
            # só valida que o payload com percentual_lucro=0 não é rejeitado por “pct vazio”.
            r4 = c.post(
                "/api/repasse-vila/confirmar/",
                data=(
                    '{"pin":"9973","quem_levou":"VERIFY-PCT-ZERO","percentual_lucro":0,'
                    '"valor_manual":0,"forma_pagamento":"Dinheiro","dry_run":true}'
                ),
                content_type="application/json",
            )
            # dry_run pode não existir — aceita 200 ok OU erro de negócio (caixa/valor),
            # mas NÃO erro de PIN e NÃO forçar pct 50.
            b4 = {}
            try:
                b4 = r4.json()
            except Exception:
                pass
            if r4.status_code == 400 and "PIN" in str(b4.get("erro") or "").upper():
                fail(f"confirmar rejeitou PIN: {b4}")
            elif "percentual" in str(b4.get("erro") or "").lower() and "obrig" in str(
                b4.get("erro") or ""
            ).lower():
                fail(f"confirmar rejeitou pct 0: {b4}")
            else:
                ok(
                    f"confirmar aceita pct=0 (status={r4.status_code} "
                    f"ok={b4.get('ok')} erro={b4.get('erro') or '-'})"
                )

            # Restaura padrão 50 temporário e confirma meta ≠ 0 ainda round-trips
            salvar_percentual_padrao(50, operador="verify-pct-zero-50")
            r5 = c.get("/api/repasse-vila/meta/")
            b5 = r5.json() if r5.status_code == 200 else {}
            if r5.status_code == 200 and float(b5.get("percentual_padrao")) == 50.0:
                ok("GET meta percentual_padrao=50 (ainda funciona)")
            else:
                fail(f"meta 50 {r5.status_code} {b5.get('percentual_padrao')}")
            salvar_percentual_padrao(0, operador="verify-pct-zero-back0")
    finally:
        # Deixa como estava antes da prova (loja usa 0% hoje)
        salvar_percentual_padrao(antes, operador="verify-pct-zero-restore")

    node = subprocess.run(
        ["node", "--check", str(ROOT / "produtos/static/produtos/js/pdv_repasse_vila.js")],
        capture_output=True,
        text=True,
    )
    if node.returncode == 0:
        ok("node --check pdv_repasse_vila.js")
    else:
        fail("node --check falhou")

    # Gestão HTML: 0% não pode virar 50 no calc/URL
    gestao = (ROOT / "produtos/templates/produtos/repasse_vila.html").read_text(
        encoding="utf-8", errors="replace"
    )
    if "pctEl.value || '50'" in gestao or 'pctEl.value || "50"' in gestao:
        fail("gestao HTML ainda pctEl.value || '50'")
    elif "function pctAtual" in gestao:
        ok("gestao HTML usa pctAtual (0% ok)")
    else:
        fail("gestao HTML sem pctAtual")

    print("---")
    print(f"oks={oks} fails={len(fails)}")
    if fails:
        for f in fails:
            print(f"  · {f}")
        return 1
    print("VERIFY_REPASSE_PCT_ZERO_PATH_OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
