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
        # API meta shape
        from django.test import Client
        from django.contrib.auth import get_user_model

        User = get_user_model()
        u = User.objects.filter(is_superuser=True).order_by("id").first()
        c = Client(HTTP_HOST="127.0.0.1")
        if u:
            c.force_login(u)
            r = c.get("/api/repasse-vila/meta/")
            if r.status_code == 200 and float(r.json().get("percentual_padrao")) == 0.0:
                ok("GET meta percentual_padrao=0")
            else:
                fail(f"meta {r.status_code} padrao={r.json().get('percentual_padrao') if r.status_code==200 else None}")
        else:
            fail("sem superuser para meta")
    finally:
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
