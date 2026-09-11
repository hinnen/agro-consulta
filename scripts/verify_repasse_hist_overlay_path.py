#!/usr/bin/env python
"""Prova — histórico overlay Repasse PDV (REPASSE-HIST-OVERLAY)."""
from __future__ import annotations

import os
import re
import subprocess
import sys
from datetime import timedelta
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
    from django.contrib.auth import get_user_model
    from django.test import Client
    from django.urls import reverse
    from django.utils import timezone

    from produtos.repasse_vila_util import listar_envios_periodo, resumo_cofrinho_vila

    overlay = (
        ROOT / "produtos/templates/produtos/partials/pdv/repasse_vila_overlay.html"
    ).read_text(encoding="utf-8", errors="replace")
    js = (ROOT / "produtos/static/produtos/js/pdv_repasse_vila.js").read_text(
        encoding="utf-8", errors="replace"
    )
    urls = (ROOT / "produtos/urls.py").read_text(encoding="utf-8", errors="replace")

    for bid in ("pdv-rp-hist-sal", "pdv-rp-hist-ve", "pdv-rp-hist-centro"):
        if f'id="{bid}"' in overlay:
            ok(f"botao {bid}")
        else:
            fail(f"sem botao {bid}")

    if 'id="pdv-rp-hist-modal"' in overlay and "rp-hist-shell" in overlay:
        ok("popup hist mesmo tamanho (rp-hist-shell)")
    else:
        fail("sem hist modal/shell")

    if 'id="pdv-rp-hist-voltar"' in overlay and "Voltar" in overlay:
        ok("botao Voltar")
    else:
        fail("sem Voltar")

    if 'id="pdv-rp-hist-imprimir"' in overlay and "pdv-rp-hist-print-80" in overlay:
        ok("imprimir 80mm/A4 no markup")
    else:
        fail("sem painel impressao")

    if 'class="agro-date-picker' in overlay and "pdv-rp-hist-de" in overlay:
        ok("AgroDatePicker de/ate")
    else:
        fail("sem calendário de/ate")

    if "openHistModal" in js and "printHist" in js and "rp-hist-table" in js:
        ok("JS openHist + print + tabela colunas")
    else:
        fail("JS hist incompleto")

    if "fmtHistQuando" in js and "rp-hist-col-data" in js and "histDirecao" in js:
        ok("colunas + setas entrada/saida")
    else:
        fail("sem colunas/setas hist")

    if "pdv-rp-hist-print-iframe" in js and "window.open(" not in js.split("printHist")[1][:2500]:
        ok("impressao via iframe (sem window.open)")
    elif "ensureHistPrintIframe" in js and "contentWindow.print" in js:
        ok("impressao via iframe (sem window.open)")
    else:
        fail("print ainda usa window.open ou sem iframe")

    if "api/repasse-vila/envios/" in urls:
        ok("rota envios")
    else:
        fail("sem rota envios")

    hoje = timezone.localdate()
    de = hoje - timedelta(days=90)
    cof = resumo_cofrinho_vila(hoje, cofre="salario", de=de, ate=hoje, limit=50)
    if cof.get("ok") and "movimentos" in cof:
        ok(f"resumo_cofrinho de/ate ({len(cof['movimentos'])} mov)")
    else:
        fail("resumo_cofrinho de/ate")

    env = listar_envios_periodo(de, hoje, limit=50)
    if env.get("ok") and "envios" in env:
        ok(f"listar_envios_periodo ({len(env['envios'])} envios)")
    else:
        fail("listar_envios_periodo")

    User = get_user_model()
    u = User.objects.filter(is_superuser=True).order_by("id").first()
    c = Client(HTTP_HOST="127.0.0.1")
    if not u:
        fail("sem superuser")
    else:
        c.force_login(u)
        r1 = c.get(
            "/api/repasse-vila/cofrinho/",
            {"cofre": "salario", "de": de.isoformat(), "ate": hoje.isoformat(), "limit": 20},
        )
        if r1.status_code == 200 and r1.json().get("ok"):
            ok("GET cofrinho?de&ate")
        else:
            fail(f"GET cofrinho {r1.status_code}")

        r2 = c.get(
            "/api/repasse-vila/envios/",
            {"de": de.isoformat(), "ate": hoje.isoformat(), "limit": 20},
        )
        if r2.status_code == 200 and r2.json().get("ok"):
            ok("GET envios?de&ate")
        else:
            fail(f"GET envios {r2.status_code}")

        try:
            name = reverse("api_repasse_vila_envios")
            ok(f"reverse envios={name}")
        except Exception as e:
            fail(f"reverse envios: {e}")

    node = subprocess.run(
        ["node", "--check", str(ROOT / "produtos/static/produtos/js/pdv_repasse_vila.js")],
        capture_output=True,
        text=True,
    )
    if node.returncode == 0:
        ok("node --check")
    else:
        fail("node --check falhou")

    # stack: hist é filho do overlay
    if re.search(r'id="pdv-rp-hist-modal"[^>]*class="[^"]*rp-popup', overlay):
        ok("hist é rp-popup (stack nest)")
    else:
        fail("hist nao é rp-popup")

    print("---")
    print(f"oks={oks} fails={len(fails)}")
    if fails:
        for f in fails:
            print(f"  · {f}")
        return 1
    print("VERIFY_REPASSE_HIST_OVERLAY_PATH_OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
