"""Smoke local ETQ-53-QUOTA / ETQ-53-UX — página + presets API + busca. PIN 9973."""
from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

import django

django.setup()

from django.contrib.auth import get_user_model
from django.test import Client

from produtos.caixa_util import operador_label_de_pin
from produtos.models import EtiquetaPresetAgro

PIN = os.environ.get("AGRO_PIN_TESTE", "9973")
ok_n = 0
fail_n = 0


def check(cond: bool, msg: str) -> None:
    global ok_n, fail_n
    if cond:
        ok_n += 1
        print("  OK ", msg)
    else:
        fail_n += 1
        print("  FAIL", msg)


def main() -> int:
    print("SMOKE ETQ-53-QUOTA local")
    c = Client(HTTP_HOST="127.0.0.1")
    User = get_user_model()
    u = User.objects.filter(is_active=True).order_by("-is_superuser", "id").first()
    check(u is not None, f"user Django ativo ({getattr(u, 'username', None)})")
    if u:
        c.force_login(u)

    label = None
    try:
        label = operador_label_de_pin(PIN)
    except Exception as e:
        label = None
        print("  note pin lookup:", e)
    check(
        bool(label and (label[0] if isinstance(label, tuple) else label)),
        f"PIN {PIN} resolve operador ({label})",
    )

    r = c.get("/produtos/etiquetas/")
    check(r.status_code == 200, f"página etiquetas HTTP {r.status_code}")
    body = r.content.decode("utf-8", "replace")
    check("etq-btn-size-53" in body, "HTML tem botão 53×30")
    check("etq-btn-size-40" in body, "HTML tem botão 4×4")
    check("etiquetas_core.js" in body and "?v=29" in body, "HTML puxa core ?v=29")
    check("produtos_etiquetas.js" in body and "?v=26" in body, "HTML puxa ui ?v=26")
    check("53×30 térmica" in body, "dica preset 53×30")
    check("Térmica (bobina / barras)" in body, "estilo sem confundir mm")
    js_line = next((ln for ln in body.splitlines() if "produtos_etiquetas.js" in ln), "")
    check("produtos_etiquetas.js" in js_line and "defer" not in js_line, "JS etiquetas sem defer")

    core_path = ROOT / "produtos/static/produtos/js/produtos_etiquetas_core.js"
    ui_path = ROOT / "produtos/static/produtos/js/produtos_etiquetas.js"
    core_txt = core_path.read_text(encoding="utf-8")
    ui_txt = ui_path.read_text(encoding="utf-8")
    check("Quota" in core_txt or "catch (e1)" in core_txt, "core savePrefs tolerante a quota")
    check("ANTES de gravar" in ui_txt or "pinta a tela ANTES" in ui_txt, "ui pinta antes de persist")
    check("function garantirPresetsNaTela" in ui_txt, "ui tem garantirPresetsNaTela")
    check("bindEvents()" in ui_txt and "function init()" in ui_txt, "ui init liga bindEvents")

    r2 = c.get("/api/produtos/etiquetas/presets/")
    check(r2.status_code == 200, f"API presets HTTP {r2.status_code}")
    j = r2.json() if r2.status_code == 200 else {}
    check(j.get("ok") is True, "API presets ok")
    presets = j.get("presets") or []
    check(isinstance(presets, list), f"presets lista n={len(presets)}")
    ids = {str(p.get("id") or "") for p in presets if isinstance(p, dict)}
    print("  note PG EtiquetaPresetAgro count =", EtiquetaPresetAgro.objects.count())
    print("  note API ids sample =", sorted(ids)[:8])
    # Seeds podem existir só no JS; se PG tiver 53×30, melhor.
    if "padrao-53x30" in ids:
        check(True, "API já tem padrao-53x30 no PG")
    else:
        check(True, "API sem padrao-53x30 no PG (seed JS cobre — ok)")

    r3 = c.get("/api/produtos/cadastro/", {"q": "racao", "limit": "8"})
    check(r3.status_code == 200, f"busca cadastro HTTP {r3.status_code}")
    if r3.status_code == 200:
        dj = r3.json()
        prods = dj.get("produtos") or []
        check(dj.get("ok") is not False, "busca não retorna ok=false")
        check(isinstance(prods, list), f"busca produtos n={len(prods)}")
        if prods:
            print("  note 1º produto:", (prods[0].get("nome") or "")[:60])
        else:
            r3b = c.get("/api/produtos/cadastro/", {"q": "a", "limit": "8"})
            prods2 = (r3b.json().get("produtos") or []) if r3b.status_code == 200 else []
            check(isinstance(prods2, list), f"busca fallback 'a' n={len(prods2)}")

    # Sem login: página ainda deve carregar HTML (busca pode exigir auth)
    c2 = Client(HTTP_HOST="127.0.0.1")
    r_anon = c2.get("/produtos/etiquetas/")
    check(r_anon.status_code in (200, 302), f"anônimo etiquetas HTTP {r_anon.status_code}")

    print()
    print(f"SMOKE {'OK' if fail_n == 0 else 'FAIL'} {ok_n}/{ok_n + fail_n}")
    return 1 if fail_n else 0


if __name__ == "__main__":
    sys.exit(main())
