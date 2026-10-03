"""Smoke local ETQ-53-UX — página + presets API + busca. PIN loja 9973 se precisar sessão PDV."""
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

PIN = "9973"
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
    print("SMOKE ETQ-53-UX local")
    c = Client(HTTP_HOST="127.0.0.1")
    User = get_user_model()
    u = User.objects.filter(is_active=True).order_by("-is_superuser", "id").first()
    check(u is not None, f"user Django ativo ({getattr(u, 'username', None)})")
    if u:
        c.force_login(u)

    # PIN 9973 = Renan (só confirma que o PIN resolve operador)
    label = None
    try:
        label = operador_label_de_pin(PIN)
    except Exception as e:
        label = None
        print("  note pin lookup:", e)
    check(bool(label and (label[0] if isinstance(label, tuple) else label)), f"PIN {PIN} resolve operador ({label})")

    r = c.get("/produtos/etiquetas/")
    check(r.status_code == 200, f"página etiquetas HTTP {r.status_code}")
    body = r.content.decode("utf-8", "replace")
    check("etq-btn-size-53" in body, "HTML tem botão 53×30")
    check("etq-btn-size-40" in body, "HTML tem botão 4×4")
    check("?v=28" in body and "etiquetas_core" in body, "HTML puxa core ?v=28")
    check("?v=25" in body and "produtos_etiquetas.js" in body, "HTML puxa ui ?v=25")
    check("53×30 térmica" in body, "dica preset 53×30")
    check("Térmica (bobina / barras)" in body, "estilo sem confundir mm")
    js_line = next((ln for ln in body.splitlines() if "produtos_etiquetas.js" in ln), "")
    check("produtos_etiquetas.js" in js_line and "defer" not in js_line, "JS etiquetas sem defer")

    r2 = c.get("/api/produtos/etiquetas/presets/")
    check(r2.status_code == 200, f"API presets HTTP {r2.status_code}")
    j = r2.json() if r2.status_code == 200 else {}
    check(j.get("ok") is True, "API presets ok")
    presets = j.get("presets") or []
    check(isinstance(presets, list), f"presets lista n={len(presets)}")
    # Seeds vêm do JS; PG pode estar vazio — isso é ok. Conta PG só informativa.
    print("  note PG EtiquetaPresetAgro count =", EtiquetaPresetAgro.objects.count())

    r3 = c.get("/api/produtos/cadastro/", {"q": "teste", "limit": "8"})
    check(r3.status_code == 200, f"busca cadastro HTTP {r3.status_code}")
    if r3.status_code == 200:
        dj = r3.json()
        prods = dj.get("produtos") or []
        check(dj.get("ok") is not False, "busca não retorna ok=false")
        check(isinstance(prods, list), f"busca produtos n={len(prods)}")
        if prods:
            print("  note 1º produto:", (prods[0].get("nome") or "")[:60])

    print()
    print(f"SMOKE {'OK' if fail_n == 0 else 'FAIL'} {ok_n}/{ok_n + fail_n}")
    return 1 if fail_n else 0


if __name__ == "__main__":
    sys.exit(main())
