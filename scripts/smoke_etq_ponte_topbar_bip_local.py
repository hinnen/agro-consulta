"""Smoke local ETQ-PONTE-TOPBAR-BIP — página + bip EAN API (PIN 9973)."""
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

PIN = os.environ.get("AGRO_PIN_TESTE", "9973")
EAN = os.environ.get("AGRO_ETQ_SMOKE_EAN", "3000000001509")
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
    print("SMOKE ETQ-PONTE-TOPBAR-BIP local")
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
        print("  note pin lookup:", e)
    check(
        bool(label and (label[0] if isinstance(label, tuple) else label)),
        f"PIN {PIN} resolve operador ({label})",
    )

    r = c.get("/produtos/etiquetas/")
    check(r.status_code == 200, f"página etiquetas HTTP {r.status_code}")
    body = r.content.decode("utf-8", "replace")
    check("etq-btn-ponte" in body, "botão Ponte na topbar")
    check("etq-bridge-back" in body, "modal Ponte")
    check("etq-texto-rodape-global" not in body, "sem rodapé duplicado na fila")
    check("produtos_etiquetas.js" in body and "?v=33" in body, "JS ui ?v=33")
    check("abrirModalPonte" in (ROOT / "produtos/static/produtos/js/produtos_etiquetas.js").read_text(encoding="utf-8"), "JS tem abrirModalPonte")

    r2 = c.get("/api/produtos/cadastro/", {"q": EAN, "page": 1, "page_size": 10})
    check(r2.status_code == 200, f"API cadastro EAN HTTP {r2.status_code}")
    data = r2.json() if r2.status_code == 200 else {}
    check(data.get("ok") is True, f"API ok=true (erro={data.get('erro')})")
    prods = data.get("produtos") or []
    check(len(prods) >= 1, f"achou produto(s) n={len(prods)}")
    if prods:
        p0 = prods[0]
        cb = str(p0.get("codigo_barras") or "").replace(" ", "")
        check(cb == EAN or EAN in cb, f"código barras casa ({cb})")
        check(bool(p0.get("nome")), f"nome={p0.get('nome')}")

    print(f"SMOKE_OK {ok_n}/{ok_n + fail_n}" if fail_n == 0 else f"SMOKE_FAIL {fail_n} fails ({ok_n} ok)")
    return 1 if fail_n else 0


if __name__ == "__main__":
    raise SystemExit(main())
