"""Smoke ETQ-PRESET-ESPELHO — gestão/PDV/NF leem a mesma API (PIN 9973).

1) PC A grava preset no Postgres
2) GET (PC B) vê o mesmo
3) Páginas cadastro / PDV / etiquetas / NF puxam core + refresh no JS
"""
from __future__ import annotations

import json
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
TEST_KEY = "preset-smoke-espelho-9973"
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
    print("SMOKE ETQ-PRESET-ESPELHO local")
    User = get_user_model()
    u = User.objects.filter(is_active=True).order_by("-is_superuser", "id").first()
    check(u is not None, f"user Django ativo ({getattr(u, 'username', None)})")

    label = None
    try:
        label = operador_label_de_pin(PIN)
    except Exception as e:
        print("  note pin:", e)
    check(
        bool(label and (label[0] if isinstance(label, tuple) else label)),
        f"PIN {PIN} resolve ({label})",
    )

    core = (ROOT / "produtos/static/produtos/js/produtos_etiquetas_core.js").read_text(
        encoding="utf-8"
    )
    cad_js = (ROOT / "produtos/static/produtos/js/cadastro_erp_panel.js").read_text(
        encoding="utf-8"
    )
    pdv_js = (ROOT / "produtos/static/produtos/js/pdv_wizard.js").read_text(encoding="utf-8")
    nfe_html = (ROOT / "produtos/templates/produtos/entrada_nota.html").read_text(
        encoding="utf-8"
    )
    check("function refreshPresetsFromServer" in core, "core tem refreshPresetsFromServer")
    check("refreshPresetsFromServer" in cad_js, "cadastro JS chama refresh")
    check("refreshPresetsFromServer" in pdv_js, "PDV JS chama refresh")
    check("refreshPresetsFromServer" in nfe_html, "entrada NF chama refresh")

    a = Client(HTTP_HOST="127.0.0.1")
    if u:
        a.force_login(u)

    pages = [
        ("/produtos/etiquetas/", "etiquetas_core", "fila"),
        ("/produtos/cadastro-erp/", "etiquetas_core", "cadastro"),
        ("/pdv/", "etiquetas_core", "PDV"),
        ("/entrada-nota/", "etiquetas_core", "entrada NF"),
    ]
    for url, needle, label_pg in pages:
        r = a.get(url)
        body = r.content.decode("utf-8", "replace")
        check(r.status_code == 200, f"{label_pg} HTTP {r.status_code}")
        check(needle in body, f"{label_pg} inclui core etiquetas")

    EtiquetaPresetAgro.objects.filter(client_key=TEST_KEY).delete()
    payload = {
        "id": TEST_KEY,
        "nome": "ESPELHO GESTAO PDV",
        "estilo": "termica",
        "largura_mm": 53,
        "altura_mm": 30,
        "preco_pt": 33,
    }
    r_post = a.post(
        "/api/produtos/etiquetas/presets/",
        data=json.dumps({"client_key": TEST_KEY, "nome": payload["nome"], "payload": payload}),
        content_type="application/json",
    )
    check(r_post.status_code == 200, f"POST preset HTTP {r_post.status_code}")
    check((r_post.json() or {}).get("ok") is True, "POST ok")
    check(
        EtiquetaPresetAgro.objects.filter(client_key=TEST_KEY).exists(),
        "Postgres tem preset espelho",
    )

    b = Client(HTTP_HOST="127.0.0.1")
    if u:
        b.force_login(u)
    r_get = b.get("/api/produtos/etiquetas/presets/")
    check(r_get.status_code == 200, f"GET lista HTTP {r_get.status_code}")
    presets = (r_get.json() or {}).get("presets") or []
    found = next((p for p in presets if str(p.get("id")) == TEST_KEY), None)
    check(found is not None, "outro client vê o preset (espelho API)")
    if found:
        check(found.get("nome") == "ESPELHO GESTAO PDV", f"nome={found.get('nome')!r}")
        try:
            pt = int(float(found.get("preco_pt")))
        except Exception:
            pt = None
        check(pt == 33, f"preco_pt={found.get('preco_pt')}")

    # update → outro client vê
    payload["nome"] = "ESPELHO ATUALIZADO"
    payload["preco_pt"] = 44
    r_up = a.post(
        "/api/produtos/etiquetas/presets/",
        data=json.dumps({"client_key": TEST_KEY, "nome": payload["nome"], "payload": payload}),
        content_type="application/json",
    )
    check(r_up.status_code == 200 and (r_up.json() or {}).get("ok") is True, "update ok")
    r_get2 = b.get("/api/produtos/etiquetas/presets/")
    found2 = next(
        (p for p in ((r_get2.json() or {}).get("presets") or []) if str(p.get("id")) == TEST_KEY),
        None,
    )
    check(
        found2 is not None and found2.get("nome") == "ESPELHO ATUALIZADO",
        "espelho: update aparece no outro client",
    )

    EtiquetaPresetAgro.objects.filter(client_key=TEST_KEY).delete()
    check(not EtiquetaPresetAgro.objects.filter(client_key=TEST_KEY).exists(), "cleanup")

    print()
    print(f"SMOKE ESPELHO {'OK' if fail_n == 0 else 'FAIL'} {ok_n}/{ok_n + fail_n}")
    return 1 if fail_n else 0


if __name__ == "__main__":
    sys.exit(main())
