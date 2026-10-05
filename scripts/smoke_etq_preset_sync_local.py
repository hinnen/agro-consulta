"""Smoke ETQ-PRESET-SYNC — API Postgres multi-PC (PIN 9973).

Simula PC A grava preset → PC B lista e vê a alteração.
"""
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
TEST_KEY = "preset-smoke-sync-9973"
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
    print("SMOKE ETQ-PRESET-SYNC local")
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

    # PC A — logado
    a = Client(HTTP_HOST="127.0.0.1")
    if u:
        a.force_login(u)

    r_page = a.get("/produtos/etiquetas/")
    check(r_page.status_code == 200, f"página etiquetas HTTP {r_page.status_code}")
    body = r_page.content.decode("utf-8", "replace")
    check("?v=31" in body and "etiquetas_core" in body, "HTML core ?v=31")
    check("?v=27" in body and "produtos_etiquetas.js" in body, "HTML ui ?v=27")

    core = (ROOT / "produtos/static/produtos/js/produtos_etiquetas_core.js").read_text(
        encoding="utf-8"
    )
    ui = (ROOT / "produtos/static/produtos/js/produtos_etiquetas.js").read_text(encoding="utf-8")
    check("servidor vence" in core or "Postgres manda" in core, "merge loja manda no core")
    check("!onServer[p.id]" in core, "migrate não sobrescreve")
    check("Arrastar posição tem que ir pro Postgres" in ui, "drag sync no UI")
    check("Presets da loja atualizados" in ui, "status pull loja")

    # Limpa lixo de smoke anterior
    EtiquetaPresetAgro.objects.filter(client_key=TEST_KEY).delete()

    payload = {
        "id": TEST_KEY,
        "nome": "SMOKE SYNC A",
        "estilo": "termica",
        "largura_mm": 53,
        "altura_mm": 30,
        "preco_pt": 19,
        "layout": {"nome": {"x": 1, "y": 2, "w": 90, "h": 20}},
    }
    r_post = a.post(
        "/api/produtos/etiquetas/presets/",
        data=__import__("json").dumps({"client_key": TEST_KEY, "nome": payload["nome"], "payload": payload}),
        content_type="application/json",
    )
    check(r_post.status_code == 200, f"PC A POST HTTP {r_post.status_code}")
    j_post = r_post.json() if r_post.status_code == 200 else {}
    check(j_post.get("ok") is True, "PC A POST ok")
    check(EtiquetaPresetAgro.objects.filter(client_key=TEST_KEY).exists(), "PG tem preset smoke")

    # PC B — outro client logado (simula outro terminal)
    b = Client(HTTP_HOST="127.0.0.1")
    if u:
        b.force_login(u)
    r_get = b.get("/api/produtos/etiquetas/presets/")
    check(r_get.status_code == 200, f"PC B GET HTTP {r_get.status_code}")
    j_get = r_get.json() if r_get.status_code == 200 else {}
    presets = j_get.get("presets") or []
    found = next((p for p in presets if str(p.get("id")) == TEST_KEY), None)
    check(found is not None, "PC B vê preset criado no A")
    if found:
        check(found.get("nome") == "SMOKE SYNC A", f"PC B nome={found.get('nome')!r}")
        check(Numberish(found.get("preco_pt")) == 19, f"PC B preco_pt={found.get('preco_pt')}")
        lay = found.get("layout") or {}
        nome_box = lay.get("nome") or {}
        check(Numberish(nome_box.get("x")) == 1, "PC B layout.nome.x da loja")

    # PC A altera → PC B vê alteração
    payload["nome"] = "SMOKE SYNC B"
    payload["preco_pt"] = 28
    payload["layout"] = {"nome": {"x": 7, "y": 8, "w": 80, "h": 18}}
    r_post2 = a.post(
        "/api/produtos/etiquetas/presets/",
        data=__import__("json").dumps(
            {"client_key": TEST_KEY, "nome": payload["nome"], "payload": payload}
        ),
        content_type="application/json",
    )
    check(r_post2.status_code == 200 and (r_post2.json() or {}).get("ok") is True, "PC A update ok")

    r_get2 = b.get("/api/produtos/etiquetas/presets/")
    found2 = next(
        (p for p in ((r_get2.json() or {}).get("presets") or []) if str(p.get("id")) == TEST_KEY),
        None,
    )
    check(found2 is not None and found2.get("nome") == "SMOKE SYNC B", "PC B vê nome atualizado")
    check(found2 is not None and Numberish(found2.get("preco_pt")) == 28, "PC B vê preco atualizado")
    lay2 = (found2 or {}).get("layout") or {}
    check(Numberish((lay2.get("nome") or {}).get("x")) == 7, "PC B vê layout atualizado")

    # Sem login não deve gravar na loja
    anon = Client(HTTP_HOST="127.0.0.1")
    r_anon = anon.post(
        "/api/produtos/etiquetas/presets/",
        data=__import__("json").dumps(
            {"client_key": TEST_KEY + "-anon", "nome": "X", "payload": {"id": TEST_KEY + "-anon"}}
        ),
        content_type="application/json",
    )
    check(r_anon.status_code in (302, 401, 403), f"anônimo POST bloqueado HTTP {r_anon.status_code}")

    # cleanup
    EtiquetaPresetAgro.objects.filter(client_key=TEST_KEY).delete()
    check(not EtiquetaPresetAgro.objects.filter(client_key=TEST_KEY).exists(), "cleanup smoke")

    print()
    print(f"SMOKE {'OK' if fail_n == 0 else 'FAIL'} {ok_n}/{ok_n + fail_n}")
    return 1 if fail_n else 0


def Numberish(v):
    try:
        return int(float(v))
    except Exception:
        return None


if __name__ == "__main__":
    sys.exit(main())
