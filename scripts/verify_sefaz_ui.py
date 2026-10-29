#!/usr/bin/env python
"""Smoke: Entrada NF aba SEFAZ limpa (SEFAZ-UI) + convivencia DFE-CIENCIA / CP-DUP-BACKUP.

Os tres pacotes mexem no mesmo `entrada_nota.html`; este verify garante que os
marcadores dos tres continuam vivos no mesmo arquivo. VERIFY_OK / VERIFY_FAIL.
"""
from __future__ import annotations

import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TPL = os.path.join(ROOT, "produtos", "templates", "produtos", "entrada_nota.html")

_okc = 0


def fail(msg: str) -> None:
    print(f"VERIFY_FAIL: {msg}")
    sys.exit(1)


def ok(msg: str) -> None:
    global _okc
    _okc += 1
    print(f"  OK {msg}")


def main() -> None:
    html = open(TPL, encoding="utf-8").read()

    if "\u251c" in html or "\u00c3\u00a7" in html:
        fail("mojibake no entrada_nota.html")
    ok("sem mojibake")

    # --- SEFAZ-UI: painel enxuto + ajuda no ? ---
    for need in (
        'id="panel-sefaz"',
        'data-help-w="sefaz"',
        "entrada-nfe-ajuda-sefaz",
        'id="sefaz-status"',
        'id="btn-sefaz-refresh-meta"',
        'id="btn-dist-dfe"',
        'id="btn-dfe-chave"',
        'id="sefaz-chave"',
        'id="sefaz-ult"',
        'id="btn-dfe-aba-pendentes"',
        'id="btn-dfe-aba-concluidas"',
    ):
        if need not in html:
            fail(f"SEFAZ-UI: falta {need}")
    ok("marcadores do painel SEFAZ")

    # ultNSU (tecnico) fica dentro de <details> Avancado
    m = re.search(r"<details[^>]*>(?:(?!</details>).)*?Avan\u00e7ado(?:(?!</details>).)*?</details>", html, re.S)
    if not m or 'id="sefaz-ult"' not in m.group(0):
        fail("SEFAZ-UI: ultNSU fora do bloco Avancado")
    ok("ultNSU recolhido em Avancado")

    # ajuda longa existe no modal de ajuda
    if "help-w" not in html or "Dar ci\u00eancia e buscar XML" not in html:
        fail("SEFAZ-UI: ajuda ? sem o texto da ciencia")
    ok("texto longo mora no ?")

    # --- DFE-CIENCIA: botao + chip So resumo ---
    for need in ("btn-dfe-ciencia", "S\u00f3 resumo", "Buscar XML"):
        if need not in html:
            fail(f"DFE-CIENCIA: falta {need}")
    if html.count("btn-dfe-ciencia") < 2:
        fail("DFE-CIENCIA: botao sem handler (so 1 ocorrencia)")
    ok("DFE-CIENCIA convive no mesmo template")

    # --- CP-DUP-BACKUP: trava de duplo clique no financeiro ---
    for need in ("__entradaNfeFinSalvando", "btn-salvar-fin"):
        if need not in html:
            fail(f"CP-DUP-BACKUP: falta {need}")
    if html.count("__entradaNfeFinSalvando") < 3:
        fail("CP-DUP-BACKUP: trava sem reset (set/clear)")
    ok("CP-DUP-BACKUP convive no mesmo template")

    # ids unicos (colisao classica de merge dos 3 pacotes)
    for eid in (
        "panel-sefaz",
        "sefaz-status",
        "btn-sefaz-refresh-meta",
        "btn-dist-dfe",
        "btn-dfe-chave",
        "sefaz-chave",
        "sefaz-ult",
        "btn-dfe-aba-pendentes",
        "btn-dfe-aba-concluidas",
    ):
        n = len(re.findall(rf'id="{eid}"', html))
        if n != 1:
            fail(f"id {eid} aparece {n}x (merge duplicou bloco)")
    ok("ids unicos no template")

    # --- tela responde 200 ---
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
    if ROOT not in sys.path:
        sys.path.insert(0, ROOT)
    import django

    django.setup()
    from django.contrib.auth import get_user_model
    from django.test import Client
    from django.urls import reverse

    User = get_user_model()
    user = User.objects.filter(is_superuser=True).first() or User.objects.first()
    if user is None:
        fail("sem usuario no banco local")
    c = Client(headers={"host": "127.0.0.1"})
    c.force_login(user)
    r = c.get(reverse("entrada_nota"))
    if r.status_code != 200:
        fail(f"GET /entrada-nota/ -> {r.status_code}")
    corpo = r.content.decode("utf-8", "replace")
    for need in ('id="panel-sefaz"', 'data-help-w="sefaz"', "__entradaNfeFinSalvando"):
        if need not in corpo:
            fail(f"render sem {need}")
    ok("GET /entrada-nota/ 200 com os 3 pacotes renderizados")

    print(f"VERIFY_OK {_okc} checks")


if __name__ == "__main__":
    main()
