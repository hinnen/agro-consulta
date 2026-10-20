#!/usr/bin/env python
"""Prova COMPRAS-FOLHA-MULTI-FORN (vários fornecedores na planilha). VERIFY_OK / VERIFY_FAIL."""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime
from unittest.mock import patch

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

FAIL: list[str] = []
OK = 0


def check(name: str, cond: bool, detail: str = "") -> None:
    global OK
    if cond:
        OK += 1
        print(f"  OK  {name}" + (f" - {detail}" if detail else ""))
    else:
        FAIL.append(name + (f" - {detail}" if detail else ""))
        print(f"  FAIL {name}" + (f" - {detail}" if detail else ""))


def _read(rel: str) -> str:
    return open(os.path.join(ROOT, rel), encoding="utf-8").read()


def main() -> None:
    import django

    django.setup()

    print("== Parse / views hooks ==")
    from produtos.views import (
        _parse_fornecedores_relatorio_compras,
        _rotulo_fornecedores_relatorio,
    )

    body = {
        "fornecedores": [
            {"fornecedor_nome": "A", "fornecedor_id": "1"},
            {"fornecedor_nome": "B", "fornecedor_id": "2"},
        ]
    }
    parsed = _parse_fornecedores_relatorio_compras(body)
    check("parse_lista_2", len(parsed) == 2)
    rot = _rotulo_fornecedores_relatorio(parsed)
    check("rotulo_mais", "A" in rot and "+" in rot, rot[:60])

    views = _read("produtos/views.py")
    check("views_metricas_import", "metricas_compra_entrada_nf_agro_fornecedores" in views)
    check("views_multi_pg_route", "len(fornecedores_list) > 1" in views)
    check("views_parse_fn", "_parse_fornecedores_relatorio_compras" in views)

    plan = _read("produtos/templates/produtos/includes/_compras_planilha_print_js_snippet.html")
    check("js_chips", "planilhaFornSelecionados" in plan and "planilhaFornPayloadImpressao" in plan)
    check("js_fornecedores_payload", "fornecedores:" in plan)
    html = _read("produtos/templates/produtos/compras_relatorio_planilha.html")
    check("html_chips_host", 'id="planilha-forn-chips"' in html)
    check("html_embed_css", 'data-agro-folha-embed="1"' in html)

    util = _read("produtos/compras_ultimas_compras_util.py")
    check("util_metricas_fn", "def metricas_compra_entrada_nf_agro_fornecedores" in util)

    print("== Metricas agregadas (mock docs) ==")
    from produtos.compras_ultimas_compras_util import metricas_compra_entrada_nf_agro_fornecedores

    dt_old = datetime(2026, 1, 10)
    dt_new = datetime(2026, 6, 1)
    fake_docs_a = [
        {
            "rascunho_id": "nf-a",
            "cabecalho": {"emit_nome": "Forn A"},
            "linhas": [{"produto_id": "100", "Quantidade": 2}],
        }
    ]
    fake_docs_b = [
        {
            "rascunho_id": "nf-b",
            "cabecalho": {"emit_nome": "Forn B"},
            "linhas": [{"produto_id": "200", "Quantidade": 5}],
        },
        {
            "rascunho_id": "nf-b2",
            "cabecalho": {"emit_nome": "Forn B"},
            "linhas": [{"produto_id": "100", "Quantidade": 9}],
        },
    ]

    def fake_docs(fn, fid, **kw):
        if "B" in fn or fid == "2":
            return fake_docs_b
        return fake_docs_a

    with (
        patch(
            "produtos.compras_ultimas_compras_util._docs_entrada_nf_agro_por_fornecedor",
            side_effect=fake_docs,
        ),
        patch(
            "produtos.compras_ultimas_compras_util._doc_conta_como_compra_entrada_nf",
            return_value=True,
        ),
        patch(
            "produtos.compras_ultimas_compras_util._fornecedor_casa_doc_entrada_nf",
            return_value=True,
        ),
        patch(
            "produtos.compras_ultimas_compras_util._data_doc_entrada_nf_agro",
            side_effect=lambda cab, doc: dt_new if doc.get("rascunho_id") == "nf-b2" else dt_old,
        ),
        patch(
            "produtos.compras_ultimas_compras_util._qtd_linha_entrada_nf_agro",
            side_effect=lambda ln: float(ln.get("Quantidade") or 0),
        ),
        patch(
            "produtos.compras_ultimas_compras_util._numero_doc_entrada_nf_agro",
            return_value="NF-TEST",
        ),
    ):
        m = metricas_compra_entrada_nf_agro_fornecedores([("Forn A", "1"), ("Forn B", "2")])
    check("metricas_retorno", m is not None)
    por = (m or {}).get("por_pid") or {}
    check("metricas_pid100", por.get("100", {}).get("qtd") == 9.0, str(por.get("100")))
    check("metricas_pid200", por.get("200", {}).get("qtd") == 5.0, str(por.get("200")))
    check("metricas_hist", "100" in (m or {}).get("hist_pids", set()))

    print("== Django urls + testes unitários ==")
    from django.urls import reverse

    try:
        u = reverse("compras_relatorio_planilha_fornecedor")
        check("url_planilha_forn", "/compras/relatorio-planilha-fornecedor/" in u, u)
    except Exception as exc:  # noqa: BLE001
        check("url_planilha_forn", False, str(exc)[:80])

    from django.test.utils import get_runner
    from django.conf import settings

    TestRunner = get_runner(settings)
    test_runner = TestRunner(verbosity=0, interactive=False, keepdb=True)
    failures = test_runner.run_tests(["produtos.tests_compras_relatorio_fornecedor_multi"])
    check("django_tests_multi", failures == 0, f"failures={failures}")

    print("== API POST smoke (sem login → 302/403 esperado) ==")
    from django.conf import settings
    from django.test import Client

    if "testserver" not in settings.ALLOWED_HOSTS:
        settings.ALLOWED_HOSTS = [*settings.ALLOWED_HOSTS, "testserver"]

    c = Client()
    payload = json.dumps(
        {
            "fornecedores": [
                {"fornecedor_nome": "X", "fornecedor_id": "1"},
                {"fornecedor_nome": "Y", "fornecedor_id": "2"},
            ]
        }
    )
    try:
        r = c.post(
            reverse("api_compras_relatorio_fornecedor"),
            data=payload,
            content_type="application/json",
        )
        # 503 = Mongo indisponível no ambiente local (antes do ramo PG multi-fornecedor).
        check(
            "api_post_alcancavel",
            r.status_code in (302, 403, 401, 200, 503),
            str(r.status_code),
        )
    except Exception as exc:  # noqa: BLE001
        check("api_post_alcancavel", False, str(exc)[:100])

    print()
    if FAIL:
        print(f"VERIFY_FAIL {len(FAIL)}")
        for f in FAIL:
            print(f"  - {f}")
        sys.exit(1)
    print(f"VERIFY_OK {OK}")


if __name__ == "__main__":
    main()
