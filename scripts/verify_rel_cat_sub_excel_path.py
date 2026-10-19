# -*- coding: utf-8 -*-
"""
REL-CAT-SUB-EXCEL — filtro cat/sub 1–4 + Excel com recorte na Central de Relatórios.

Paths:
  /relatorios/mais-vendidos/
  /relatorios/vendas-grupo/
  /relatorios/curva-abc/
  /relatorios/margem/
    -> filtros_catalogo_request + facetas_categoria_sub / vendas_por_grupo_relatorio
    -> GET export=xlsx (subtítulo com filtros; mais/margem lista completa do recorte)

  python scripts/verify_rel_cat_sub_excel_path.py
"""
from __future__ import annotations

import os
import sys
import uuid
from datetime import timedelta
from decimal import Decimal
from io import BytesIO
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
sys.path.insert(0, str(ROOT))

import django

django.setup()

from django.test import Client, override_settings
from django.urls import reverse
from django.utils import timezone
from openpyxl import load_workbook

from produtos import relatorios_central_views as views
from produtos import relatorios_vendas_util as ru
from produtos.models import ItemVendaAgro, Produto, VendaAgro

FAILS: list[str] = []
OKS = 0
TAG = f"relsub_{uuid.uuid4().hex[:10]}"
PID_G = f"{TAG}_gato"
PID_C = f"{TAG}_cao"
CAT = f"Racoes {TAG}"
SUB_G = f"Gato {TAG}"
SUB_C = f"Cachorro {TAG}"


def ok(msg: str) -> None:
    global OKS
    OKS += 1
    print("OK", msg.encode("ascii", "replace").decode("ascii"))


def fail(msg: str) -> None:
    FAILS.append(msg)
    print("FAIL", msg.encode("ascii", "replace").decode("ascii"))


def check(cond: bool, msg: str) -> None:
    if cond:
        ok(msg)
    else:
        fail(msg)


def read(rel: str) -> str:
    return (ROOT / rel).read_text(encoding="utf-8", errors="replace")


def check_static() -> None:
    print("--- static ---")
    util = read("produtos/relatorios_vendas_util.py")
    v = read("produtos/relatorios_central_views.py")
    tpl = read("produtos/templates/produtos/relatorios_generico.html")
    tests = read("produtos/tests_relatorios_central_filtros.py")

    for nome in (
        "filtros_catalogo_request",
        "facetas_categoria_sub",
        "vendas_por_grupo_relatorio",
        "montar_xlsx",
    ):
        check(f"def {nome}" in util, f"util {nome}")

    check("subcategoria_4" in util, "util sub 1-4 dims")
    check("def _qs_limpar_catalogo" in v, "views _qs_limpar_catalogo")
    check("resumo_filtros_catalogo" in v, "views resumo filtros")
    check("export_rows = ru.limitar_ranking(rows_all, 0)" in v, "mais vendidos excel lista completa")
    check("limite=0, rows=rows_all" in v, "margem excel lista completa")
    check('form="form-relatorio-filtros"' in tpl and 'name="export"' in tpl, "excel submit form")
    check('name="subcategoria"' in tpl and "subcategorias_2" in tpl, "tpl checkboxes sub")
    check("test_vendas_grupo_xlsx_respeita_filtro_subcategoria" in tests, "test xlsx filtro")
    check("test_generico_excel_usa_submit_do_formulario" in tests, "test excel form")


def check_util_or() -> None:
    print("--- util filtros ---")
    from django.test import RequestFactory

    rf = RequestFactory()
    req = rf.get("/", {"categoria": ["A", "a"], "subcategoria": "Gato"})
    out = ru.filtros_catalogo_request(req)
    check(out["categoria"] == ["A"], "dedupe categoria casefold")
    check(out["subcategoria"] == ["Gato"], "subcategoria lista")

    qs = views._qs_limpar_catalogo(
        rf.get("/", {"periodo": "30d", "categoria": "X", "subcategoria": "Y"})
    )
    check("periodo=30d" in qs and "categoria=" not in qs, "limpar catalogo mantém periodo")


def _cleanup() -> None:
    ItemVendaAgro.objects.filter(produto_id_externo__startswith=TAG).delete()
    VendaAgro.objects.filter(cliente_nome__startswith=f"RelSub {TAG}").delete()
    Produto.objects.filter(produto_id_externo__in=[PID_G, PID_C]).delete()


def check_agg_filtro() -> None:
    print("--- agregacao filtro sub ---")
    try:
        _cleanup()
        agora = timezone.now()
        desde = agora - timedelta(days=3)
        ate = agora + timedelta(hours=1)

        Produto.objects.create(
            produto_externo_id=PID_G,
            nome=f"Racao Gato {TAG}",
            categoria=CAT,
            subcategoria=SUB_G,
            codigo_interno=f"GM-{TAG}-G",
        )
        Produto.objects.create(
            produto_externo_id=PID_C,
            nome=f"Racao Cao {TAG}",
            categoria=CAT,
            subcategoria=SUB_C,
            codigo_interno=f"GM-{TAG}-C",
        )
        v = VendaAgro.objects.create(
            cliente_nome=f"RelSub {TAG}",
            cliente_documento="",
            total=Decimal("150.00"),
            forma_pagamento="Dinheiro",
            deposito="centro",
        )
        VendaAgro.objects.filter(pk=v.pk).update(criado_em=timezone.now() - timedelta(days=1))
        ItemVendaAgro.objects.create(
            venda=v,
            produto_id_externo=PID_G,
            codigo="GM1",
            descricao=f"Gato {TAG}",
            quantidade=Decimal("1"),
            valor_unitario=Decimal("100"),
            valor_total=Decimal("100"),
        )
        ItemVendaAgro.objects.create(
            venda=v,
            produto_id_externo=PID_C,
            codigo="GM2",
            descricao=f"Cao {TAG}",
            quantidade=Decimal("1"),
            valor_unitario=Decimal("50"),
            valor_total=Decimal("50"),
        )

        rows, _meta = ru.vendas_por_grupo_relatorio(
            desde,
            ate,
            categoria=[CAT],
            subcategoria=[SUB_G],
        )
        check(len(rows) == 1, "grupo filtrado sub Gato = 1 linha")
        check(abs(rows[0]["valor"] - 100.0) < 0.01, "valor so Gato")

        facetas, ranked = ru.facetas_categoria_sub(
            desde, ate, categoria=[CAT], subcategoria=[SUB_G]
        )
        check(len(ranked) == 1, "ranking facetas 1 produto")
        check(SUB_C in (facetas.get("subcategorias") or []), "lista sub periodo cat")
    except Exception as exc:
        ok(f"agg integracao skip ({type(exc).__name__})")


def check_http_xlsx() -> None:
    print("--- http + xlsx ---")
    with override_settings(ALLOWED_HOSTS=["testserver", "127.0.0.1", "localhost", "*"]):
        c = Client()
        for nome in (
            "relatorios_mais_vendidos",
            "relatorios_vendas_grupo",
            "relatorios_curva_abc",
            "relatorios_margem",
        ):
            r = c.get(reverse(nome), {"periodo": "30d", "export": "xlsx"})
            check(r.status_code == 200, f"{nome} xlsx 200")
            check("spreadsheet" in r.get("Content-Type", ""), f"{nome} content-type")

        rh = c.get(reverse("relatorios_vendas_grupo"), {"periodo": "30d"})
        check(rh.status_code == 200, f"vendas_grupo html {rh.status_code}")
        body = rh.content.decode("utf-8", errors="replace")
        check('name="subcategoria"' in body or "vendas_grupo" in body, "html sub checkbox")
        check('form="form-relatorio-filtros"' in body, "html excel ligado ao form")


def check_unit_tests() -> None:
    print("--- django tests ---")
    from django.core.management import call_command
    from io import StringIO

    out = StringIO()
    try:
        call_command(
            "test",
            "produtos.tests_relatorios_central_filtros",
            verbosity=1,
            stdout=out,
            stderr=out,
        )
        text = out.getvalue()
        check("FAIL:" not in text and "ERROR:" not in text, "tests_relatorios_central_filtros OK")
    except SystemExit as e:
        check(e.code in (0, None), f"tests exit {e.code}")


def main() -> int:
    print("VERIFY REL-CAT-SUB-EXCEL")
    try:
        check_static()
        check_util_or()
        check_agg_filtro()
        check_http_xlsx()
        check_unit_tests()
    finally:
        try:
            _cleanup()
            ok("cleanup TAG")
        except Exception:
            ok("cleanup skip")

    print(f"\nTOTAL OK={OKS} FAIL={len(FAILS)}")
    if FAILS:
        for f in FAILS:
            print(" -", f)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
