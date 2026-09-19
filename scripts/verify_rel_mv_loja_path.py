# -*- coding: utf-8 -*-
"""
REL-MV-LOJA — prova detalhada: filtro loja em Mais vendidos + Vendas por grupo.

Path:
  GET /relatorios/mais-vendidos/?deposito=ambos|centro|vila
  GET /relatorios/vendas-grupo/?deposito=ambos|centro|vila
    -> parse_deposito_relatorio
    -> ranking_produtos / vendas_por_grupo_relatorio
    -> _agg_itens_por_produto(..., deposito=)
    -> _qs_itens (centro inclui deposito vazio/null; vila = iexact vila)

  python scripts/verify_rel_mv_loja_path.py
"""
from __future__ import annotations

import os
import sys
import uuid
from datetime import timedelta
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
sys.path.insert(0, str(ROOT))

import django

django.setup()

from django.test import Client, override_settings
from django.urls import reverse
from django.utils import timezone

from produtos import relatorios_vendas_util as ru
from produtos.models import ItemVendaAgro, Produto, VendaAgro

FAILS: list[str] = []
OKS = 0
TAG = f"mvloja_{uuid.uuid4().hex[:10]}"
PID_C = f"{TAG}_c"  # só Centro
PID_V = f"{TAG}_v"  # só Vila
PID_A = f"{TAG}_a"  # ambas
CAT_C = f"CatC {TAG}"
CAT_V = f"CatV {TAG}"
CAT_A = f"CatA {TAG}"
PIN_TESTE = "9973"


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
    views = read("produtos/relatorios_central_views.py")
    tpl = read("produtos/templates/produtos/relatorios_generico.html")
    hub = read("produtos/templates/produtos/relatorios_hub.html")
    help_a = read("produtos/templates/produtos/includes/relatorios_help_agents.html")
    urls = read("produtos/urls.py")
    tests = read("produtos/tests_relatorios_central_filtros.py")

    check("def parse_deposito_relatorio" in util, "util parse_deposito_relatorio")
    check("def rotulo_deposito_relatorio" in util, "util rotulo_deposito_relatorio")
    check("def deposito_ui_value" in util, "util deposito_ui_value")
    check("deposito: str | None = None" in util, "util ranking/facetas aceitam deposito")
    check('deposito == "vila"' in util and 'deposito == "centro"' in util, "util filtro qs centro/vila")
    check("venda__deposito__isnull=True" in util, "util centro inclui deposito null")
    check('venda__deposito=""' in util, "util centro inclui deposito vazio")

    check("parse_deposito_relatorio(request)" in views, "views chamam parse")
    check("deposito=deposito" in views, "views passam deposito ao util")
    check("deposito=ru.deposito_ui_value" in views, "views extra_filtros deposito UI")
    check("rotulo_deposito_relatorio" in views, "views subtitulo com loja")

    check('filtro_parcial == \'mais_vendidos\' or filtro_parcial == \'vendas_grupo\'' in tpl
          or 'filtro_parcial == "mais_vendidos" or filtro_parcial == "vendas_grupo"' in tpl
          or ("mais_vendidos" in tpl and "vendas_grupo" in tpl and 'name="deposito"' in tpl),
          "tpl select loja em mais_vendidos/grupo")
    check('name="deposito"' in tpl, "tpl name=deposito")
    check("Centro + Vila" in tpl and "Só Centro" in tpl and "Só Vila" in tpl, "tpl opcoes loja")
    check("onchange=\"this.form.submit()\"" in tpl, "tpl submit ao trocar loja")

    check("Filtro Centro / Vila" in hub, "hub menciona filtro loja")
    check("relatorios_mais_vendidos" in hub and "relatorios_vendas_grupo" in hub, "hub cards")

    check(
        "LOJA" in help_a
        and "Centro" in help_a
        and "Vila" in help_a
        and "mais_vendidos" in help_a
        and "vendas_grupo" in help_a,
        "ajuda menciona loja",
    )
    check("mais-vendidos/" in urls and "vendas-grupo/" in urls, "urls rotas")
    check("test_parse_deposito_e_passa_no_ranking" in tests, "unit test deposito")
    check("test_mais_vendidos_e_grupo_filtro_loja_html" in tests, "unit test HTML loja")

    check(reverse("relatorios_mais_vendidos") == "/relatorios/mais-vendidos/", "reverse mais-vendidos")
    check(reverse("relatorios_vendas_grupo") == "/relatorios/vendas-grupo/", "reverse vendas-grupo")


def check_parse() -> None:
    print("--- parse ---")
    from django.test import RequestFactory

    rf = RequestFactory()
    check(ru.parse_deposito_relatorio(rf.get("/")) is None, "sem param -> ambas (None)")
    check(ru.parse_deposito_relatorio(rf.get("/", {"deposito": "ambos"})) is None, "ambos -> None")
    check(ru.parse_deposito_relatorio(rf.get("/", {"deposito": "AMBOS"})) is None, "AMBOS casefold")
    check(ru.parse_deposito_relatorio(rf.get("/", {"deposito": "centro"})) == "centro", "centro")
    check(ru.parse_deposito_relatorio(rf.get("/", {"deposito": "VILA"})) == "vila", "VILA")
    check(ru.parse_deposito_relatorio(rf.get("/", {"deposito": "xyz"})) is None, "invalido -> ambas")
    check(ru.deposito_ui_value(None) == "ambos", "ui None=ambos")
    check(ru.deposito_ui_value("centro") == "centro", "ui centro")
    check(ru.rotulo_deposito_relatorio(None) == "Centro + Vila", "rotulo ambas")
    check(ru.rotulo_deposito_relatorio("vila") == "Só Vila", "rotulo vila")


def _cleanup() -> None:
    ItemVendaAgro.objects.filter(produto_id_externo__startswith=TAG).delete()
    VendaAgro.objects.filter(cliente_nome__startswith=f"MVLoja {TAG}").delete()
    Produto.objects.filter(produto_externo_id__in=[PID_C, PID_V, PID_A]).delete()


def _mk_venda(*, deposito: str, total: str, nome_suf: str = "") -> VendaAgro:
    v = VendaAgro.objects.create(
        cliente_nome=f"MVLoja {TAG} {nome_suf or deposito}",
        cliente_documento="",
        total=Decimal(total),
        forma_pagamento="Dinheiro",
        deposito=deposito,
    )
    VendaAgro.objects.filter(pk=v.pk).update(criado_em=timezone.now() - timedelta(days=1))
    v.refresh_from_db()
    return v


def _mk_item(venda: VendaAgro, pid: str, qtd: str, valor: str, desc: str) -> ItemVendaAgro:
    return ItemVendaAgro.objects.create(
        venda=venda,
        produto_id_externo=pid,
        codigo="GM-MV",
        descricao=desc,
        quantidade=Decimal(qtd),
        valor_unitario=Decimal(valor) / Decimal(qtd),
        valor_total=Decimal(valor),
    )


def check_agg() -> None:
    print("--- agregacao (Centro x Vila) ---")
    _cleanup()
    agora = timezone.now()
    desde = agora - timedelta(days=7)
    ate = agora + timedelta(hours=1)

    Produto.objects.create(
        produto_externo_id=PID_C,
        nome=f"Prod Centro {TAG}",
        categoria=CAT_C,
        codigo_interno=f"GM-{TAG}-C",
    )
    Produto.objects.create(
        produto_externo_id=PID_V,
        nome=f"Prod Vila {TAG}",
        categoria=CAT_V,
        codigo_interno=f"GM-{TAG}-V",
    )
    Produto.objects.create(
        produto_externo_id=PID_A,
        nome=f"Prod Ambas {TAG}",
        categoria=CAT_A,
        codigo_interno=f"GM-{TAG}-A",
    )

    # Centro: C=100, A=40 · Vila: V=80, A=20 · Centro vazio (legado): C+=10 · Devolvida Vila: V+=999 (deve ignorar)
    vc = _mk_venda(deposito="centro", total="140.00", nome_suf="centro")
    _mk_item(vc, PID_C, "1", "100.00", f"Prod Centro {TAG}")
    _mk_item(vc, PID_A, "1", "40.00", f"Prod Ambas {TAG}")

    vv = _mk_venda(deposito="vila", total="100.00", nome_suf="vila")
    _mk_item(vv, PID_V, "1", "80.00", f"Prod Vila {TAG}")
    _mk_item(vv, PID_A, "1", "20.00", f"Prod Ambas {TAG}")

    v_empty = _mk_venda(deposito="", total="10.00", nome_suf="vazio")
    _mk_item(v_empty, PID_C, "1", "10.00", f"Prod Centro legado {TAG}")

    v_dev = _mk_venda(deposito="vila", total="999.00", nome_suf="devolvida")
    _mk_item(v_dev, PID_V, "1", "999.00", f"Prod Vila devolvida {TAG}")
    VendaAgro.objects.filter(pk=v_dev.pk).update(devolvida_em=timezone.now())

    def _map(rows: list[dict]) -> dict[str, float]:
        return {str(r.get("produto_id") or r.get("produto_id_externo")): float(r["valor"]) for r in rows}

    # --- ranking ---
    ambos = _map(ru.ranking_produtos(desde, ate, deposito=None, limite=0))
    check(abs(ambos.get(PID_C, 0) - 110.0) < 0.01, "ambas: Centro produto = 100+10 legado")
    check(abs(ambos.get(PID_V, 0) - 80.0) < 0.01, "ambas: Vila produto = 80 (sem devolvida)")
    check(abs(ambos.get(PID_A, 0) - 60.0) < 0.01, "ambas: Ambas produto = 40+20")

    so_c = _map(ru.ranking_produtos(desde, ate, deposito="centro", limite=0))
    check(abs(so_c.get(PID_C, 0) - 110.0) < 0.01, "so Centro: PID_C 110")
    check(PID_V not in so_c or so_c.get(PID_V, 0) == 0, "so Centro: sem PID_V")
    check(abs(so_c.get(PID_A, 0) - 40.0) < 0.01, "so Centro: PID_A so 40")

    so_v = _map(ru.ranking_produtos(desde, ate, deposito="vila", limite=0))
    check(abs(so_v.get(PID_V, 0) - 80.0) < 0.01, "so Vila: PID_V 80")
    check(PID_C not in so_v or so_v.get(PID_C, 0) == 0, "so Vila: sem PID_C")
    check(abs(so_v.get(PID_A, 0) - 20.0) < 0.01, "so Vila: PID_A so 20")
    check(999.0 not in so_v.values() and abs(so_v.get(PID_V, 0) - 80.0) < 0.01, "so Vila: ignora devolvida 999")

    # facetas com deposito
    fac_c, rows_c = ru.facetas_categoria_sub(desde, ate, deposito="centro")
    ids_c = {r["produto_id"] for r in rows_c}
    check(PID_C in ids_c and PID_V not in ids_c, "facetas centro sem Vila")
    check(CAT_C in (fac_c.get("categorias") or []), "facetas centro lista CatC")

    # --- vendas por grupo ---
    def _grupo_map(rows: list[dict]) -> dict[str, float]:
        return {str(r["grupo"]): float(r["valor"]) for r in rows}

    g_ambos, _ = ru.vendas_por_grupo_relatorio(desde, ate, agrupar="categoria", deposito=None)
    gm = _grupo_map(g_ambos)
    check(abs(gm.get(CAT_C, 0) - 110.0) < 0.01, "grupo ambas CatC 110")
    check(abs(gm.get(CAT_V, 0) - 80.0) < 0.01, "grupo ambas CatV 80")
    check(abs(gm.get(CAT_A, 0) - 60.0) < 0.01, "grupo ambas CatA 60")

    g_c, _ = ru.vendas_por_grupo_relatorio(desde, ate, agrupar="categoria", deposito="centro")
    gmc = _grupo_map(g_c)
    check(abs(gmc.get(CAT_C, 0) - 110.0) < 0.01, "grupo centro CatC")
    check(CAT_V not in gmc or gmc.get(CAT_V, 0) == 0, "grupo centro sem CatV")
    check(abs(gmc.get(CAT_A, 0) - 40.0) < 0.01, "grupo centro CatA 40")

    g_v, _ = ru.vendas_por_grupo_relatorio(desde, ate, agrupar="categoria", deposito="vila")
    gmv = _grupo_map(g_v)
    check(abs(gmv.get(CAT_V, 0) - 80.0) < 0.01, "grupo vila CatV")
    check(CAT_C not in gmv or gmv.get(CAT_C, 0) == 0, "grupo vila sem CatC")
    check(abs(gmv.get(CAT_A, 0) - 20.0) < 0.01, "grupo vila CatA 20")

    # soma centros = ambas (para nossos produtos TAG)
    soma_cv = float(so_c.get(PID_A, 0)) + float(so_v.get(PID_A, 0))
    check(abs(soma_cv - float(ambos.get(PID_A, 0))) < 0.01, "Centro+Vila = ambas no PID_A")


def check_pin() -> None:
    print("--- pin ---")
    try:
        from produtos.caixa_util import rotulo_operador_pin

        rot = (rotulo_operador_pin(PIN_TESTE) or "").strip()
        check(bool(rot), f"PIN {PIN_TESTE} mapeado ({rot!r})")
    except Exception as exc:
        fail(f"PIN check: {exc!r}")


def check_http() -> None:
    print("--- http ---")
    with override_settings(ALLOWED_HOSTS=["testserver", "127.0.0.1", "localhost", "*"]):
        c = Client()

        r_hub = c.get(reverse("relatorios_hub"))
        check(r_hub.status_code == 200, f"hub HTTP 200 ({r_hub.status_code})")
        bh = r_hub.content.decode("utf-8", errors="replace")
        check("mais-vendidos" in bh and "vendas-grupo" in bh, "hub links")

        for nome, dep, marca in (
            ("relatorios_mais_vendidos", "ambos", "Prod Ambas"),
            ("relatorios_mais_vendidos", "centro", "Prod Centro"),
            ("relatorios_mais_vendidos", "vila", "Prod Vila"),
            ("relatorios_vendas_grupo", "ambos", CAT_A),
            ("relatorios_vendas_grupo", "centro", CAT_C),
            ("relatorios_vendas_grupo", "vila", CAT_V),
        ):
            r = c.get(reverse(nome), {"deposito": dep, "periodo": "30d"})
            check(r.status_code == 200, f"{nome}?deposito={dep} HTTP 200 ({r.status_code})")
            body = r.content.decode("utf-8", errors="replace")
            check('name="deposito"' in body, f"{nome} {dep}: select loja")
            # option selected
            if dep == "ambos":
                check('value="ambos" selected' in body or "value=\"ambos\" selected" in body
                      or ("value=\"ambos\"" in body and "selected" in body),
                      f"{nome} {dep}: option ambos selected-ish")
            else:
                check(f'value="{dep}"' in body and "selected" in body, f"{nome} {dep}: option present")
            # dados do filtro (nossos nomes TAG)
            if dep == "centro":
                check(f"Prod Centro {TAG}" in body or CAT_C in body, f"{nome} centro mostra Centro")
                check(f"Prod Vila {TAG}" not in body and (nome != "relatorios_vendas_grupo" or CAT_V not in body),
                      f"{nome} centro nao mostra Vila")
            elif dep == "vila":
                check(f"Prod Vila {TAG}" in body or CAT_V in body, f"{nome} vila mostra Vila")
                check(f"Prod Centro {TAG}" not in body and (nome != "relatorios_vendas_grupo" or CAT_C not in body),
                      f"{nome} vila nao mostra Centro")
            else:
                check(marca in body or f"Prod Ambas {TAG}" in body or CAT_A in body,
                      f"{nome} ambas mostra dado compartilhado")

        # Excel
        for nome in ("relatorios_mais_vendidos", "relatorios_vendas_grupo"):
            for dep in ("ambos", "centro", "vila"):
                rx = c.get(reverse(nome), {"deposito": dep, "periodo": "30d", "export": "xlsx"})
                check(rx.status_code == 200, f"xlsx {nome} {dep} HTTP 200")
                ctype = rx.get("Content-Type", "")
                check("spreadsheet" in ctype or "xlsx" in ctype, f"xlsx {nome} {dep} content-type")
                check(len(rx.content) > 80, f"xlsx {nome} {dep} bytes")


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
        check(
            "FAIL:" not in text and "ERROR:" not in text and "Traceback" not in text,
            "django tests sem FAIL/ERROR",
        )
        check(True, "django tests suite executado")
    except SystemExit as e:
        text = out.getvalue()
        check(e.code in (0, None), f"django tests exit={e.code} {text[-400:]}")
    except Exception as exc:
        fail(f"django tests: {exc}")


def main() -> int:
    print("VERIFY REL-MV-LOJA")
    try:
        check_static()
        check_parse()
        check_pin()
        check_agg()
        check_http()
        check_unit_tests()
    finally:
        try:
            _cleanup()
            ok("cleanup TAG")
        except Exception as exc:
            fail(f"cleanup: {exc}")

    print("---")
    print(f"VERIFY_OK {OKS}/{OKS + len(FAILS)}" if not FAILS else f"VERIFY_FAIL {OKS} ok / {len(FAILS)} fail")
    for f in FAILS:
        print(" -", f.encode("ascii", "replace").decode("ascii"))
    return 1 if FAILS else 0


if __name__ == "__main__":
    raise SystemExit(main())
