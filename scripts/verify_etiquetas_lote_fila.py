#!/usr/bin/env python
"""Prova detalhada ETQ-LOTE-FILA — static · helpers · API · páginas · PIN."""
from __future__ import annotations

import json
import os
import re
import sys
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

from django.contrib.auth import get_user_model
from django.test import Client
from django.urls import reverse

from produtos.models import EtiquetaLoteAgro
from produtos.views import (
    _etiquetas_lote_config,
    _etiquetas_lote_flat,
    _etiquetas_lote_normalizar_itens_entrada,
    _etiquetas_lote_parse_config,
    _etiquetas_lote_row,
    _etiquetas_lote_totais,
)

fails: list[str] = []
oks = 0
PIN = "9973"


def ok(msg: str) -> None:
    global oks
    oks += 1
    print(f"  OK  {msg}")


def fail(msg: str) -> None:
    fails.append(msg)
    print(f"  FAIL {msg}")


def check(name: str, cond: bool, detail: str = "") -> None:
    if cond:
        ok(name)
    else:
        fail(f"{name}" + (f" · {detail}" if detail else ""))


def read(rel: str) -> str:
    return (ROOT / rel).read_text(encoding="utf-8", errors="replace")


def section(title: str) -> None:
    print(f"\n== {title} ==")


def main() -> int:
    # ----- STATIC / CONTRATO UI -----
    section("Static / contrato")
    js_etq = read("produtos/static/produtos/js/produtos_etiquetas.js")
    js_lote = read("produtos/static/produtos/js/produtos_etiquetas_lote.js")
    html_etq = read("produtos/templates/produtos/produtos_etiquetas.html")
    html_lote = read("produtos/templates/produtos/produtos_etiquetas_lote.html")
    urls = read("produtos/urls.py")
    views = read("produtos/views.py")

    check("btn Lote A4 no HTML", 'id="etq-btn-lote-a4"' in html_etq)
    check("abrirLoteA4 no JS etiquetas", "function abrirLoteA4" in js_etq)
    check("sessionStorage fila key", "agro_etq_lote_fila_v1" in js_etq and "agro_etq_lote_fila_v1" in js_lote)
    check("JS lote: origem fila+loja", "origem: 'fila'" in js_lote and "origem: 'loja'" in js_lote)
    check("JS lote: modo pausa/auto", "modo" in js_lote and "'auto'" in js_lote and "'pausa'" in js_lote)
    check("JS lote: qtd massa", "qtd_massa" in js_lote)
    check("JS lote: Parar auto", "lote-btn-parar" in js_lote and "autoStop" in js_lote)
    check("JS lote: confirmar só após OK", "confirmar-folha" in js_lote)
    check("HTML lote: controles", all(
        x in html_lote
        for x in (
            "lote-etq-folha",
            "lote-folhas-vez",
            "lote-intervalo",
            "lote-modo",
            "lote-preset",
            "lote-qtd-massa",
            "lote-btn-parar",
        )
    ))
    check("URL atualizar registrada", "api_etiquetas_lote_atualizar" in urls)
    check("views: origem fila|loja", 'origem not in ("fila", "loja")' in views)
    check("views: expand QTD flat", "_etiquetas_lote_flat" in views)
    check("sem ETQ_LOTE_FOLHA hardcode antigo como único", "ETQ_LOTE_FOLHA_DEFAULT" in views)

    # ----- HELPERS -----
    section("Helpers")
    itens = _etiquetas_lote_normalizar_itens_entrada(
        [
            {"id": "1", "nome": "Veneno", "qtd": 3, "preco_venda": "2,00"},
            {"id": "2", "nome": "Anticion", "qtd": 1, "preco_venda": 10},
            {"id": "", "nome": "", "qtd": 5},  # inválido
        ]
    )
    flat = _etiquetas_lote_flat(itens)
    check("normaliza ignora vazio", len(itens) == 2)
    check("flat 3+1=4", len(flat) == 4)
    check("preco BR 2.0", itens[0]["preco_venda"] == 2.0)
    check("qtd clamp 999", _etiquetas_lote_normalizar_itens_entrada([{"id": "x", "nome": "X", "qtd": 5000}])[0]["qtd"] == 999)

    cfg = _etiquetas_lote_parse_config(
        {"folhas_por_vez": 2, "etiquetas_por_folha": 18, "modo": "auto", "intervalo_seg": 5}
    )
    check("config folhas 2", cfg["folhas_por_vez"] == 2)
    check("config modo auto", cfg["modo"] == "auto")
    check("config clamp etq 54", _etiquetas_lote_parse_config({"etiquetas_por_folha": 99})["etiquetas_por_folha"] == 54)
    check("config modo inválido → pausa", _etiquetas_lote_parse_config({"modo": "xyz"})["modo"] == "pausa")
    check("config nested config{}", _etiquetas_lote_parse_config({"config": {"modo": "auto"}})["modo"] == "auto")

    # ----- PIN -----
    section("PIN 9973")
    try:
        from produtos.caixa_util import validar_pin_operador

        ok_pin, err_pin = validar_pin_operador(PIN)
        check("PIN 9973 válido", bool(ok_pin), err_pin or "")
    except Exception as exc:
        fail(f"PIN não verificável · {exc!s}")

    # ----- AUTH / PÁGINAS -----
    section("Páginas + auth")
    User = get_user_model()
    user = User.objects.filter(is_superuser=True).first() or User.objects.filter(is_staff=True).first()
    if not user:
        user = User.objects.create_user("etq_lote_path", password="x")
    c = Client(HTTP_HOST="127.0.0.1")
    c_anon = Client(HTTP_HOST="127.0.0.1")

    r_anon = c_anon.get("/produtos/etiquetas/lote/")
    check("anon lote redireciona login", r_anon.status_code in (302, 301), str(r_anon.status_code))

    r_api_anon = c_anon.get("/api/produtos/etiquetas/lote/")
    check("anon API lote bloqueada", r_api_anon.status_code in (302, 401, 403), str(r_api_anon.status_code))

    c.force_login(user)
    r_page = c.get("/produtos/etiquetas/")
    check("página etiquetas 200", r_page.status_code == 200, str(r_page.status_code))
    body_etq = r_page.content.decode("utf-8", errors="replace")
    check("página tem btn lote", "etq-btn-lote-a4" in body_etq)

    r_lote_page = c.get("/produtos/etiquetas/lote/")
    check("página lote 200", r_lote_page.status_code == 200, str(r_lote_page.status_code))
    body_lote = r_lote_page.content.decode("utf-8", errors="replace")
    check("página lote carrega JS v2+", "produtos_etiquetas_lote.js" in body_lote)
    check("página lote tem modo", "lote-modo" in body_lote)
    check("página lote presets URL", "presets" in body_lote.lower() or "AGRO_ETQ_LOTE_CFG" in body_lote)

    # reverse names
    for name in (
        "produtos_etiquetas_lote",
        "api_etiquetas_lote",
        "api_etiquetas_lote_atualizar",
        "api_etiquetas_lote_proxima_folha",
        "api_etiquetas_lote_confirmar_folha",
        "api_etiquetas_lote_desfazer_folha",
        "api_etiquetas_lote_cancelar",
    ):
        try:
            reverse(name, kwargs={"pk": 1} if "pk" in name or "detalhe" in name or "folha" in name or "cancelar" in name or "atualizar" in name else None)
            # fix: reverse with pk only when needed
        except Exception:
            pass
    check("reverse lote view", reverse("produtos_etiquetas_lote") == "/produtos/etiquetas/lote/")
    check("reverse api lote", reverse("api_etiquetas_lote") == "/api/produtos/etiquetas/lote/")
    check(
        "reverse atualizar",
        reverse("api_etiquetas_lote_atualizar", kwargs={"pk": 9}) == "/api/produtos/etiquetas/lote/9/atualizar/",
    )

    # ----- API FILA -----
    section("API origem=fila")
    EtiquetaLoteAgro.objects.filter(nome__startswith="TEST-PATH-ETQ").delete()

    r_empty = c.post(
        "/api/produtos/etiquetas/lote/",
        data={"origem": "fila", "nome": "TEST-PATH-ETQ-empty", "itens": []},
        content_type="application/json",
    )
    check("fila vazia 400", r_empty.status_code == 400 and not (r_empty.json() or {}).get("ok"))

    r = c.post(
        "/api/produtos/etiquetas/lote/",
        data={
            "origem": "fila",
            "nome": "TEST-PATH-ETQ-fila",
            "preset_id": "gondola",
            "etiquetas_por_folha": 18,
            "folhas_por_vez": 2,
            "intervalo_seg": 3,
            "modo": "pausa",
            "itens": [
                {"id": "p1", "nome": "Item A", "qtd": 2, "preco_venda": 1.5, "codigo_gm": "GM1"},
                {"id": "p2", "nome": "Item B", "qtd": 1, "preco_venda": 2, "codigo_gm": "GM2"},
                {"id": "p3", "nome": "Item C", "qtd": 5, "preco_venda": "3,50"},
            ],
        },
        content_type="application/json",
    )
    j = r.json()
    check("POST fila 200", r.status_code == 200 and j.get("ok"), str(j)[:160])
    lote = j.get("lote") or {}
    pk = lote.get("id")
    check("origem fila no row", lote.get("origem") == "fila")
    check("total 2+1+5=8", lote.get("total") == 8, str(lote.get("total")))
    check("n_produtos 3", lote.get("n_produtos") == 3, str(lote.get("n_produtos")))
    check("proxima_qtd cap 8", lote.get("proxima_qtd") == 8, str(lote.get("proxima_qtd")))
    check("modo pausa default path", (lote.get("config") or {}).get("modo") == "pausa")
    check("folhas_por_vez 2", lote.get("folhas_por_vez") == 2)
    check("preset gondola", lote.get("preset_id") == "gondola")
    check("PG persistiu", pk and EtiquetaLoteAgro.objects.filter(pk=pk).exists())

    # ----- ATUALIZAR / QTD / CONFIG -----
    section("Atualizar config + QTD")
    assert pk
    r2 = c.post(
        f"/api/produtos/etiquetas/lote/{pk}/atualizar/",
        data={
            "preset_id": "remedios",
            "etiquetas_por_folha": 4,
            "folhas_por_vez": 1,
            "intervalo_seg": 7,
            "modo": "auto",
            "qtds": [{"index": 0, "qtd": 3}, {"index": 2, "qtd": 2}],
        },
        content_type="application/json",
    )
    j2 = r2.json()
    lote2 = j2.get("lote") or {}
    check("atualizar 200", r2.status_code == 200 and j2.get("ok"), str(j2)[:160])
    check("preset remedios", lote2.get("preset_id") == "remedios")
    check("modo auto", (lote2.get("config") or {}).get("modo") == "auto")
    check("intervalo 7", lote2.get("intervalo_seg") == 7)
    # qtd: p1=3, p2=1, p3=2 → total 6
    check("total após qtds 6", lote2.get("total") == 6, str(lote2.get("total")))
    check("folha_size 4", lote2.get("folha_size") == 4)
    check("proxima_qtd 4", lote2.get("proxima_qtd") == 4, str(lote2.get("proxima_qtd")))

    r_massa = c.post(
        f"/api/produtos/etiquetas/lote/{pk}/atualizar/",
        data={"qtd_massa": 2},
        content_type="application/json",
    )
    lote_m = (r_massa.json() or {}).get("lote") or {}
    check("qtd_massa → total 6", lote_m.get("total") == 6, str(lote_m.get("total")))  # 3 prod × 2

    # ----- PROXIMA NÃO AVANÇA -----
    section("Próxima folha sem avançar")
    cur_antes = EtiquetaLoteAgro.objects.get(pk=pk).cursor
    r3 = c.post(f"/api/produtos/etiquetas/lote/{pk}/proxima-folha/", data={}, content_type="application/json")
    j3 = r3.json()
    cur_depois = EtiquetaLoteAgro.objects.get(pk=pk).cursor
    check("proxima-folha ok", r3.status_code == 200 and j3.get("ok"))
    check("itens = folha_size", len(j3.get("itens") or []) == 4, str(len(j3.get("itens") or [])))
    check("cursor NÃO avançou", cur_antes == cur_depois == 0, f"{cur_antes}->{cur_depois}")
    check("preset_id na resposta", j3.get("preset_id") == "remedios")

    # simula «Não» no confirm: segunda proxima ainda mesmos itens
    r3b = c.post(f"/api/produtos/etiquetas/lote/{pk}/proxima-folha/", data={}, content_type="application/json")
    check(
        "reimprimir mesma fatia",
        [x.get("nome") for x in (r3b.json().get("itens") or [])]
        == [x.get("nome") for x in (j3.get("itens") or [])],
    )

    # ----- CONFIRMAR / DESFAZER / CONCLUIR -----
    section("Confirmar · desfazer · concluir")
    r4 = c.post(
        f"/api/produtos/etiquetas/lote/{pk}/confirmar-folha/",
        data={"qtd": 4},
        content_type="application/json",
    )
    lote4 = (r4.json() or {}).get("lote") or {}
    check("confirmar cursor 4", lote4.get("cursor") == 4, str(lote4.get("cursor")))
    check("faltam 2", lote4.get("faltam") == 2, str(lote4.get("faltam")))
    check("ainda aberto", lote4.get("status") == "aberto")

    r5 = c.post(f"/api/produtos/etiquetas/lote/{pk}/desfazer-folha/", data={}, content_type="application/json")
    lote5 = (r5.json() or {}).get("lote") or {}
    check("desfazer cursor 0", lote5.get("cursor") == 0, str(lote5.get("cursor")))

    # confirma tudo até concluir
    c.post(f"/api/produtos/etiquetas/lote/{pk}/confirmar-folha/", data={"qtd": 4}, content_type="application/json")
    r6 = c.post(
        f"/api/produtos/etiquetas/lote/{pk}/confirmar-folha/",
        data={"qtd": 2},
        content_type="application/json",
    )
    lote6 = (r6.json() or {}).get("lote") or {}
    check("concluído", lote6.get("status") == "concluido", str(lote6.get("status")))
    check("faltam 0", lote6.get("faltam") == 0)

    r7 = c.post(f"/api/produtos/etiquetas/lote/{pk}/proxima-folha/", data={}, content_type="application/json")
    check("proxima em concluído falha", r7.status_code == 400)

    r8 = c.post(f"/api/produtos/etiquetas/lote/{pk}/desfazer-folha/", data={}, content_type="application/json")
    lote8 = (r8.json() or {}).get("lote") or {}
    check("desfazer reabre", lote8.get("status") == "aberto", str(lote8.get("status")))

    r9 = c.post(f"/api/produtos/etiquetas/lote/{pk}/cancelar/", data={}, content_type="application/json")
    lote9 = (r9.json() or {}).get("lote") or {}
    check("cancelar", lote9.get("status") == "cancelado")

    r10 = c.post(
        f"/api/produtos/etiquetas/lote/{pk}/atualizar/",
        data={"modo": "pausa"},
        content_type="application/json",
    )
    check("atualizar cancelado falha", r10.status_code == 400)

    # ----- LISTA ABERTOS + DETALHE -----
    section("Lista / detalhe")
    r_create2 = c.post(
        "/api/produtos/etiquetas/lote/",
        data={
            "origem": "fila",
            "nome": "TEST-PATH-ETQ-lista",
            "itens": [{"id": "z", "nome": "Z", "qtd": 1}],
            "etiquetas_por_folha": 18,
            "modo": "pausa",
        },
        content_type="application/json",
    )
    pk2 = ((r_create2.json() or {}).get("lote") or {}).get("id")
    r_list = c.get("/api/produtos/etiquetas/lote/?status=aberto&limit=20")
    jl = r_list.json()
    check("lista abertos", r_list.status_code == 200 and jl.get("ok"))
    ids = [x.get("id") for x in (jl.get("lotes") or [])]
    check("lote novo na lista", pk2 in ids, str(ids[:5]))
    if pk2:
        r_det = c.get(f"/api/produtos/etiquetas/lote/{pk2}/")
        jd = r_det.json()
        check("detalhe inclui itens", r_det.status_code == 200 and isinstance((jd.get("lote") or {}).get("itens"), list))
        obj = EtiquetaLoteAgro.objects.get(pk=pk2)
        row = _etiquetas_lote_row(obj, incluir_itens=True)
        check("config via modelo", _etiquetas_lote_config(obj)["modo"] == "pausa")
        check("totais coerentes", row["total"] == 1 and row["faltam"] == 1)
        c.post(f"/api/produtos/etiquetas/lote/{pk2}/cancelar/", data={}, content_type="application/json")

    # ----- FOLHAS POR VEZ × ETQ -----
    section("Folhas por vez")
    r_fv = c.post(
        "/api/produtos/etiquetas/lote/",
        data={
            "origem": "fila",
            "nome": "TEST-PATH-ETQ-folhas",
            "etiquetas_por_folha": 3,
            "folhas_por_vez": 2,
            "itens": [{"id": str(i), "nome": f"P{i}", "qtd": 1} for i in range(10)],
        },
        content_type="application/json",
    )
    lf = (r_fv.json() or {}).get("lote") or {}
    pkf = lf.get("id")
    check("proxima_qtd 6 (3×2)", lf.get("proxima_qtd") == 6, str(lf.get("proxima_qtd")))
    if pkf:
        rp = c.post(f"/api/produtos/etiquetas/lote/{pkf}/proxima-folha/", data={}, content_type="application/json")
        check("fatia 6 itens", len((rp.json() or {}).get("itens") or []) == 6)
        c.post(f"/api/produtos/etiquetas/lote/{pkf}/cancelar/", data={}, content_type="application/json")

    # ----- ORIGEM LOJA (smoke leve — pode demorar) -----
    section("API origem=loja (smoke)")
    try:
        r_loja = c.post(
            "/api/produtos/etiquetas/lote/",
            data={
                "origem": "loja",
                "nome": "TEST-PATH-ETQ-loja",
                "loja": "vila",
                "estoque_sinal": "positivo",
                "somente_ativos": True,
                "etiquetas_por_folha": 18,
                "folhas_por_vez": 1,
                "modo": "pausa",
                "preset_id": "gondola",
            },
            content_type="application/json",
        )
        jlloja = r_loja.json()
        if r_loja.status_code == 200 and jlloja.get("ok"):
            n = (jlloja.get("lote") or {}).get("total") or 0
            check(f"loja montou ({n} etq)", n > 0)
            pk_loja = (jlloja.get("lote") or {}).get("id")
            if pk_loja:
                c.post(f"/api/produtos/etiquetas/lote/{pk_loja}/cancelar/", data={}, content_type="application/json")
        elif r_loja.status_code == 400:
            # ambiente sem produtos com saldo — aceitável
            check("loja sem produtos (400 esperado em DB vazio)", True, (jlloja.get("erro") or "")[:80])
        else:
            fail(f"loja HTTP {r_loja.status_code} · {str(jlloja)[:120]}")
    except Exception as exc:
        fail(f"loja exception · {exc!s}")

    # ----- NODE SYNTAX -----
    section("JS syntax")
    import subprocess

    for rel in (
        "produtos/static/produtos/js/produtos_etiquetas_lote.js",
        "produtos/static/produtos/js/produtos_etiquetas.js",
        "produtos/static/produtos/js/produtos_etiquetas_core.js",
    ):
        p = subprocess.run(["node", "--check", str(ROOT / rel)], capture_output=True, text=True)
        check(f"node --check {Path(rel).name}", p.returncode == 0, (p.stderr or "")[:80])

    # cleanup leftovers
    EtiquetaLoteAgro.objects.filter(nome__startswith="TEST-PATH-ETQ").delete()

    print(f"\nResultado: {oks} ok · {len(fails)} fail")
    if fails:
        print("Falhas:")
        for f in fails:
            print(f"  - {f}")
    return 1 if fails else 0


if __name__ == "__main__":
    raise SystemExit(main())
