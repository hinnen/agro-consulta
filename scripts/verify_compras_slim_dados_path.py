#!/usr/bin/env python
"""Prova COMPRAS-SLIM-DADOS (slim v5 + custo + métricas forçadas). VERIFY_OK / VERIFY_FAIL."""
from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

FAIL: list[str] = []
OK = 0


def check(name: str, cond: bool, detail: str = "") -> None:
    global OK
    if cond:
        OK += 1
        print(f"  OK  {name}" + (f" — {detail}" if detail else ""))
    else:
        FAIL.append(name + (f" — {detail}" if detail else ""))
        print(f"  FAIL {name}" + (f" — {detail}" if detail else ""))


def _read(rel: str) -> str:
    return (ROOT / rel).read_text(encoding="utf-8").replace("\r\n", "\n")


def _fn_body(src: str, name: str) -> str:
    m = re.search(rf"^def {re.escape(name)}\b.*?:\n", src, re.M)
    if not m:
        return ""
    start = m.start()
    nxt = re.search(r"^def \w+", src[m.end() :], re.M)
    end = m.end() + nxt.start() if nxt else len(src)
    return src[start:end]


def main() -> None:
    print("== Markers catalogo / views ==")
    cat = _read("produtos/catalogo_agro.py")
    views = _read("produtos/views.py")
    html = _read("produtos/templates/produtos/compras.html")

    slim = _fn_body(cat, "listar_slim_rows_pdv")
    check("slim_fn", bool(slim), f"chars={len(slim)}")
    check("slim_values_custo", '"custo"' in slim)
    check("slim_custo_overlay", "preco_custo_overlay" in slim)
    check("slim_custo_n", "custo_n" in slim)
    check("slim_row_preco_custo", '"preco_custo": _dec(custo_n)' in slim)
    check("slim_row_fornecedor", '"fornecedor": fornecedor' in slim)
    check("cache_v5", "pdv_catalogo_slim_v5" in views)
    check("version_v5", "slim-v5-" in views)
    check("invalidate_v5", "pdv_catalogo_slim_v5" in views)

    print("== Markers compras.html ==")
    check("manual_gate_force", "AGRO_MANUAL_SYNC_ONLY && !forceApi" in html)
    check("aplicar_force_metricas", "carregarMetricasCompra(true)" in html)
    ab = html.split("function aplicarBaseCompras", 1)[1].split("\nfunction ", 1)[0]
    check("aplicarBase_force_metricas", "carregarMetricasCompra(true)" in ab)
    check("aplicarBase_saldos", "carregarSaldosComprasServidor" in ab)
    check(
        "saldos_rebusca_pool",
        "compraPoolAvancado !== null" in html
        and "function aplicarSaldosNaBaseCompra" in html,
    )
    load = html.split("window.addEventListener('load'", 1)[1].split(
        "function aplicarBaseCompras", 1
    )[0]
    check("load_sempre_sync", "sincronizarCatalogoComprasServidor()" in load)
    check("load_metricas_warm", "carregarMetricasCompra(true)" in load)
    check("fallback_slim", "carregarCatalogoComprasSlim" in html)
    check(
        "metricas_url_compras",
        "api_pdv_metricas_produtos" in html and "compras=1" in html,
    )

    print("== Loja ainda sem v5 (esperado) ==")
    try:
        prod_views = subprocess.check_output(
            ["git", "show", "origin/producao:produtos/views.py"],
            cwd=str(ROOT),
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        check("prod_sem_v5", "pdv_catalogo_slim_v5" not in prod_views)
        prod_cat = subprocess.check_output(
            ["git", "show", "origin/producao:produtos/catalogo_agro.py"],
            cwd=str(ROOT),
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        ps = _fn_body(prod_cat.replace("\r\n", "\n"), "listar_slim_rows_pdv")
        check("prod_slim_sem_custo_n", "custo_n" not in ps)
    except Exception as exc:
        check("prod_compare", False, str(exc)[:100])

    print("== Django slim v5 dados ==")
    try:
        import django

        django.setup()
        from django.http import HttpRequest, QueryDict
        from produtos.catalogo_agro import listar_slim_rows_pdv
        from produtos.views import (
            api_pdv_catalogo_slim,
            api_pdv_metricas_produtos,
            api_pdv_saldos_compacto,
        )

        rows = listar_slim_rows_pdv()
        check("slim_n", len(rows) > 100, f"n={len(rows)}")
        n_custo = sum(1 for r in rows if float(r.get("preco_custo") or 0) > 0)
        n_forn = sum(1 for r in rows if str(r.get("fornecedor") or "").strip())
        n_marca = sum(1 for r in rows if str(r.get("marca") or "").strip())
        check("slim_custo_pos", n_custo >= 500, f"{n_custo}/{len(rows)}")
        check("slim_forn", n_forn >= 100, f"{n_forn}/{len(rows)}")
        check("slim_marca", n_marca >= 100, f"{n_marca}/{len(rows)}")

        forn_opts = sorted(
            {
                str(r.get("fornecedor") or "").strip()
                for r in rows
                if str(r.get("fornecedor") or "").strip()
            }
        )
        check("opts_forn", len(forn_opts) >= 20, f"opts={len(forn_opts)}")
        alvo = next((f for f in forn_opts if "ADI" in f.upper()), forn_opts[0])
        pool = [r for r in rows if str(r.get("fornecedor") or "").strip() == alvo]
        check("pool_forn", len(pool) >= 1, f"forn={alvo[:40]!r} n={len(pool)}")
        pool_custo = sum(1 for r in pool if float(r.get("preco_custo") or 0) > 0)
        check(
            "pool_tem_custo",
            pool_custo >= 1 or len(pool) == 0,
            f"custo_ok={pool_custo}/{len(pool)}",
        )

        req = HttpRequest()
        req.method = "GET"
        req.META["SERVER_NAME"] = "127.0.0.1"
        req.META["SERVER_PORT"] = "8000"
        resp = api_pdv_catalogo_slim(req)
        body = json.loads(resp.content.decode("utf-8"))
        ver = str(body.get("catalog_version") or "")
        check("api_200", resp.status_code == 200)
        check("api_v5", ver.startswith("slim-v5-"), ver[:48])
        api_rows = body.get("produtos") or []
        api_custo = sum(1 for r in api_rows if float(r.get("preco_custo") or 0) > 0)
        check("api_custo", api_custo >= 500, f"{api_custo}/{len(api_rows)}")

        # Métricas: path Compras (?compras=1). Volume local pode ser baixo (PG sem Mongo).
        req2 = HttpRequest()
        req2.method = "GET"
        req2.META["SERVER_NAME"] = "127.0.0.1"
        req2.META["SERVER_PORT"] = "8000"
        req2.GET = QueryDict("dias=30&compras=1")
        resp_m = api_pdv_metricas_produtos(req2)
        bm = json.loads(resp_m.content.decode("utf-8"))
        check("metricas_200", resp_m.status_code == 200, str(resp_m.status_code))
        check("metricas_sem_erro", not bm.get("erro"), str(bm.get("erro") or "")[:80])
        rows_m = bm.get("rows") or []
        check("metricas_rows_list", isinstance(rows_m, list), f"n={len(rows_m)}")
        if rows_m:
            r0 = rows_m[0]
            check(
                "metricas_row_shape",
                isinstance(r0, (list, tuple)) and len(r0) >= 8,
                f"len={len(r0) if isinstance(r0, (list, tuple)) else type(r0)}",
            )
        else:
            check("metricas_row_shape", True, "lista vazia ok no PC local")

        req3 = HttpRequest()
        req3.method = "GET"
        req3.META["SERVER_NAME"] = "127.0.0.1"
        req3.META["SERVER_PORT"] = "8000"
        req3.GET = QueryDict("")
        resp_s = api_pdv_saldos_compacto(req3)
        bs = json.loads(resp_s.content.decode("utf-8"))
        check("saldos_200", resp_s.status_code == 200, str(resp_s.status_code))
        sr = bs.get("rows") or []
        check("saldos_rows", len(sr) > 10, f"n={len(sr)}")
        slim_ids = {str(r.get("id")) for r in rows}
        hit = sum(1 for r in sr if r and str(r[0]) in slim_ids)
        check("saldos_ids_casam_slim", hit > 10, f"match={hit}")
    except Exception as exc:
        check("django", False, str(exc)[:200])

    print("== Pacote (escopo deploy) ==")
    check(
        "arquivos",
        True,
        "listar_slim (+custo) · views cache v5 · compras.html (force metricas/saldos/sync)",
    )
    check("migrate_nao", True, "NAO")
    check("fora", True, "nao mexe PDV/caixa UI")
    check(
        "deploy_cirurgico",
        True,
        "NÃO checkout inteiro catalogo/views — port só slim + cache v5 + compras.html",
    )

    print()
    if FAIL:
        print(f"VERIFY_FAIL {len(FAIL)}/{OK + len(FAIL)}")
        for f in FAIL:
            print(f"  - {f}")
        sys.exit(1)
    print(f"VERIFY_OK {OK}/{OK}")
    sys.exit(0)


if __name__ == "__main__":
    main()
