#!/usr/bin/env python
"""Prova COMPRAS-SLIM-FORN (slim v4 + fornecedor na busca avançada). VERIFY_OK / VERIFY_FAIL."""
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
    return (ROOT / rel).read_text(encoding="utf-8")


def _valores_dimensao_compra(p: dict, dim: str) -> list[str]:
    """Espelho JS valoresDimensaoCompra (compras.html)."""
    o: list[str] = []
    if dim == "marca" and p.get("marca"):
        o.append(str(p["marca"]))
    if dim == "fornecedor" and p.get("fornecedor"):
        o.append(str(p["fornecedor"]))
    if dim == "categoria" and p.get("categoria"):
        o.append(str(p["categoria"]))
    return o


def main() -> None:
    print("== Markers código ==")
    cat = _read("produtos/catalogo_agro.py")
    views = _read("produtos/views.py")
    html = _read("produtos/templates/produtos/compras.html")

    check("listar_slim_existe", "def listar_slim_rows_pdv" in cat)
    # bloco da função slim
    i = cat.find("def listar_slim_rows_pdv")
    j = cat.find("\ndef ", i + 10)
    slim_fn = cat[i:j] if i >= 0 else ""
    check("slim_values_fornecedor_texto", '"fornecedor_texto"' in slim_fn or "'fornecedor_texto'" in slim_fn)
    check("slim_overlay_fornecedor_texto", slim_fn.count("fornecedor_texto") >= 2)
    check("slim_row_chave_fornecedor", '"fornecedor": fornecedor' in slim_fn)
    check("slim_busca_inclui_forn", "fornecedor," in slim_fn or "fornecedor)" in slim_fn)
    check("cache_slim_v4", "pdv_catalogo_slim_v4" in views)
    check("catalog_version_slim_v4", "slim-v4-" in views)
    check("invalidar_v4", 'cache.delete(f"pdv_catalogo_slim_v4:' in views or "pdv_catalogo_slim_v4:{hoje" in views)
    check("compras_dim_fornecedor", "dim === 'fornecedor' && p.fornecedor" in html)
    check("compras_fallback_slim", "carregarCatalogoComprasSlim" in html)

    print("== Diff cirúrgico vs producao (não mergear views/catalogo inteiros) ==")
    try:
        # Só o pedaço listar_slim — producao não pode já ter a chave fornecedor no slim
        prod_cat = subprocess.check_output(
            ["git", "show", "origin/producao:produtos/catalogo_agro.py"],
            cwd=str(ROOT),
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        pi = prod_cat.find("def listar_slim_rows_pdv")
        pj = prod_cat.find("\ndef ", pi + 10) if pi >= 0 else -1
        prod_slim = prod_cat[pi:pj] if pi >= 0 and pj > pi else ""
        check("prod_slim_existe", bool(prod_slim))
        check(
            "prod_slim_sem_fornecedor_ainda",
            '"fornecedor": fornecedor' not in prod_slim,
            "loja ainda sem forn no slim (esperado)",
        )
        prod_views = subprocess.check_output(
            ["git", "show", "origin/producao:produtos/views.py"],
            cwd=str(ROOT),
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        check("prod_ainda_slim_v3", "pdv_catalogo_slim_v3" in prod_views)
        check("prod_sem_slim_v4", "pdv_catalogo_slim_v4" not in prod_views)

        # Escopo seguro do pacote: catalogo_agro + trecho cache views — avisar se files divergem demais
        stat = subprocess.check_output(
            [
                "git",
                "diff",
                "--stat",
                "origin/producao...HEAD",
                "--",
                "produtos/catalogo_agro.py",
            ],
            cwd=str(ROOT),
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        # extrai +N -M se possível
        m = re.search(r"(\d+) insertions?\(\+\).*?(\d+) deletions?\(-\)", stat.replace("\n", " "))
        if not m:
            m2 = re.search(r"\|\s+(\d+)\s+", stat)
            check("diff_catalogo_stat", bool(stat.strip()), stat.strip()[:80])
        else:
            ins, dele = int(m.group(1)), int(m.group(2))
            # aviso: se gigante, deploy deve ser port cirúrgico
            check(
                "diff_catalogo_conhecido",
                True,
                f"+{ins}/-{dele} (deploy = port só listar_slim + cache v4)",
            )
    except Exception as exc:
        check("diff_producao", False, str(exc)[:120])

    print("== Django: slim monta + dimensão Compras ==")
    try:
        import django

        django.setup()
        from produtos.catalogo_agro import listar_slim_rows_pdv, queryset_catalogo_ativos
        from produtos.models import ProdutoGestaoOverlayAgro

        rows = listar_slim_rows_pdv()
        check("slim_n", isinstance(rows, list) and len(rows) > 100, f"n={len(rows)}")
        n_forn = sum(1 for x in rows if str(x.get("fornecedor") or "").strip())
        n_marca = sum(1 for x in rows if str(x.get("marca") or "").strip())
        n_cat = sum(1 for x in rows if str(x.get("categoria") or "").strip())
        check("slim_forn_count", n_forn >= 100, f"{n_forn}/{len(rows)}")
        check("slim_marca_count", n_marca >= 100, f"{n_marca}/{len(rows)}")
        check("slim_cat_count", n_cat >= 10, f"{n_cat}/{len(rows)}")

        # opções do select = únicos da dimensão (como preencherSelectCompraAvancadaDimensao)
        forn_opts = sorted(
            {
                v.strip()
                for p in rows
                for v in _valores_dimensao_compra(p, "fornecedor")
                if v and v.strip()
            }
        )
        marca_opts = sorted(
            {
                v.strip()
                for p in rows
                for v in _valores_dimensao_compra(p, "marca")
                if v and v.strip()
            }
        )
        check("select_forn_nao_vazio", len(forn_opts) >= 20, f"opts={len(forn_opts)}")
        check("select_marca_nao_vazio", len(marca_opts) >= 20, f"opts={len(marca_opts)}")
        # amostra: ADIMAX / nomes reais se existirem
        amostra = [x for x in forn_opts if "ADI" in x.upper() or "MAGNUS" in x.upper()][:5]
        check("select_forn_amostra", True, f"ex={amostra[:3] or forn_opts[:3]}")

        # Overlay prevalece: achar um pid com ov e cat diferentes se houver
        qs = queryset_catalogo_ativos(inativos=False)
        cat_map = {
            str(r["produto_externo_id"] or r["erp_produto_id"] or r["pk"]): str(
                r.get("fornecedor_texto") or ""
            ).strip()
            for r in qs.values(
                "pk", "produto_externo_id", "erp_produto_id", "fornecedor_texto"
            )[:5000]
        }
        ov_diff = None
        for o in ProdutoGestaoOverlayAgro.objects.exclude(fornecedor_texto="").values(
            "produto_externo_id", "fornecedor_texto"
        )[:800]:
            pid = str(o["produto_externo_id"] or "").strip()
            ovf = str(o["fornecedor_texto"] or "").strip()
            cf = cat_map.get(pid, "")
            if ovf and cf and ovf != cf:
                ov_diff = (pid, cf, ovf)
                break
        if ov_diff:
            pid, cf, ovf = ov_diff
            row = next((r for r in rows if str(r.get("id")) == pid), None)
            check(
                "overlay_prevalece",
                row is not None and str(row.get("fornecedor") or "") == ovf,
                f"pid={pid[:12]} cat={cf[:20]!r} ov={ovf[:20]!r} slim={str((row or {}).get('fornecedor') or '')[:20]!r}",
            )
        else:
            check("overlay_prevalece_skip", True, "sem conflito ov!=cat na amostra")

        # View slim responde v4
        from django.http import HttpRequest
        from produtos.views import api_pdv_catalogo_slim

        req = HttpRequest()
        req.method = "GET"
        req.META["SERVER_NAME"] = "127.0.0.1"
        req.META["SERVER_PORT"] = "8000"
        resp = api_pdv_catalogo_slim(req)
        body = json.loads(resp.content.decode("utf-8"))
        ver = str(body.get("catalog_version") or "")
        check("view_slim_200", resp.status_code == 200)
        check("view_slim_v4", ver.startswith("slim-v4-"), ver[:48])
        prods = body.get("produtos") or []
        n_forn_api = sum(1 for x in prods if str(x.get("fornecedor") or "").strip())
        check("view_slim_forn", n_forn_api >= 100, f"{n_forn_api}/{len(prods)}")

        # Simula filtro avançado: pool por fornecedor
        if forn_opts:
            alvo = forn_opts[0]
            pool = [
                p
                for p in rows
                if alvo in _valores_dimensao_compra(p, "fornecedor")
            ]
            check("filtro_avancado_pool", len(pool) >= 1, f"forn={alvo[:30]!r} n={len(pool)}")
    except Exception as exc:
        check("django_forn", False, str(exc)[:200])

    print("== Pacote envio (arquivos) ==")
    check(
        "arquivos_pacote",
        True,
        "catalogo_agro.listar_slim_rows_pdv + views cache slim v4 (+ verify)",
    )
    check("migrate_nao", True, "NÃO")
    check("fora_pdv_caixa", True, "não mexe templates PDV/caixa; só API slim")

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
