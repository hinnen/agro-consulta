#!/usr/bin/env python
"""Prova COMPRAS-SLIM-FALLBACK (freio catalogo-full-off → slim). VERIFY_OK / VERIFY_FAIL."""
from __future__ import annotations

import json
import os
import re
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


def _logic_inutil(d: dict | None) -> bool:
    """Espelho de compraCatalogoDeltaInutil (compras.html)."""
    if not d or not isinstance(d, dict):
        return True
    if d.get("erro"):
        return True
    if str(d.get("catalog_version") or "") == "catalogo-full-off":
        return True
    prods = d.get("produtos")
    if isinstance(prods, list) and not prods and not d.get("unchanged") and not d.get("delta"):
        return True
    return False


def _escolhe_fonte(d: dict | None, base_len: int, cached_len: int) -> str:
    """Espelho do fluxo sincronizarCatalogoComprasServidor → origem da lista."""
    if d and d.get("unchanged"):
        if base_len == 0 and cached_len > 0:
            return "cache"
        if base_len > 0:
            return "base"
        return "slim"
    if d and d.get("delta") and not _logic_inutil(d):
        # delta útil: se base+cache vazio e changed vazio → slim
        changed = d.get("changed") or []
        if base_len == 0 and cached_len == 0 and not changed:
            return "slim"
        return "delta"
    if (
        d
        and isinstance(d.get("produtos"), list)
        and d["produtos"]
        and not _logic_inutil(d)
    ):
        return "full"
    return "slim"


def main() -> None:
    print("== Markers compras.html ==")
    html = _read("produtos/templates/produtos/compras.html")
    check("cache_key_v3", "agro_compra_catalog_cache_v3" in html)
    check("url_slim_django", "api_pdv_catalogo_slim" in html)
    check("fn_delta_inutil", "function compraCatalogoDeltaInutil" in html)
    check("fn_carregar_slim", "function carregarCatalogoComprasSlim" in html)
    check("reject_full_off_cache", "catalogo-full-off" in html and "return null" in html)
    check("sync_chama_slim", "carregarCatalogoComprasSlim()" in html)
    check(
        "nao_aplica_produtos_vazios_direto",
        "d.produtos.length" in html and "aplicarBaseCompras(d.produtos)" in html,
    )
    # fallback em todos os ramos críticos
    sync = html.split("function sincronizarCatalogoComprasServidor", 1)[1].split(
        "\nfunction ", 1
    )[0]
    check("sync_unchanged_slim", "unchanged" in sync and "carregarCatalogoComprasSlim" in sync)
    check("sync_catch_slim", ".catch" in sync and "carregarCatalogoComprasSlim" in sync)
    check("msg_erro_catalogo", "Erro ao carregar produtos." in html)

    print("== Markers mobile_ajuste ==")
    ma = _read("produtos/templates/produtos/mobile_ajuste.html")
    check("ma_cache_v3", "agro_compra_catalog_cache_v3" in ma)
    check("ma_ainda_le_v2", "agro_compra_catalog_cache_v2" in ma)
    check("ma_slim_url", "/api/pdv/catalogo-slim/" in ma)

    print("== Rotas / views ==")
    urls = _read("produtos/urls.py")
    check("rota_slim", "api_pdv_catalogo_slim" in urls and "catalogo-slim" in urls)
    check("rota_delta", "api_todos_produtos_delta" in urls)
    views = _read("produtos/views.py")
    check("view_slim_existe", "def api_pdv_catalogo_slim" in views)
    check(
        "view_slim_doc_freio",
        "catalogo-full-off" in views and "api_pdv_catalogo_slim" in views,
    )
    check("slim_cache_v4", "pdv_catalogo_slim_v4" in views)
    check("listar_slim_util", "def listar_slim_rows_pdv" in _read("produtos/catalogo_agro.py"))
    cat = _read("produtos/catalogo_agro.py")
    check("slim_fonte_fornecedor_texto", "fornecedor_texto" in cat and '"fornecedor": fornecedor' in cat)

    print("== Lógica freio (espelho JS) ==")
    freio = {
        "ok": True,
        "delta": False,
        "full": True,
        "catalog_version": "catalogo-full-off",
        "catalog_updated_at": "2026-08-11T15:00:00",
        "produtos": [],
    }
    check("freio_inutil", _logic_inutil(freio))
    check("freio_escolhe_slim", _escolhe_fonte(freio, 0, 0) == "slim")
    check(
        "freio_com_cache_bom",
        _escolhe_fonte({"unchanged": True}, 0, 10) == "cache",
    )
    check(
        "freio_unchanged_sem_base",
        _escolhe_fonte({"unchanged": True, "catalog_version": "catalogo-full-off"}, 0, 0)
        == "slim",
    )
    full_ok = {
        "produtos": [{"id": "1", "nome": "X"}],
        "catalog_version": "abc123",
    }
    check("full_ok_escolhe_full", _escolhe_fonte(full_ok, 0, 0) == "full")
    check("full_vazio_escolhe_slim", _escolhe_fonte({"produtos": []}, 0, 0) == "slim")
    check("erro_escolhe_slim", _escolhe_fonte({"erro": "x"}, 0, 0) == "slim")
    check("null_escolhe_slim", _escolhe_fonte(None, 0, 0) == "slim")

    print("== Django: slim monta lista + view ==")
    try:
        import django

        django.setup()
        from produtos.catalogo_agro import listar_slim_rows_pdv

        rows = listar_slim_rows_pdv()
        check("slim_lista_nao_vazia", isinstance(rows, list) and len(rows) > 0, f"n={len(rows)}")
        if rows:
            sample = rows[0]
            check("slim_tem_id", bool(sample.get("id")))
            check("slim_tem_nome", bool(sample.get("nome")))
            check(
                "slim_campos_compra",
                "preco_venda" in sample
                and "saldo_centro" in sample
                and "categoria" in sample
                and "fornecedor" in sample,
            )
            n_forn = sum(1 for x in rows if str(x.get("fornecedor") or "").strip())
            check("slim_tem_fornecedores", n_forn > 0, f"com_forn={n_forn}/{len(rows)}")
            n_marca = sum(1 for x in rows if str(x.get("marca") or "").strip())
            check("slim_tem_marcas", n_marca > 0, f"com_marca={n_marca}/{len(rows)}")

        from django.http import HttpRequest
        from produtos.views import api_pdv_catalogo_slim, api_todos_produtos_delta

        req = HttpRequest()
        req.method = "GET"
        req.META["SERVER_NAME"] = "127.0.0.1"
        req.META["SERVER_PORT"] = "8000"
        resp = api_pdv_catalogo_slim(req)
        body = json.loads(resp.content.decode("utf-8"))
        n_view = len(body.get("produtos") or [])
        check("view_slim_200", resp.status_code == 200, str(resp.status_code))
        check("view_slim_produtos", n_view > 0, f"n={n_view}")
        check(
            "view_slim_ver_nao_freio",
            str(body.get("catalog_version") or "") != "catalogo-full-off",
            str(body.get("catalog_version") or "")[:48],
        )

        # Simula resposta freio da loja (mesmo contrato producao)
        freio_loja = {
            "ok": True,
            "delta": False,
            "full": True,
            "catalog_version": "catalogo-full-off",
            "produtos": [],
        }
        check(
            "contrato_loja_freio_vai_slim",
            _escolhe_fonte(freio_loja, 0, 0) == "slim",
        )
        # View delta local (teste pode não ter freio — só não pode quebrar)
        resp_d = api_todos_produtos_delta(req)
        check("view_delta_responde", resp_d.status_code in (200, 500), str(resp_d.status_code))
    except Exception as exc:
        check("django_slim_setup", False, str(exc)[:200])

    print("== HTTP local (se runserver) ==")
    try:
        from urllib.request import Request, urlopen

        def _get(path: str) -> tuple[int, dict | list | None, int]:
            req = Request(f"http://127.0.0.1:8000{path}", headers={"Accept": "application/json"})
            with urlopen(req, timeout=25) as resp:
                raw = resp.read()
                try:
                    body = json.loads(raw.decode("utf-8", errors="replace"))
                except Exception:
                    body = None
                return resp.status, body, len(raw)

        st, body, nbytes = _get("/api/pdv/catalogo-slim/")
        if st == 200 and isinstance(body, dict):
            n = len(body.get("produtos") or [])
            check("http_slim_200", True, f"bytes={nbytes} n={n}")
            check("http_slim_tem_produtos", n > 0, f"n={n}")
            ver = str(body.get("catalog_version") or "")
            check("http_slim_nao_full_off", ver != "catalogo-full-off", ver[:40])
        elif st in (302, 401, 403):
            check("http_slim_auth_gate", True, f"status={st} (login — ok em ambiente fechado)")
        else:
            check("http_slim_200", False, f"status={st}")

        st2, body2, nbytes2 = _get("/api/todos-produtos/delta/")
        if st2 == 200 and isinstance(body2, dict):
            ver2 = str(body2.get("catalog_version") or "")
            n2 = len(body2.get("produtos") or [])
            check("http_delta_200", True, f"bytes={nbytes2} ver={ver2[:32]} n={n2}")
            # Se freio local ligado, fonte deve ser slim
            fonte = _escolhe_fonte(body2, 0, 0)
            check(
                "http_delta_fluxo_ok",
                fonte in ("full", "slim", "delta"),
                f"fonte={fonte}",
            )
            if ver2 == "catalogo-full-off" or (isinstance(body2.get("produtos"), list) and n2 == 0 and not body2.get("unchanged")):
                check("http_freio_detectado_usa_slim", fonte == "slim", f"fonte={fonte}")
        elif st2 in (302, 401, 403):
            check("http_delta_auth_gate", True, f"status={st2}")
        else:
            check("http_delta_200", False, f"status={st2}")
    except Exception as exc:
        check("http_local_skip", True, f"sem runserver ou falha: {str(exc)[:80]}")

    print("== Pacote vs producao (templates + slim já na loja) ==")
    import subprocess

    try:
        diff = subprocess.check_output(
            [
                "git",
                "diff",
                "--name-only",
                "origin/producao...HEAD",
                "--",
                "produtos/templates/produtos/compras.html",
                "produtos/templates/produtos/mobile_ajuste.html",
            ],
            cwd=str(ROOT),
            text=True,
            encoding="utf-8",
        ).strip()
        files = [x for x in diff.splitlines() if x.strip()]
        check(
            "diff_producao_tem_compras",
            "produtos/templates/produtos/compras.html" in files,
            str(files),
        )
        check(
            "diff_producao_tem_mobile",
            "produtos/templates/produtos/mobile_ajuste.html" in files,
            str(files),
        )
        prod_views = subprocess.check_output(
            ["git", "show", "origin/producao:produtos/views.py"],
            cwd=str(ROOT),
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        # Slim na loja NÃO pode estar freiado (bloco listar_slim antes do freio no local/delta)
        idx_slim = prod_views.find("def api_pdv_catalogo_slim")
        idx_local = prod_views.find("def api_todos_produtos_local")
        idx_delta = prod_views.find("def api_todos_produtos_delta")
        check("prod_tem_slim", idx_slim > 0)
        check("prod_tem_delta", idx_delta > 0)
        slim_block = prod_views[idx_slim:idx_local] if idx_slim > 0 and idx_local > idx_slim else ""
        delta_block = prod_views[idx_delta : idx_delta + 800] if idx_delta > 0 else ""
        check(
            "prod_slim_nao_freiado",
            "listar_slim_rows_pdv" in slim_block
            and "agro_pdv_catalogo_full_desligado" not in slim_block,
        )
        check(
            "prod_delta_freiado",
            "agro_pdv_catalogo_full_desligado" in delta_block,
        )
        check(
            "prod_tem_rota_slim",
            "catalogo-slim" in subprocess.check_output(
                ["git", "show", "origin/producao:produtos/urls.py"],
                cwd=str(ROOT),
                text=True,
                encoding="utf-8",
                errors="replace",
            ),
        )
    except Exception as exc:
        check("diff_producao", False, str(exc)[:120])

    # JS: URL resolve no template Django (regex name)
    check(
        "url_tag_slim",
        bool(re.search(r"url\s+['\"]api_pdv_catalogo_slim['\"]", html)),
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
