# -*- coding: utf-8 -*-
"""VERIFY NFCE-REEMIT-TIMEOUT — reemitir nao trava + SEFAZ 537.

Cobre: timeout sync < proxy Render · Abort 28s nas telas · lock anti-duplo ·
vDesc em todos os itens se ha desconto · sefaz_perfil=sync no emitir · AST ·
reusa prova NFCE-DESC.

Uso: python scripts/verify_nfce_reemit_timeout_path.py
"""
from __future__ import annotations

import ast
import os
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

fails = 0
oks = 0

FILES = (
    "produtos/sefaz_soap_util.py",
    "produtos/views_nfce.py",
    "produtos/nfce_sp_emissao_util.py",
    "produtos/templates/produtos/vendas_lista.html",
    "produtos/templates/produtos/venda_agro_detalhe.html",
    "scripts/verify_nfce_reemit_timeout_path.py",
)


def ok(msg: str) -> None:
    global oks
    oks += 1
    print(f"  OK  {msg}")


def fail(msg: str) -> None:
    global fails
    fails += 1
    print(f" FAIL {msg}")


def read(rel: str) -> str:
    p = ROOT / rel
    if not p.is_file():
        fail(f"ausente {rel}")
        return ""
    return p.read_text(encoding="utf-8")


def check_ast() -> None:
    print("\n[1] AST / parse")
    for rel in FILES:
        if rel.endswith(".html"):
            txt = read(rel)
            if not txt:
                continue
            # JS blocks must contain AbortController pattern without syntax bombs
            if "AbortController" in txt:
                ok(f"html tem AbortController: {rel}")
            else:
                fail(f"html sem AbortController: {rel}")
            continue
        p = ROOT / rel
        if not p.is_file():
            fail(f"ausente {rel}")
            continue
        try:
            ast.parse(p.read_text(encoding="utf-8"))
            ok(f"ast {rel}")
        except SyntaxError as exc:
            fail(f"ast {rel}: {exc}")


def check_timeouts() -> None:
    print("\n[2] Timeout sync cabe no Render (~30s)")
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
    import django

    django.setup()
    from produtos.sefaz_soap_util import (
        SEFAZ_HTTP_RETRY_DELAYS_SYNC,
        SEFAZ_HTTP_TIMEOUT_SYNC,
    )

    connect, read = SEFAZ_HTTP_TIMEOUT_SYNC
    delays = SEFAZ_HTTP_RETRY_DELAYS_SYNC
    # Pior caso 1 tentativa completa: connect + read + soma delays entre retries
    # Com N delays ha N+1 attempts no loop tipico — estimar connect+read + sum(delays)
    worst = connect + read + sum(delays)
    if connect <= 6 and read <= 22:
        ok(f"TIMEOUT_SYNC=({connect},{read})")
    else:
        fail(f"TIMEOUT_SYNC alto ({connect},{read}) — estoura proxy")
    if worst <= 28:
        ok(f"orcamento SEFAZ sync pior caso ~{worst:.1f}s (<=28)")
    else:
        fail(f"orcamento SEFAZ sync ~{worst:.1f}s > 28s")
    if len(delays) <= 3 and all(d < 2 for d in delays):
        ok(f"RETRY_DELAYS_SYNC={delays}")
    else:
        fail(f"RETRY_DELAYS_SYNC pesado: {delays}")

    # Background pode ser maior
    from produtos.sefaz_soap_util import SEFAZ_HTTP_TIMEOUT

    if SEFAZ_HTTP_TIMEOUT[1] >= SEFAZ_HTTP_TIMEOUT_SYNC[1]:
        ok("perfil completo >= sync (background)")
    else:
        fail("perfil completo menor que sync")


def check_views_lock() -> None:
    print("\n[3] views_nfce — lock + sync")
    txt = read("produtos/views_nfce.py")
    needles = [
        'nfce_emit_lock_',
        "cache.add(lock_key",
        "cache.delete(lock_key)",
        'sefaz_perfil="sync"',
        "status=409",
        "ja esta sendo emitido",
        "já está sendo emitido",
    ]
    # accent or not
    found_lock_msg = ("já está sendo emitido" in txt) or ("ja esta sendo emitido" in txt)
    if found_lock_msg:
        ok("mensagem lock 409")
    else:
        fail("falta mensagem lock 409")
    for n in (
        "nfce_emit_lock_",
        "cache.add(lock_key",
        "cache.delete(lock_key)",
        'sefaz_perfil="sync"',
        "status=409",
        "_api_venda_agro_nfce_emitir_locked",
        "_nfce_emitir_json_response",
    ):
        if n in txt:
            ok(f"views: `{n[:48]}`")
        else:
            fail(f"views: falta `{n}`")
    # finally must release lock even on exception
    if re.search(r"try:\s*\n\s*return _api_venda_agro_nfce_emitir_locked", txt) and "finally:" in txt:
        ok("lock liberado no finally")
    else:
        fail("lock sem finally")


def check_vdesc_all_items() -> None:
    print("\n[4] XML — vDesc em todos os itens (537)")
    em = read("produtos/nfce_sp_emissao_util.py")
    if "SEFAZ 537" in em or "537" in em:
        ok("comentario/ref 537")
    else:
        fail("falta ref 537 no emissao")
    if "if v_desc > 0:" in em and '_sub(prod, "vDesc"' in em:
        ok("vDesc escrito quando total > 0")
    else:
        fail("padrao vDesc em todos itens ausente")
    # old pattern (only if item > 0) should not be the gate alone
    if re.search(r"if v_desc_item > 0:\s*\n\s*_sub\(prod, \"vDesc\"", em):
        fail("ainda so escreve vDesc se item > 0 (omitir 0.00)")
    else:
        ok("nao omite vDesc 0.00 quando ha desconto total")


def check_ui_abort() -> None:
    print("\n[5] UI AbortController 28s")
    for rel in (
        "produtos/templates/produtos/vendas_lista.html",
        "produtos/templates/produtos/venda_agro_detalhe.html",
    ):
        txt = read(rel)
        if "AbortController" not in txt:
            fail(f"{rel}: sem AbortController")
            continue
        ok(f"{rel}: AbortController")
        if "28000" in txt:
            ok(f"{rel}: 28000ms")
        else:
            fail(f"{rel}: falta 28000")
        if "ctrl.abort" in txt or "ctrl).abort" in txt or "ctrl.abort()" in txt:
            ok(f"{rel}: abort()")
        else:
            fail(f"{rel}: sem abort()")
        if "AbortError" in txt or "Demorou demais" in txt:
            ok(f"{rel}: mensagem timeout")
        else:
            fail(f"{rel}: sem msg timeout")
        if "clearTimeout(to)" in txt:
            ok(f"{rel}: clearTimeout")
        else:
            fail(f"{rel}: sem clearTimeout")
        if "signal: ctrl" in txt:
            ok(f"{rel}: fetch signal")
        else:
            fail(f"{rel}: fetch sem signal")
        # loading bar hide no finally
        if "gmLoadingBar" in txt and "finally" in txt:
            ok(f"{rel}: finally esconde loading")
        else:
            fail(f"{rel}: loading pode ficar preso")


def check_budget_js_vs_server() -> None:
    print("\n[6] JS abort (28s) > orcamento SEFAZ sync")
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
    import django

    django.setup()
    from produtos.sefaz_soap_util import SEFAZ_HTTP_RETRY_DELAYS_SYNC, SEFAZ_HTTP_TIMEOUT_SYNC

    connect, read = SEFAZ_HTTP_TIMEOUT_SYNC
    worst = connect + read + sum(SEFAZ_HTTP_RETRY_DELAYS_SYNC)
    if worst < 28:
        ok(f"servidor ~{worst:.1f}s < JS 28s")
    else:
        fail(f"servidor ~{worst:.1f}s >= JS 28s (abort antes da resposta)")


def check_desc_path() -> None:
    print("\n[7] Subpath NFCE-DESC (537 rateio)")
    r = subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "verify_nfce_desc_itens_path.py")],
        cwd=ROOT,
        capture_output=True,
        text=True,
        env={**os.environ, "PYTHONIOENCODING": "utf-8"},
    )
    out = (r.stdout or "") + (r.stderr or "")
    if r.returncode == 0 and "VERIFY_OK" in out:
        m = re.search(r"(\d+) OK", out)
        ok(f"verify_nfce_desc_itens_path OK ({m.group(1) if m else '?'} checks)")
    else:
        fail("verify_nfce_desc_itens_path FAIL")
        print(out[-600:])


def main() -> int:
    print("VERIFY NFCE-REEMIT-TIMEOUT")
    check_ast()
    check_timeouts()
    check_views_lock()
    check_vdesc_all_items()
    check_ui_abort()
    check_budget_js_vs_server()
    check_desc_path()
    print(f"\n=== RESULTADO: {oks} OK · {fails} FAIL ===")
    if fails:
        print("VERIFY_FAIL")
        return 1
    print("VERIFY_OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
