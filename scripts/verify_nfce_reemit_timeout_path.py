# -*- coding: utf-8 -*-
"""VERIFY NFCE-REEMIT-TIMEOUT — reemitir nao trava + SEFAZ 537.

Cobre: timeout sync < proxy Render · Abort 22s nas telas · lock anti-duplo ·
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
    n_try = max(1, len(delays))
    worst = n_try * (connect + read) + sum(delays[1:] if len(delays) > 1 else [])
    if connect <= 5 and read <= 18:
        ok(f"TIMEOUT_SYNC=({connect},{read})")
    else:
        fail(f"TIMEOUT_SYNC alto ({connect},{read}) — estoura proxy/Abort")
    if n_try <= 1:
        ok(f"RETRY_DELAYS_SYNC 1 tentativa ({delays})")
    elif worst <= 22:
        ok(f"RETRY_DELAYS_SYNC={delays} pior~{worst:.1f}s")
    else:
        fail(f"RETRY_DELAYS_SYNC pesado: {delays} pior~{worst:.1f}s")
    if worst <= 22:
        ok(f"orcamento SEFAZ sync pior caso ~{worst:.1f}s (<=22)")
    else:
        fail(f"orcamento SEFAZ sync ~{worst:.1f}s > 22s")

    from produtos.sefaz_soap_util import SEFAZ_HTTP_TIMEOUT

    if SEFAZ_HTTP_TIMEOUT[1] >= SEFAZ_HTTP_TIMEOUT_SYNC[1]:
        ok("perfil completo >= sync (background)")
    else:
        fail("perfil completo menor que sync")


def check_views_lock() -> None:
    print("\n[3] views_nfce — lock + sync")
    txt = read("produtos/views_nfce.py")
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
        "timeout=45",
    ):
        if n in txt:
            ok(f"views: `{n[:48]}`")
        else:
            fail(f"views: falta `{n}`")
    if re.search(r"try:\s*\n\s*return _api_venda_agro_nfce_emitir_locked", txt) and "finally:" in txt:
        ok("lock liberado no finally")
    else:
        fail("lock sem finally")


def check_vdesc_all_items() -> None:
    print("\n[4] XML — vDesc em todos os itens (537) + gravar apos SEFAZ")
    em = read("produtos/nfce_sp_emissao_util.py")
    if "SEFAZ 537" in em or "537" in em:
        ok("comentario/ref 537")
    else:
        fail("falta ref 537 no emissao")
    if "if v_desc > 0:" in em and '_sub(prod, "vDesc"' in em:
        ok("vDesc escrito quando total > 0")
    else:
        fail("padrao vDesc em todos itens ausente")
    if re.search(r"if v_desc_item > 0:\s*\n\s*_sub\(prod, \"vDesc\"", em):
        fail("ainda so escreve vDesc se item > 0 (omitir 0.00)")
    else:
        ok("nao omite vDesc 0.00 quando ha desconto total")
    if "def _gravar_doc_nfce_venda" in em:
        ok("_gravar_doc_nfce_venda")
    else:
        fail("falta _gravar_doc_nfce_venda")
    if re.search(
        r"reutilizada.: True,\s*\n\s*\}\s*\n\s*NfceDocumentoAgro\.objects\.filter\(venda=venda\)\.exclude",
        em,
    ):
        fail("ainda apaga doc rejeitado ANTES da SEFAZ")
    else:
        ok("nao apaga rejeitada antes da SEFAZ")


def check_ui_abort() -> None:
    print("\n[5] UI AbortController 22s")
    for rel in (
        "produtos/templates/produtos/vendas_lista.html",
        "produtos/templates/produtos/venda_agro_detalhe.html",
    ):
        txt = read(rel)
        if "AbortController" not in txt:
            fail(f"{rel}: sem AbortController")
            continue
        ok(f"{rel}: AbortController")
        if "22000" in txt:
            ok(f"{rel}: 22000ms")
        else:
            fail(f"{rel}: falta 22000")
        if "ctrl.abort" in txt or "ctrl.abort()" in txt:
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
        if "gmLoadingBar" in txt and "finally" in txt:
            ok(f"{rel}: finally esconde loading")
        else:
            fail(f"{rel}: loading pode ficar preso")
    lista = read("produtos/templates/produtos/vendas_lista.html")
    if "Desconto já foi corrigido" in lista:
        ok("vendas_lista: tip 537 no modal")
    else:
        fail("vendas_lista: falta tip 537")


def check_budget_js_vs_server() -> None:
    print("\n[6] JS abort (22s) > orcamento SEFAZ sync")
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
    import django

    django.setup()
    from produtos.sefaz_soap_util import SEFAZ_HTTP_RETRY_DELAYS_SYNC, SEFAZ_HTTP_TIMEOUT_SYNC

    connect, read = SEFAZ_HTTP_TIMEOUT_SYNC
    delays = SEFAZ_HTTP_RETRY_DELAYS_SYNC
    n_try = max(1, len(delays))
    worst = n_try * (connect + read) + sum(delays[1:] if len(delays) > 1 else [])
    if worst < 22:
        ok(f"servidor ~{worst:.1f}s < JS 22s")
    else:
        fail(f"servidor ~{worst:.1f}s >= JS 22s (abort antes da resposta)")


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
