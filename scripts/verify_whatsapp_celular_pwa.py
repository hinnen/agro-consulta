"""
Prova PWA do Zap loja (celular standalone).

  python scripts/verify_whatsapp_celular_pwa.py
"""
from __future__ import annotations

import ast
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
fails: list[str] = []
oks: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    if cond:
        oks.append(name)
        print(f"  OK  {name}" + (f" — {detail}" if detail else ""))
    else:
        fails.append(name)
        print(f"  FAIL {name}" + (f" — {detail}" if detail else ""))


def _read(rel: str) -> str:
    return (ROOT / rel).read_text(encoding="utf-8")


def _fn_src(path: str, fn: str) -> str:
    tree = ast.parse(_read(path))
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name == fn:
            return ast.get_source_segment(_read(path), node) or ""
    return ""


def main() -> int:
    print("VERIFY WA-CEL-PWA")
    urls = _read("produtos/urls.py")
    views = _read("produtos/views_atendimento_whatsapp.py")
    cel = _read("produtos/templates/produtos/atendimento_whatsapp_celular.html")
    dash = _read("produtos/templates/produtos/dashboard_gerencial.html")
    dw = _read("produtos/static/produtos/js/agro_dual_window.js")
    man = _fn_src("produtos/views_atendimento_whatsapp.py", "atendimento_whatsapp_celular_manifest")
    sw = _fn_src("produtos/views_atendimento_whatsapp.py", "atendimento_whatsapp_celular_sw")

    check("url_manifest", "atendimento_whatsapp_celular_manifest" in urls)
    check("url_sw", "atendimento_whatsapp_celular_sw" in urls)
    check(
        "url_antes_tela",
        urls.find("atendimento-whatsapp/celular/manifest.webmanifest")
        < urls.find('"atendimento-whatsapp/celular/",'),
    )
    check("view_manifest", "def atendimento_whatsapp_celular_manifest" in views)
    check("view_sw", "def atendimento_whatsapp_celular_sw" in views)
    check("manifest_publico", "@login_required" not in man and "start_url" in man)
    check("manifest_scope", '"/atendimento-whatsapp/celular/"' in man and '"scope"' in man)
    check("manifest_standalone", '"standalone"' in man)
    check("sw_passthrough", "respondWith(fetch" in sw)
    check("sw_allowed", "Service-Worker-Allowed" in sw and "/atendimento-whatsapp/celular/" in sw)
    check("icon_192", (ROOT / "produtos/static/produtos/pwa/zap-loja-192.png").is_file())
    check("icon_512", (ROOT / "produtos/static/produtos/pwa/zap-loja-512.png").is_file())
    check("tpl_manifest", "atendimento_whatsapp_celular_manifest" in cel)
    check("tpl_sw", "serviceWorker" in cel and "atendimento_whatsapp_celular_sw" in cel)
    check("tpl_apple", "apple-mobile-web-app-capable" in cel and "Zap loja" in cel)
    check("tpl_sem_shell", "{% extends" not in cel and "agro_dual_window" not in cel and "agro_bug_report" not in cel)
    check("tpl_breakout", "window.top.location" in cel)
    check("sw_scope_js", 'scope: "/atendimento-whatsapp/celular/"' in cel)
    check("dash_top", "isWhatsAppCelularHref" in dash)
    check("dw_exclui", "isWhatsAppCelularPath" in dw)
    check("theme", "#075E54" in cel or "#075e54" in cel)

    print(f"\nVERIFY {'OK' if not fails else 'FAIL'} {len(oks)}/{len(oks) + len(fails)}")
    if fails:
        print("Falhas:", ", ".join(fails))
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
