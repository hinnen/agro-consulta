"""
Prova PWA do Ajuste mobile (Instalar no Chrome).

  python scripts/verify_ajuste_mobile_pwa.py
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
    print("VERIFY AJUSTE-MOBILE-PWA")
    urls = _read("produtos/urls.py")
    views = _read("produtos/views.py")
    ma = _read("produtos/templates/produtos/mobile_ajuste.html")
    login = _read("produtos/templates/produtos/ajuste_mobile_login.html")
    man = _fn_src("produtos/views.py", "ajuste_mobile_manifest")
    sw = _fn_src("produtos/views.py", "ajuste_mobile_sw")
    view = _fn_src("produtos/views.py", "ajuste_mobile_view")

    check("url_manifest", "ajuste_mobile_manifest" in urls)
    check("url_sw", "ajuste_mobile_sw" in urls)
    check(
        "url_antes_tela",
        urls.find("ajuste-mobile/manifest.webmanifest") < urls.find("path('ajuste-mobile/', views.ajuste_mobile_view"),
    )
    check("url_sw_antes_tela", urls.find("ajuste-mobile/sw.js") < urls.find("path('ajuste-mobile/', views.ajuste_mobile_view"))
    check("view_manifest", "def ajuste_mobile_manifest" in views)
    check("view_sw", "def ajuste_mobile_sw" in views)
    check("manifest_publico", "@login_required" not in man and "start_url" in man)
    check("manifest_scope", '"/ajuste-mobile/"' in man and '"scope"' in man)
    check("manifest_standalone", '"standalone"' in man)
    check("sw_passthrough", "respondWith(fetch" in sw)
    check("sw_sem_cache_store", "caches.open" not in sw and "cache.put" not in sw)
    check("sw_allowed", 'Service-Worker-Allowed' in sw and "/ajuste-mobile/" in sw)
    check("view_pin_igual", "ajuste_mobile_gate" in view)
    check("icon_192", (ROOT / "produtos/static/produtos/pwa/ajuste-mobile-192.png").is_file())
    check("icon_512", (ROOT / "produtos/static/produtos/pwa/ajuste-mobile-512.png").is_file())
    check("tpl_ma_manifest", "ajuste_mobile_manifest" in ma)
    check("tpl_ma_sw", "serviceWorker" in ma and "ajuste_mobile_sw" in ma)
    check("tpl_login_manifest", "ajuste_mobile_manifest" in login)
    check("tpl_login_sw", "serviceWorker" in login and "ajuste_mobile_sw" in login)
    check("tpl_apple_icon", "ajuste-mobile-192.png" in ma and "ajuste-mobile-192.png" in login)
    check("tpl_apple_title", "Ajuste" in ma and "apple-mobile-web-app-title" in ma)
    check("sw_scope_js", 'scope: "/ajuste-mobile/"' in ma and 'scope: "/ajuste-mobile/"' in login)
    check("tpl_numeros_grandes", "clamp(2.15rem" in ma and "clamp(1.55rem" in ma)
    check("tpl_numpad_toque", "min-height: 2.95rem" in ma)
    check("tpl_busca_alta", "min-height: 3.25rem" in ma)
    check("tpl_login_pin_grande", "min-h-[4.25rem]" in login)
    check("tpl_cabecalho_grade5", "repeat(5, minmax(0, 1fr))" in ma)
    check("tpl_cabecalho_titulo_sozinho", "ma-head-row--title" in ma)
    ver = _read("VERSION").strip()
    check("version_bump", ver >= "16.51", ver)

    print(f"\nVERIFY {'OK' if not fails else 'FAIL'} {len(oks)}/{len(oks) + len(fails)}")
    if fails:
        print("Falhas:", ", ".join(fails))
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
