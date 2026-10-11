"""
Checagens estáticas do PWA GM Lojas (Android + iOS).

  python3 scripts/verify_vendas_lojas_pwa_install.py
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
fails: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    if cond:
        print(f"  OK  {name}" + (f" -- {detail}" if detail else ""))
    else:
        fails.append(name)
        print(f"  FAIL {name}" + (f" -- {detail}" if detail else ""))


def read(rel: str) -> str:
    return (ROOT / rel).read_text(encoding="utf-8")


def main() -> int:
    print("== GM Lojas PWA install ==")
    head = read("produtos/templates/produtos/includes/vendas_lojas_pwa_head.html")
    install = read("produtos/templates/produtos/includes/vendas_lojas_pwa_install.html")
    views = read("produtos/views.py")
    pin = read("produtos/templates/produtos/vendas_lojas_pin.html")
    hub = read("produtos/templates/produtos/vendas_lojas_hub.html")
    urls = read("produtos/urls.py")

    check("head_manifest", "vendas_lojas_manifest" in head)
    check("head_apple_capable", "apple-mobile-web-app-capable" in head)
    check("head_apple_touch", "apple-touch-icon" in head)
    check("head_icons_png", "vendas-lojas-192.png" in head and "vendas-lojas-512.png" in head)

    check("install_beforeinstall", "beforeinstallprompt" in install)
    check("install_shell", "vl-pwa-shell" in install)
    check("install_ios_hint", "vl-pwa-steps-ios" in install and "Adicionar" in install)
    check("install_android_hint", "vl-pwa-steps-android" in install)
    check("install_inapp_hint", "vl-pwa-steps-inapp" in install and "Chrome" in install)
    check("install_btn_visible", "vl-pwa-instalar" in install and "disabled" in install)
    check("install_sw", "vendas_lojas_sw" in install)
    check("install_standalone_hide", "ehStandalone" in install)

    mf = views.split("def vendas_lojas_manifest(request):")[1].split("def vendas_lojas_sw")[0]
    check("manifest_fn", "payload = {" in mf)
    check("manifest_display", '"display": "standalone"' in mf)
    check("manifest_scope", '"scope": "/vendas/lojas/"' in mf)
    check("manifest_icons_192_512", '"192x192"' in mf and '"512x512"' in mf)
    check("manifest_prefer_related", '"prefer_related_applications": False' in mf)
    check("manifest_sistapp", '"name": "SistApp"' in mf and '"short_name": "SistApp"' in mf)

    check("pin_uses_head", "vendas_lojas_pwa_head.html" in pin)
    check("pin_uses_install", "vendas_lojas_pwa_install.html" in pin)
    check("hub_uses_install", "vendas_lojas_pwa_install.html" in hub)
    check("icon_files", (ROOT / "produtos/static/produtos/pwa/vendas-lojas-512.png").is_file())

    print(f"\n{len(fails)} falha(s)")
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
