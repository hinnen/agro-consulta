#!/usr/bin/env python
"""Prova PDV topbar WhatsApp — abre chat `/atendimento-whatsapp/`.

  python scripts/verify_pdv_wa_topbar_breve.py
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

import django

django.setup()

from django.urls import reverse

from produtos.pdv_topbar_clique_util import BOTAO_KEYS, registrar_clique
from produtos.pdv_topbar_layout_util import FRIO_DEFAULT, MOVABLE_KEYS, QUENTE_DEFAULT

ok = 0
fail = 0


def check(name: str, cond: bool, detail: str = ""):
    global ok, fail
    if cond:
        ok += 1
        print(f"  OK  {name}" + (f" — {detail}" if detail else ""))
    else:
        fail += 1
        print(f" FAIL {name}" + (f" — {detail}" if detail else ""))


def _read(rel: str) -> str:
    return (ROOT / rel).read_text(encoding="utf-8")


def main():
    print("=== PDV-WA-TOPBAR-CHAT ===")
    html = _read("produtos/templates/produtos/pdv_wizard.html")
    js = _read("produtos/static/produtos/js/pdv_topbar_whatsapp.js")
    layout_js = _read("produtos/static/produtos/js/pdv_topbar_layout.js")
    layout_util = _read("produtos/pdv_topbar_layout_util.py")
    mais_js = _read("produtos/static/produtos/js/pdv_topbar_mais.js")

    check("arquivo_js", (ROOT / "produtos/static/produtos/js/pdv_topbar_whatsapp.js").is_file())
    check("html_btn", 'id="pdv-topbar-whatsapp-btn"' in html)
    check("html_svg", "<svg" in html[html.find("pdv-topbar-whatsapp-btn") : html.find("pdv-topbar-whatsapp-btn") + 900])
    check("html_sr_only", "sr-only" in html and "WhatsApp" in html)
    check("html_classe_wa", "pdv-wiz-topbar-btn--wa" in html)
    check("html_css_wa", ".pdv-wiz-topbar-btn--wa" in html)
    check("html_cor_oficial", "#25D366" in html and "color: #fff" in html)
    check("html_chat_url", "data-wa-chat-url" in html and "atendimento_whatsapp" in html)
    check("html_overlay_legacy", 'id="pdv-wa-em-breve"' in html)

    check("js_abrir_chat", "abrirChat" in js and "data-wa-chat-url" in js)
    check("js_prevent", "preventDefault" in js and "stopPropagation" in js)
    check("js_open_blank", "window.open" in js)
    check("js_keep_place", "keepPlace" in js and "insertBefore" in js)
    check("js_sem_em_breve", "pdv-wa-em-breve" not in js)

    check("layout_sem_node_wa", "whatsapp" not in layout_js)
    check("util_sem_wa_movel", "whatsapp" not in QUENTE_DEFAULT and "whatsapp" not in FRIO_DEFAULT)
    check("util_nao_movel", "whatsapp" not in MOVABLE_KEYS)
    check("util_arquivo_sem_wa", "whatsapp" not in layout_util.lower())
    check("mais_nao_come_wa", "whatsapp" not in mais_js)

    check("clique_key", "whatsapp" in BOTAO_KEYS)
    ok_reg, err = registrar_clique(botao="whatsapp", deposito="centro")
    check("registrar_clique_wa", ok_reg, err)

    try:
        reverse("atendimento_whatsapp")
        check("url_chat_existe_mas_fora_deste_path", True, "lote loja = só botão, não a página")
    except Exception as e:
        check("url_chat_existe_mas_fora_deste_path", False, str(e)[:120])

    # Reverse PDV
    try:
        u = reverse("pdv_checkout")
        check("url_pdv", "/pdv/" in u or u.endswith("checkout/") or "pdv" in u)
    except Exception:
        try:
            u = reverse("pdv_home")
            check("url_pdv", "pdv" in u)
        except Exception as e:
            check("url_pdv", False, str(e)[:80])

    print(f"\nRESULTADO: {ok} ok / {fail} fail")
    return 0 if fail == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
