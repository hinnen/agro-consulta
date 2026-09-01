"""
Prova atendimento WhatsApp (path WA-ATEND-QR).

  python scripts/verify_atendimento_whatsapp.py
"""
from __future__ import annotations

import os
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


def _static() -> None:
    print("VERIFY WA-ATEND-QR — estático")
    urls = _read("produtos/urls.py")
    models = _read("produtos/models.py")
    util = _read("produtos/atendimento_whatsapp_util.py")
    views = _read("produtos/views_atendimento_whatsapp.py")
    html = _read("produtos/templates/produtos/atendimento_whatsapp.html")
    js = _read("produtos/static/produtos/js/atendimento_whatsapp.js")
    dash = _read("produtos/templates/produtos/dashboard_gerencial.html")
    home = _read("produtos/views.py")
    sett = _read("config/settings.py")
    bat = _read("whatsapp_atendimento/iniciar.bat")
    node = _read("whatsapp_atendimento/index.js")
    gitig = _read(".gitignore")
    mig = ROOT / "produtos/migrations/0108_atendimento_whatsapp.py"

    check("url_pagina", "atendimento_whatsapp" in urls and "atendimento-whatsapp/" in urls)
    check("url_bot_cfg", "atendimento_whatsapp_bot" in urls)
    check("model_bot_cfg", "class WhatsAppBotConfigAgro" in models)
    check("mig_0111", (ROOT / "produtos/migrations/0111_whatsapp_bot_config.py").is_file())
    check("html_btn_bot", "atendimento_whatsapp_bot" in html)
    check("js_bot_cfg", "atendimento_whatsapp_bot.js" in _read("produtos/templates/produtos/atendimento_whatsapp_bot.html"))
    check("url_bridge_entrada", "api_atendimento_whatsapp_bridge_entrada" in urls)
    check("url_bridge_saida", "api_atendimento_whatsapp_bridge_saida" in urls)
    check("model_ponte", "class WhatsAppPonteEstadoAgro" in models)
    check("model_conv", "class WhatsAppConversaAgro" in models)
    check("model_msg", "class WhatsAppMensagemAgro" in models)
    check("migrate", mig.is_file() and "WhatsAppConversaAgro" in mig.read_text(encoding="utf-8"))
    check("util_bot", "MSG_MENU" in util and "interpretar_loja" in util)
    check("util_fiado_msg", "interpretar_consulta_fiado" in util and "montar_texto_fiado" in util)
    check("menu_cita_fiado", "escreva *fiado*" in util)
    check("util_entrada", "def processar_entrada" in util)
    check("view_csrf_bridge", "csrf_exempt" in views)
    check("html_abas", 'data-loja="vila"' in html and 'data-loja="centro"' in html)
    check("js_poll", "2500" in js)
    check("menu_dash", "atendimento_whatsapp" in dash)
    check("menu_home", "WhatsApp lojas" in home)
    check("settings_token", "AGRO_WA_BRIDGE_TOKEN" in sett)
    check("bat_iniciar", "node index.js" in bat)
    check("node_baileys", "@whiskeysockets/baileys" in node or "makeWASocket" in node)
    check("gitignore_auth", "whatsapp_atendimento/auth/" in gitig)
    html_pdv = _read("produtos/templates/produtos/pdv_wizard.html")
    check("pdv_wa_btn", 'id="pdv-topbar-whatsapp-btn"' in html_pdv)
    check("pdv_wa_js", "pdv_topbar_whatsapp.js" in html_pdv)


def _logic() -> None:
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
    sys.path.insert(0, str(ROOT))
    import django

    django.setup()
    from produtos.atendimento_whatsapp_bot_config import BOT_DEFAULT, delays_bot
    from produtos.atendimento_whatsapp_util import interpretar_consulta_fiado, interpretar_loja

    check("logic_vila", interpretar_loja("2") == "vila")
    check("logic_centro", interpretar_loja("centro") == "centro")
    check("logic_vazio", interpretar_loja("oi") == "")
    check("logic_fiado", interpretar_consulta_fiado("fiado") is True)
    check("logic_fiado_nao_loja", interpretar_consulta_fiado("1") is False)
    check("logic_delay", delays_bot(BOT_DEFAULT, 2)[0] >= 0)


def main() -> int:
    _static()
    try:
        _logic()
    except Exception as e:
        check("logic", False, str(e)[:200])
    print(f"\n{len(oks)} ok · {len(fails)} fail")
    if fails:
        print("FAIL:", ", ".join(fails))
        return 1
    print("VERIFY_OK WA-ATEND-QR")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
