"""
Prova Chat loja PDV (grupo único + som).

  python scripts/verify_pdv_chat_loja.py
"""
from __future__ import annotations

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


def main() -> int:
    print("VERIFY PDV-CHAT-LOJA")
    urls = _read("produtos/urls.py")
    html = _read("produtos/templates/produtos/partials/pdv/chat_loja_overlay.html")
    js = _read("produtos/static/produtos/js/pdv_chat_loja.js")
    wiz = _read("produtos/templates/produtos/pdv_wizard.html")
    util = _read("produtos/pdv_chat_loja_util.py")
    views = _read("produtos/views_pdv_chat_loja.py")
    boot = _read("pdv/views.py")
    models = _read("produtos/models.py")
    mig = ROOT / "produtos/migrations/0105_chat_loja_mensagem.py"

    check("url_lista", "api_pdv_chat_loja_lista" in urls)
    check("url_enviar", "api_pdv_chat_loja_enviar" in urls)
    check("wizard_botao", "pdv-chat-loja-fab" in wiz or "pdv-chat-loja-fab" in html)
    check("wizard_include", "chat_loja_overlay.html" in wiz)
    check("wizard_js", "pdv_chat_loja.js" in wiz)
    check("boot_urls", "apiPdvChatLojaLista" in boot and "apiPdvChatLojaEnviar" in boot)
    check("overlay_id", 'id="pdv-chat-loja-overlay"' in html)
    check("dock_msn", 'id="pdv-chat-loja-dock"' in html and "bottom:" in html)
    check("js_beep", "clBeep" in js and "AudioContext" in js)
    check("js_poll", "POLL_MS" in js and "after_id" in js)
    check("js_device", "agro_device_id_v1" in js)
    check("js_fab", "pdv-chat-loja-fab" in js)
    check("util_criar", "def criar_mensagem" in util)
    check("util_listar", "def listar_mensagens" in util)
    check("view_lista", "def api_pdv_chat_loja_lista" in views)
    check("view_enviar", "def api_pdv_chat_loja_enviar" in views)
    check("model", "class ChatLojaMensagemAgro" in models)
    check("migrate", mig.is_file())
    check("texto_max_500", "TEXTO_MAX = 500" in util)
    check("sem_topbar", "pdv-topbar-chat-loja-btn" not in wiz)

    print()
    print(f"RESULT {len(oks)}/{len(oks) + len(fails)}")
    if fails:
        print("FAILED:", ", ".join(fails))
        return 1
    print("VERIFY_OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
