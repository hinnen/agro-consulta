# -*- coding: utf-8 -*-
"""WA-APP-SEM-PDV — Zap app sem Voltar PDV / FAB F1; Bot fica no Zap."""
from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
os.chdir(ROOT)
sys.path.insert(0, str(ROOT))

FAILS: list[str] = []
OKS = 0


def ok(msg: str) -> None:
    global OKS
    OKS += 1
    print("OK", msg)


def fail(msg: str) -> None:
    FAILS.append(msg)
    print("FAIL", msg)


def check(cond: bool, msg: str) -> None:
    if cond:
        ok(msg)
    else:
        fail(msg)


def read(rel: str) -> str:
    return (ROOT / rel).read_text(encoding="utf-8")


def main() -> int:
    html = read("produtos/templates/produtos/atendimento_whatsapp.html")
    bot = read("produtos/templates/produtos/atendimento_whatsapp_bot.html")
    fab = read("produtos/templates/produtos/_agro_pdv_fab.html")
    dual = read("produtos/static/produtos/js/agro_dual_window.js")
    js = read("produtos/static/produtos/js/atendimento_whatsapp.js")

    # Chat Zap
    check("wa-voltar-pdv" not in html or "id=\"wa-voltar-pdv\"" not in html, "chat sem span Voltar PDV")
    check("_pdv_voltar_link" not in html, "chat sem include Voltar PDV")
    check("data-agro-hide-pdv" in html, "chat seta hide-pdv")
    check("#agro-pdv-fab-wrap" in html and "display: none" in html, "chat CSS esconde FAB")
    check("id=\"wa-btn-bot\"" in html, "botao Bot no chat")
    check("atendimento_whatsapp_bot" in html, "link Bot → tela bot")

    # Bot
    check("_pdv_voltar_link" not in bot, "bot sem Voltar PDV")
    check("wa-bot-voltar-chat" in bot, "bot tem voltar Chat")
    check("data-agro-hide-pdv" in bot, "bot seta hide-pdv")
    check("#agro-pdv-fab-wrap" in bot, "bot CSS esconde FAB")

    # FAB global
    check("function pathIsWhatsApp" in fab, "FAB pathIsWhatsApp")
    check("if (pathIsWhatsApp()) return true;" in fab, "FAB some no Zap")
    check("if (pathIsWhatsApp()) return;" in fab, "F1 ignorado no Zap")

    # Dual: Bot faz parte do Zap PC
    check(
        "if (p === '/atendimento-whatsapp/bot' || p.indexOf('/atendimento-whatsapp/bot/') === 0) return false;"
        not in dual,
        "dual nao exclui Bot do Zap PC",
    )
    check("isWhatsAppPcPath" in dual, "dual isWhatsAppPcPath")
    check("inclui Bot" in dual or "mesmo app" in dual, "comentario Bot no Zap PC")

    # Overlay ainda pode passar overlay=1; fora do overlay botHref limpo
    check("botHref: '/atendimento-whatsapp/bot/'" in js, "botHref limpo (sem overlay default)")

    print("---")
    print(f"OK={OKS} FAIL={len(FAILS)}")
    for f in FAILS:
        print(" ", f)
    return 1 if FAILS else 0


if __name__ == "__main__":
    raise SystemExit(main())
