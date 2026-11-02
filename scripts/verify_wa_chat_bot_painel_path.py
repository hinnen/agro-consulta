#!/usr/bin/env python3
"""Prova o pacote local: Bot na mesma tela e clique da conversa na frente da fila."""
from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OK = 0
FAIL = 0


def check(label: str, cond: bool) -> None:
    global OK, FAIL
    if cond:
        OK += 1
        print(f"  OK  {label}")
    else:
        FAIL += 1
        print(f" FAIL {label}")


def main() -> int:
    chat = (ROOT / "produtos/templates/produtos/atendimento_whatsapp.html").read_text(encoding="utf-8")
    bot = (ROOT / "produtos/templates/produtos/atendimento_whatsapp_bot.html").read_text(encoding="utf-8")
    painel = (ROOT / "produtos/templates/produtos/_wa_bot_painel.html").read_text(encoding="utf-8")
    js = (ROOT / "produtos/static/produtos/js/atendimento_whatsapp.js").read_text(encoding="utf-8")

    check("painel tem o formulario", 'id="wa-bot-form"' in painel)
    check("chat inclui o painel", "_wa_bot_painel.html" in chat)
    check("pagina do bot inclui o mesmo painel", "_wa_bot_painel.html" in bot)
    check(
        "chat nao tem script src do bot no load",
        "src=\"{% static 'produtos/js/atendimento_whatsapp_bot.js'" not in chat,
    )
    check("botao guarda o endereco do js", "data-bot-js=" in chat)
    check("chat nao inclui o pin da loja", "_screensaver_pin.html" not in chat)
    check("clique no bot nao navega", "preventDefault" in js and "mostrarPainelBot" in js)
    check("js do bot so no primeiro clique", "garantirBotJs" in js)
    check("poll para enquanto a conversa abre", "if (abrindoConversa) return;" in js)
    check("marcar lida fica depois das mensagens", js.find("marcar-lida") > js.find("mensagens/?conversa_id="))
    check("so uma chamada de marcar lida na abertura", js.count("marcar-lida/") == 1)
    check(
        "resolvidas pede a lista arquivada",
        "chaveLista" in js and "arquivadas" in js,
    )
    check("lista fica na memoria da aba", "cacheListas" in js)
    check("conversa aberta fica na memoria", "guardarMsgs" in js)
    check("no maximo 8 conversas guardadas", "cacheMsgsOrdem.length > 8" in js)
    busca = js[js.find("function buscarTopo") : js.find("function buscarTopo") + 1800]
    check(
        "busca mostra o nome fora da coluna da foto",
        "wa-item-body" in busca and "wa-av-ini" in busca,
    )
    util = (ROOT / "produtos/atendimento_whatsapp_util.py").read_text(encoding="utf-8")
    check(
        "mensagem nao varre o cadastro inteiro",
        "cliente_agro_por_whatsapp_flex" not in util and "_cadastro_ficha_por_telefone" in util,
    )
    print(f"\n{OK} ok, {FAIL} falha")
    return 0 if FAIL == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
