# -*- coding: utf-8 -*-
"""
Prova path — WA-ARQUIVO + WA-SAUDACAO-RICH

  python scripts/verify_wa_arquivo_saudacao_path.py
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

fails: list[str] = []
oks: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    if cond:
        oks.append(name)
        print(f"  OK  {name}" + (f" — {detail}" if detail else ""))
    else:
        fails.append(name)
        print(f"  FAIL {name}" + (f" — {detail}" if detail else ""))


def main() -> int:
    print("=== WA-ARQUIVO / WA-SAUDACAO-RICH path ===")

    mig = ROOT / "produtos" / "migrations" / "0126_whatsapp_conversa_arquivada.py"
    check("migrate 0126", mig.is_file())

    models = (ROOT / "produtos" / "models.py").read_text(encoding="utf-8")
    check("model arquivada", "arquivada = models.BooleanField" in models)
    check("model arquivada_em", "arquivada_em = models.DateTimeField" in models)
    check("model arquivada_por", "arquivada_por = models.CharField" in models)

    util = (ROOT / "produtos" / "atendimento_whatsapp_util.py").read_text(encoding="utf-8")
    check("arquivar_conversa", "def arquivar_conversa" in util)
    check("reabrir_conversa", "def reabrir_conversa" in util)
    check("desarquiva msg", "_desarquivar_por_msg_cliente" in util)
    check("listar arquivadas", 'loja_n in ("arquivadas"' in util or '"arquivadas"' in util)
    check("saudacao codes hora/loja", '"{hora}"' in util and '"{loja}"' in util)
    check("saudacao_depois_menu", "saudacao_depois_menu" in util)

    cfg = (ROOT / "produtos" / "atendimento_whatsapp_bot_config.py").read_text(encoding="utf-8")
    check("default arquivo OFF", '"arquivo_auto_ligado": False' in cfg)
    check("saudacao keys", "saudacao_so_em_horario" in cfg and "saudacao_atraso_seg" in cfg)

    urls = (ROOT / "produtos" / "urls.py").read_text(encoding="utf-8")
    check("url reabrir", "api/atendimento-whatsapp/reabrir/" in urls)

    js = (ROOT / "produtos" / "static" / "produtos" / "js" / "atendimento_whatsapp.js").read_text(
        encoding="utf-8"
    )
    check("js reabrir", "wa-reabrir" in js and "/reabrir/" in js)
    check("js arquivadas badge", "arquivadas" in js)

    bot_html = (ROOT / "produtos" / "templates" / "produtos" / "atendimento_whatsapp_bot.html").read_text(
        encoding="utf-8"
    )
    check("aba Saudação", 'data-panel="saudacao"' in bot_html)
    check("aba Arquivo", 'data-panel="arquivo"' in bot_html)
    check("menu sem boas-vindas cru", bot_html.count("msg_boas_vindas") == 1)

    web = (ROOT / "produtos" / "templates" / "produtos" / "atendimento_whatsapp.html").read_text(
        encoding="utf-8"
    )
    cel = (ROOT / "produtos" / "templates" / "produtos" / "atendimento_whatsapp_celular.html").read_text(
        encoding="utf-8"
    )
    check("tab Resolvidas web", 'data-loja="arquivadas"' in web)
    check("tab Resolvidas cel", 'data-loja="arquivadas"' in cel)
    head = (ROOT / "produtos" / "templates" / "produtos" / "_wa_chat_head.html").read_text(
        encoding="utf-8"
    )
    check("botão Reabrir", 'id="wa-reabrir"' in head)

    print(f"\n{len(oks)} ok · {len(fails)} fail")
    return 1 if fails else 0


if __name__ == "__main__":
    raise SystemExit(main())
