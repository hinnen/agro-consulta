# -*- coding: utf-8 -*-
"""WA-BOT-SALVAR — prova: Salvar nao trava com poll<3 / HTML5."""
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
    html = read("produtos/templates/produtos/atendimento_whatsapp_bot.html")
    js = read("produtos/static/produtos/js/atendimento_whatsapp_bot.js")
    views = read("produtos/views_atendimento_whatsapp.py")

    check("novalidate" in html, "form novalidate")
    check('id="wa-bot-save"' in html and 'type="button"' in html, "Salvar type=button")
    check("function dispararSalvar" in js, "JS dispararSalvar")
    check("credentials: 'same-origin'" in js or 'credentials: "same-origin"' in js, "fetch credentials")
    check("poll antigo" in js or "pollEl.value = '5'" in js, "clamp poll no preencher")
    check("btnSave.type = 'button'" in js, "JS forca type=button")
    check("Handlers primeiro" in js, "handlers antes do montar UI")
    check("Nao salvou: " in views.replace("ã", "a") or "Não salvou:" in views, "API try/except salvar")
    check('min="3"' in html and "poll_saida_seg" in html, "poll ainda tem min=3 (ok com novalidate)")

    # logica: salvar com poll=2 nao quebra backend
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
    import django

    django.setup()
    from produtos.atendimento_whatsapp_bot_config import carregar_bot, salvar_bot
    from produtos.caixa_util import rotulo_operador_pin

    pin = (os.environ.get("AGRO_PIN_TESTE") or "9973").strip()
    check(bool(rotulo_operador_pin(pin)), f"PIN {pin}")

    bot = carregar_bot()
    bot["poll_saida_seg"] = 2
    limpo = salvar_bot(bot, usuario="verify-bot-salvar")
    check(int(limpo.get("poll_saida_seg") or 0) >= 3, f"backend sobe poll 2-> {limpo.get('poll_saida_seg')}")
    check("horario_por_dia" in limpo and "6" in limpo["horario_por_dia"], "horario_por_dia apos salvar")

    print("---")
    print(f"OK={OKS} FAIL={len(FAILS)}")
    for f in FAILS:
        print(" ", f)
    return 1 if FAILS else 0


if __name__ == "__main__":
    raise SystemExit(main())
