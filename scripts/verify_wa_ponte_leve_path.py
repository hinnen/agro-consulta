#!/usr/bin/env python
"""Prova path WA-PONTE-LEVE: poll no Bot + sync madrugada + bridge sem spam."""
from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

ok_n = 0
fail: list[str] = []


def check(nome: str, cond: bool) -> None:
    global ok_n
    if cond:
        ok_n += 1
        print("OK", nome)
    else:
        fail.append(nome)
        print("FAIL", nome)


def _read(rel: str) -> str:
    return (ROOT / rel).read_text(encoding="utf-8")


def main() -> int:
    cfg = _read("produtos/atendimento_whatsapp_bot_config.py")
    check("default poll_saida_seg", '"poll_saida_seg": 5' in cfg)
    check("default sync hora 00:00", '"sync_agenda_fotos_hora": "00:00"' in cfg)
    check("clamp poll 2-15", "max(2, min(15" in cfg and "poll_saida_seg" in cfg)

    html = _read("produtos/templates/produtos/atendimento_whatsapp_bot.html")
    check("campo poll_saida_seg", 'name="poll_saida_seg"' in html)
    check("campo sync hora", 'name="sync_agenda_fotos_hora"' in html)
    check("hint mensagem cliente na hora", "cliente" in html.lower() and "Checar saída" in html)

    bot_js = _read("produtos/static/produtos/js/atendimento_whatsapp_bot.js")
    check("bot.js poll_saida_seg", "poll_saida_seg" in bot_js)
    check("bot.js sync_agenda_fotos_hora", "sync_agenda_fotos_hora" in bot_js)

    views = _read("produtos/views_atendimento_whatsapp.py")
    check("saida poll_seg", "poll_seg" in views)
    check("saida sync_agenda_fotos_hora", "sync_agenda_fotos_hora" in views)
    check("fotos só com ?fotos=1", 'request.GET.get("fotos")' in views)
    check("fotos default vazio", "incluir_fotos" in views)

    bridge = _read("whatsapp_atendimento/index.js")
    check("bridge SYNC_FILE", "last_agenda_foto_sync.txt" in bridge)
    check("bridge ajustarPollSaida", "function ajustarPollSaida" in bridge)
    check("bridge rodarSyncAgendaFotos", "function rodarSyncAgendaFotos" in bridge)
    check("bridge pollQuerFotos", "pollQuerFotos" in bridge)
    check("bridge sem agenda no connect", "enviarAgenda(0).catch" not in bridge.split("connection === \"open\"")[1].split("connection === \"close\"")[0] or "WA-PONTE-LEVE" in bridge)
    check("bridge connect sem despejo", "não despeja agenda" in bridge or "WA-PONTE-LEVE: não despeja" in bridge)
    check("bridge agendarEnvioAgenda noop", "não manda agenda no meio do dia" in bridge or "1×/dia" in bridge)
    check("bridge sync completo", "completo: true" in bridge or "opts.completo" in bridge)
    check("bridge lê poll_seg", "j.poll_seg" in bridge)

    import django

    django.setup()
    from produtos.atendimento_whatsapp_bot_config import BOT_DEFAULT, carregar_bot, salvar_bot

    check("BOT_DEFAULT tem poll", int(BOT_DEFAULT.get("poll_saida_seg") or 0) == 5)
    check("BOT_DEFAULT sync 00:00", BOT_DEFAULT.get("sync_agenda_fotos_hora") == "00:00")

    b = carregar_bot()
    check("carregar tem poll", "poll_saida_seg" in b)
    check("carregar tem sync hora", "sync_agenda_fotos_hora" in b)

    poll_antes = b.get("poll_saida_seg")
    hora_antes = b.get("sync_agenda_fotos_hora")
    salvo = salvar_bot({**b, "poll_saida_seg": 99, "sync_agenda_fotos_hora": "25:99"})
    check("poll clamp max 15", int(salvo.get("poll_saida_seg") or 0) == 15)
    check("hora invalida vira 00:00", salvo.get("sync_agenda_fotos_hora") == "00:00")
    salvo2 = salvar_bot({**salvo, "poll_saida_seg": 3, "sync_agenda_fotos_hora": "00:00"})
    check("poll 3 ok", int(salvo2.get("poll_saida_seg") or 0) == 3)
    # restaura
    salvar_bot({**salvo2, "poll_saida_seg": poll_antes, "sync_agenda_fotos_hora": hora_antes or "00:00"})

    from django.contrib.auth import get_user_model
    from django.test import Client, override_settings

    User = get_user_model()
    u = User.objects.filter(is_superuser=True).first()
    with override_settings(ALLOWED_HOSTS=["*", "testserver", "127.0.0.1"]):
        cl = Client(HTTP_HOST="127.0.0.1")
        if u:
            cl.force_login(u)
        r = cl.get("/atendimento-whatsapp/bot/")
        check("bot page 200", r.status_code == 200)
        body = r.content.decode("utf-8", errors="replace")
        check("bot page poll field", "poll_saida_seg" in body)
        check("bot page sync field", "sync_agenda_fotos_hora" in body)
        api = cl.get("/api/atendimento-whatsapp/bot/")
        check("api bot 200", api.status_code == 200)
        j = api.json()
        check("api bot tem poll", "poll_saida_seg" in (j.get("bot") or {}))

    print("---")
    total = ok_n + len(fail)
    if fail:
        print(f"VERIFY_FAIL {ok_n}/{total}")
        for f in fail:
            print(" -", f)
        return 1
    print(f"VERIFY_OK {ok_n}/{total}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
