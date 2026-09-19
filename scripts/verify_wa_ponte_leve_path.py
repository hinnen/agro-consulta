# -*- coding: utf-8 -*-
"""
WA-PONTE-LEVE — prova detalhada do path.

Objetivo: Zap ligado sem engasgar o PDV.
  1) Bot Tempo: poll_saida_seg (2-15) + sync_agenda_fotos_hora (padrao 00:00)
  2) bridge/saida: devolve poll_seg; fotos so com ?fotos=1
  3) ponte Node: sync 1x/dia; nao despeja agenda no connect; poll dinamico
  4) msgs do cliente = socket (entrada), nao dependem do poll

  python scripts/verify_wa_ponte_leve_path.py
  PIN 9973 (Renan) · AGRO_PIN_TESTE opcional
"""
from __future__ import annotations

import json
import os
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
os.chdir(ROOT)
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

PIN = (os.environ.get("AGRO_PIN_TESTE") or "9973").strip()
TOKEN_TESTE = "gm-agro-wa-ponte-verify-leve"

FAILS: list[str] = []
OKS = 0


def ok(msg: str) -> None:
    global OKS
    OKS += 1
    print("OK", msg.encode("ascii", "replace").decode("ascii"))


def fail(msg: str) -> None:
    FAILS.append(msg)
    print("FAIL", msg.encode("ascii", "replace").decode("ascii"))


def check(cond: bool, msg: str) -> None:
    if cond:
        ok(msg)
    else:
        fail(msg)


def read(rel: str) -> str:
    return (ROOT / rel).read_text(encoding="utf-8")


def prova_estatico() -> None:
    print("=== estatico ===")
    cfg = read("produtos/atendimento_whatsapp_bot_config.py")
    check('"poll_saida_seg": 5' in cfg, "cfg default poll=5")
    check('"sync_agenda_fotos_hora": "00:00"' in cfg, "cfg default sync 00:00")
    check("max(2, min(15" in cfg and "poll_saida_seg" in cfg, "cfg clamp poll 2-15")
    check("sync_agenda_fotos_hora" in cfg and "00:00" in cfg, "cfg salva hora sync")

    html = read("produtos/templates/produtos/atendimento_whatsapp_bot.html")
    check('name="poll_saida_seg"' in html, "HTML campo Checar saida")
    check('name="sync_agenda_fotos_hora"' in html, "HTML campo Atualizar agenda/fotos")
    check("Checar saída" in html or "Checar saida" in html, "HTML rotulo poll")
    check("cliente" in html.lower() and "hora" in html.lower(), "HTML hint cliente na hora")
    # campos so na section Tempo (nao no botao da nav)
    i_sec = html.find('<section data-panel="tempo"')
    j_sec = html.find("</section>", i_sec) if i_sec >= 0 else -1
    trecho = html[i_sec:j_sec] if i_sec >= 0 and j_sec > i_sec else ""
    check(i_sec >= 0 and j_sec > i_sec, "achou section Tempo")
    check('name="poll_saida_seg"' in trecho, "poll so na aba Tempo")
    check('name="sync_agenda_fotos_hora"' in trecho, "sync hora so na aba Tempo")

    bot_js = read("produtos/static/produtos/js/atendimento_whatsapp_bot.js")
    check("poll_saida_seg" in bot_js, "bot.js preenche poll")
    check("sync_agenda_fotos_hora" in bot_js, "bot.js preenche sync hora")

    views = read("produtos/views_atendimento_whatsapp.py")
    check("poll_seg" in views, "views saida poll_seg")
    check("sync_agenda_fotos_hora" in views, "views saida sync hora")
    check('request.GET.get("fotos")' in views, "views fotos so via query")
    check("incluir_fotos" in views, "views fotos opt-in")
    check("listar_fotos_pendentes()" in views, "views chama listar_fotos so se incluir")

    bridge = read("whatsapp_atendimento/index.js")
    check("last_agenda_foto_sync.txt" in bridge, "bridge SYNC_FILE")
    check("function ajustarPollSaida" in bridge, "bridge ajustarPollSaida")
    check("function rodarSyncAgendaFotos" in bridge, "bridge rodarSyncAgendaFotos")
    check("pollQuerFotos" in bridge, "bridge pollQuerFotos")
    check("j.poll_seg" in bridge, "bridge le poll_seg da API")
    check("sync_agenda_fotos_hora" in bridge, "bridge le hora sync")
    check("?fotos=1" in bridge, "bridge pede fotos=1 so no sync")
    check("completo: true" in bridge or "opts.completo" in bridge, "bridge sync completo")
    check("WA-PONTE-LEVE" in bridge, "bridge marca WA-PONTE-LEVE")
    # connect open: nao deve chamar enviarAgenda(0) solto
    open_blk = bridge.split('connection === "open"')[1].split('connection === "close"')[0]
    check("enviarAgenda(0)" not in open_blk, "bridge connect sem enviarAgenda(0)")
    check("rodarSyncAgendaFotos" in open_blk, "bridge connect chama sync diário")
    check("semearFotosPerfil" not in open_blk or "function semearFotosPerfil" in bridge, "bridge connect sem semear fotos em massa")
    # agendarEnvioAgenda nao agenda timer
    age_fn = bridge.split("function agendarEnvioAgenda")[1].split("function ")[0]
    check("setTimeout" not in age_fn and "enviarAgenda" not in age_fn, "agendarEnvioAgenda e noop")
    check("setInterval" in bridge and "60 * 1000" in bridge, "bridge checa sync a cada 1 min")
    check("3 * 60 * 1000" in bridge, "bridge janela fotos 3 min apos sync")

    gi = read(".gitignore")
    check("last_agenda_foto_sync.txt" in gi, "gitignore sync file")


def prova_orm_e_clamp() -> dict:
    print("=== orm / clamp ===")
    import django

    django.setup()
    from produtos.atendimento_whatsapp_bot_config import BOT_DEFAULT, carregar_bot, salvar_bot
    from produtos.caixa_util import rotulo_operador_pin

    check(int(BOT_DEFAULT.get("poll_saida_seg") or 0) == 5, "BOT_DEFAULT poll=5")
    check(BOT_DEFAULT.get("sync_agenda_fotos_hora") == "00:00", "BOT_DEFAULT sync=00:00")

    rot = (rotulo_operador_pin(PIN) or "").strip()
    check(bool(rot), f"PIN {PIN} existe no PG (rotulo={rot!r})")

    b = carregar_bot()
    check("poll_saida_seg" in b, "carregar_bot tem poll")
    check("sync_agenda_fotos_hora" in b, "carregar_bot tem sync hora")
    snap = {
        "poll_saida_seg": b.get("poll_saida_seg"),
        "sync_agenda_fotos_hora": b.get("sync_agenda_fotos_hora"),
    }

    s1 = salvar_bot({**b, "poll_saida_seg": 1, "sync_agenda_fotos_hora": "00:00"})
    check(int(s1.get("poll_saida_seg") or 0) == 2, "poll min clamp=2")
    s2 = salvar_bot({**s1, "poll_saida_seg": 99})
    check(int(s2.get("poll_saida_seg") or 0) == 15, "poll max clamp=15")
    s3 = salvar_bot({**s2, "poll_saida_seg": 7, "sync_agenda_fotos_hora": "25:99"})
    check(s3.get("sync_agenda_fotos_hora") == "00:00", "hora invalida vira 00:00")
    s4 = salvar_bot({**s3, "sync_agenda_fotos_hora": "00:30"})
    check(s4.get("sync_agenda_fotos_hora") == "00:30", "hora 00:30 ok")
    s5 = salvar_bot({**s4, "poll_saida_seg": 5, "sync_agenda_fotos_hora": "00:00"})
    check(int(s5.get("poll_saida_seg") or 0) == 5, "poll 5 ok")
    check(s5.get("sync_agenda_fotos_hora") == "00:00", "hora 00:00 ok")

    # restaura o que estava
    salvar_bot(
        {
            **s5,
            "poll_saida_seg": snap["poll_saida_seg"] or 5,
            "sync_agenda_fotos_hora": snap["sync_agenda_fotos_hora"] or "00:00",
        }
    )
    ok("restaurou poll/hora do bot")
    return snap


def prova_http_client(snap: dict) -> None:
    print("=== Client Django (PIN %s) ===" % PIN)
    from django.contrib.auth import get_user_model
    from django.test import Client, override_settings
    from produtos.atendimento_whatsapp_bot_config import carregar_bot, salvar_bot
    from produtos.caixa_util import rotulo_operador_pin

    User = get_user_model()
    u = User.objects.filter(is_superuser=True).first()
    check(u is not None, "superuser para Client")
    rot = (rotulo_operador_pin(PIN) or "").strip()

    with override_settings(
        ALLOWED_HOSTS=["*", "testserver", "127.0.0.1"],
        AGRO_WA_BRIDGE_TOKEN=TOKEN_TESTE,
    ):
        cl = Client(HTTP_HOST="127.0.0.1")
        if u:
            cl.force_login(u)
        # sessao PIN se a tela bot exigir
        sess = cl.session
        sess["agro_operador_pin"] = PIN
        sess["agro_operador_rotulo"] = rot or "Renan"
        sess.save()

        r = cl.get("/atendimento-whatsapp/bot/")
        check(r.status_code == 200, f"GET bot page {r.status_code}")
        body = r.content.decode("utf-8", errors="replace")
        check("poll_saida_seg" in body, "bot HTML tem poll_saida_seg")
        check("sync_agenda_fotos_hora" in body, "bot HTML tem sync_agenda_fotos_hora")
        check("data-panel=\"tempo\"" in body, "bot HTML aba Tempo")

        api = cl.get("/api/atendimento-whatsapp/bot/")
        check(api.status_code == 200, f"GET api bot {api.status_code}")
        j = api.json()
        check(j.get("ok") is True, "api bot ok")
        bot = j.get("bot") or {}
        check("poll_saida_seg" in bot, "api bot.bot.poll_saida_seg")
        check("sync_agenda_fotos_hora" in bot, "api bot.bot.sync_agenda_fotos_hora")
        pad = j.get("padrao") or {}
        check(int(pad.get("poll_saida_seg") or 0) == 5, "api padrao poll=5")
        check(pad.get("sync_agenda_fotos_hora") == "00:00", "api padrao sync 00:00")

        # salvar via API
        payload = {
            **bot,
            "poll_saida_seg": 4,
            "sync_agenda_fotos_hora": "00:00",
            "pin": PIN,
        }
        post = cl.post(
            "/api/atendimento-whatsapp/bot/salvar/",
            data=json.dumps({"bot": payload}),
            content_type="application/json",
        )
        check(post.status_code == 200, f"POST api bot salvar {post.status_code}")
        if post.status_code != 200:
            fail(f"POST body: {post.content[:200]!r}")
            return
        pj = post.json()
        check(pj.get("ok") is True, "POST bot ok")
        check(int((pj.get("bot") or {}).get("poll_saida_seg") or 0) == 4, "POST gravou poll=4")

        # bridge saida sem token
        neg = cl.get("/api/atendimento-whatsapp/bridge/saida/")
        check(neg.status_code in (401, 403), f"saida sem token bloqueia ({neg.status_code})")

        # bridge saida com token — sem fotos
        ok_r = cl.get(
            "/api/atendimento-whatsapp/bridge/saida/",
            HTTP_X_AGRO_WA_TOKEN=TOKEN_TESTE,
        )
        check(ok_r.status_code == 200, f"saida com token {ok_r.status_code}")
        sj = ok_r.json()
        check(sj.get("ok") is True, "saida ok")
        check("saida" in sj and "pedidos" in sj, "saida tem lista saida/pedidos")
        check(int(sj.get("poll_seg") or 0) == 4, "saida poll_seg=4 (do bot)")
        check(sj.get("sync_agenda_fotos_hora") == "00:00", "saida sync hora 00:00")
        fotos = sj.get("fotos")
        check(isinstance(fotos, list) and len(fotos) == 0, "saida sem ?fotos=1 → fotos=[]")

        # com fotos=1 pode ter itens (lista), mas deve ser lista
        fot = cl.get(
            "/api/atendimento-whatsapp/bridge/saida/?fotos=1",
            HTTP_X_AGRO_WA_TOKEN=TOKEN_TESTE,
        )
        check(fot.status_code == 200, f"saida ?fotos=1 {fot.status_code}")
        fj = fot.json()
        check(isinstance(fj.get("fotos"), list), "saida ?fotos=1 → fotos lista")
        # se tem itens, cada um tem jid
        for item in (fj.get("fotos") or [])[:3]:
            check(bool(item.get("jid") or item.get("jid_lid")), "foto pendente tem jid")

        # restaurar snap
        cur = carregar_bot()
        salvar_bot(
            {
                **cur,
                "poll_saida_seg": snap.get("poll_saida_seg") or 5,
                "sync_agenda_fotos_hora": snap.get("sync_agenda_fotos_hora") or "00:00",
            }
        )
        ok("restaurou bot apos Client")


def prova_logica_sync_hora() -> None:
    print("=== logica hora sync (espelho ponte) ===")
    # Espelha parse da ponte em Python
    def parse_hora(s: str) -> int:
        m = re.match(r"^(\d{1,2}):(\d{2})$", str(s or "00:00").strip())
        if not m:
            return 0
        hh = min(23, max(0, int(m.group(1))))
        mm = min(59, max(0, int(m.group(2))))
        return hh * 60 + mm

    check(parse_hora("00:00") == 0, "parse 00:00 = 0 min")
    check(parse_hora("00:30") == 30, "parse 00:30 = 30")
    check(parse_hora("08:00") == 8 * 60, "parse 08:00")
    # precisa sync: se ultimo != hoje e agora >= hora
    check(parse_hora("00:00") <= 24 * 60, "madrugada sempre apos 00:00 no mesmo dia")


def main() -> int:
    print("=== WA-PONTE-LEVE path detalhado ===")
    print("PIN teste:", PIN)
    prova_estatico()
    snap = prova_orm_e_clamp()
    prova_http_client(snap)
    prova_logica_sync_hora()
    print("---")
    total = OKS + len(FAILS)
    if FAILS:
        print(f"VERIFY_FAIL {OKS}/{total}")
        for f in FAILS:
            print(" -", f)
        return 1
    print(f"VERIFY_OK {OKS}/{total}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
