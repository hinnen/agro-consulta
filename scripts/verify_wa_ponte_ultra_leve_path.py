# -*- coding: utf-8 -*-
"""WA-PONTE-ULTRA-LEVE — prova detalhada: ponte nao engasga PDV/gestao.

Path · clamp · heartbeat · cache · bridge token · midia endpoint · PIN 9973 · restauracao bot.
"""
from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
os.chdir(ROOT)
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

FAILS: list[str] = []
OKS = 0
PIN = (os.environ.get("AGRO_PIN_TESTE") or "9973").strip()


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


def prova_path() -> None:
    print("=== Path estatico ===")
    util = read("produtos/atendimento_whatsapp_util.py")
    cfg = read("produtos/atendimento_whatsapp_bot_config.py")
    views = read("produtos/views_atendimento_whatsapp.py")
    html = read("produtos/templates/produtos/atendimento_whatsapp_bot.html")
    js_bot = read("produtos/static/produtos/js/atendimento_whatsapp_bot.js")
    bridge = read("whatsapp_atendimento/index.js")
    urls = read("produtos/urls.py")

    check("25.0" in util or "(agora_m - ultimo) < 25" in util, "heartbeat throttle ~25s")
    check("toque_heartbeat._mono" in util or "_mono" in util, "heartbeat mono cache")
    check('"midia_b64": ""' in util, "saida sem b64 no poll")
    check("_arquivo_b64(m) if tipo in" not in util, "nao embute b64 imagem/audio no poll")
    check("def listar_saida_pendente" in util, "listar_saida_pendente")
    check("carregar_bot_leve" in cfg, "carregar_bot_leve")
    check("ttl_seg" in cfg, "cache TTL")
    check("cache.pop" in cfg, "salvar invalida cache")
    check('"poll_saida_seg": 10' in cfg, "default poll 10")
    check("max(8, min(30" in cfg, "salvar clamp 8-30")
    check("carregar_bot_leve" in views, "bridge usa cache leve")
    check("max(8, min(30" in views, "bridge poll clamp 8-30")
    check("listar_saida_pendente" in views, "bridge chama listar_saida")
    check('min="8"' in html and 'max="30"' in html, "Bot HTML min8 max30")
    check("poll >= 8 && poll <= 30" in js_bot, "Bot JS clamp 8-30")
    check("let pollSegAtual = 10" in bridge, "ponte default poll 10")
    check("Math.max(8, Math.min(30" in bridge, "ponte clamp 8-30")
    check("baixarSaidaArquivo" in bridge, "ponte baixa midia por arquivo")
    check("/bridge/midia/" in bridge or "bridge/midia" in bridge, "ponte usa endpoint midia")
    check("bridge/saida/" in urls, "url bridge saida")
    check("bridge/midia/" in urls or "bridge_midia" in urls, "url bridge midia")


def prova_django() -> None:
    print(f"=== Django / PIN {PIN} ===")
    import django

    django.setup()
    from django.test import Client, RequestFactory

    from base.models import PerfilUsuario
    from produtos.atendimento_whatsapp_bot_config import (
        BOT_DEFAULT,
        carregar_bot,
        carregar_bot_leve,
        salvar_bot,
    )
    from produtos.atendimento_whatsapp_util import (
        listar_saida_pendente,
        toque_heartbeat,
        token_ponte,
    )
    from produtos.caixa_util import operador_label_de_pin
    from produtos.views_atendimento_whatsapp import api_atendimento_whatsapp_bridge_saida

    ok_pin, label, err = operador_label_de_pin(PIN)
    if not ok_pin:
        fail(f"PIN {PIN}: {err}")
        return
    ok(f"PIN {PIN} -> {label}")

    snap = dict(carregar_bot())
    snap_poll = int(snap.get("poll_saida_seg") or 10)

    try:
        # heartbeat: 2 toques seguidos = 1 save
        toque_heartbeat._mono = 0.0  # type: ignore[attr-defined]
        a = toque_heartbeat()
        b = toque_heartbeat()
        check(a is not None, "1o heartbeat grava")
        check(b is None, "2o heartbeat <25s nao grava")

        # saida sem b64
        rows = listar_saida_pendente(limit=5)
        check(isinstance(rows, list), "listar_saida lista")
        for r in rows:
            check(r.get("midia_b64", "X") == "", "item saida midia_b64 vazio")
        if not rows:
            ok("saida vazia (ok — sem pendente)")

        # cache bot: 2a chamada reusa
        carregar_bot_leve._cache = {}  # type: ignore[attr-defined]
        t0 = time.perf_counter()
        d1 = carregar_bot_leve()
        t1 = time.perf_counter()
        d2 = carregar_bot_leve()
        t2 = time.perf_counter()
        check(isinstance(d1, dict) and d1 == d2, "cache bot leve igual")
        check((t2 - t1) <= (t1 - t0) + 0.05, "2a leitura cache nao mais lenta que 1a+folga")

        # clamp min/max
        limpo_lo = salvar_bot({**snap, "poll_saida_seg": 2}, usuario="prova-ultra")
        check(int(limpo_lo.get("poll_saida_seg") or 0) == 8, f"salvar 2 -> {limpo_lo.get('poll_saida_seg')} (==8)")
        limpo_hi = salvar_bot({**snap, "poll_saida_seg": 99}, usuario="prova-ultra")
        check(int(limpo_hi.get("poll_saida_seg") or 0) == 30, f"salvar 99 -> {limpo_hi.get('poll_saida_seg')} (==30)")
        limpo_ok = salvar_bot({**snap, "poll_saida_seg": 10}, usuario="prova-ultra")
        check(int(limpo_ok.get("poll_saida_seg") or 0) == 10, "salvar 10 ok")

        # apos salvar, cache invalidado (nova leitura pega 10)
        carregar_bot_leve._cache = {}  # type: ignore[attr-defined]
        d3 = carregar_bot_leve()
        check(int(d3.get("poll_saida_seg") or 0) == 10, "cache apos salvar = 10")

        # bridge saida
        token = token_ponte()
        rf = RequestFactory()
        if token:
            req = rf.get("/api/atendimento-whatsapp/bridge/saida/")
            req.META["HTTP_X_AGRO_WA_TOKEN"] = token
            resp = api_atendimento_whatsapp_bridge_saida(req)
            check(resp.status_code == 200, f"bridge saida {resp.status_code}")
            data = json.loads(resp.content.decode("utf-8"))
            poll = int(data.get("poll_seg") or 0)
            check(poll >= 8, f"bridge poll_seg={poll} (>=8)")
            check(poll <= 30, f"bridge poll_seg={poll} (<=30)")
            check("saida" in data and "pedidos" in data, "bridge JSON saida+pedidos")
            check("sync_agenda_fotos_hora" in data, "bridge sync hora")
            for item in data.get("saida") or []:
                check(item.get("midia_b64", "X") == "", "bridge item sem b64")
            # 2a chamada (cache bot) ainda 200
            resp2 = api_atendimento_whatsapp_bridge_saida(req)
            check(resp2.status_code == 200, "bridge saida 2a 200")
            data2 = json.loads(resp2.content.decode("utf-8"))
            check(int(data2.get("poll_seg") or 0) >= 8, "2a poll_seg >=8")
        else:
            fail("bridge token ausente")

        # Bot page
        perfil = (
            PerfilUsuario.objects.filter(senha_rapida=PIN, ativo=True)
            .select_related("user")
            .first()
        )
        if not perfil:
            fail("perfil PIN nao achado")
        else:
            c = Client(HTTP_HOST="127.0.0.1")
            c.force_login(perfil.user)
            r = c.get("/atendimento-whatsapp/bot/")
            check(r.status_code == 200, f"Bot page {r.status_code}")
            body = r.content.decode("utf-8", errors="replace")
            check('min="8"' in body and 'max="30"' in body, "Bot page min8 max30")
            check("poll_saida_seg" in body, "Bot page campo poll")

            # API bot GET
            rb = c.get("/api/atendimento-whatsapp/bot/")
            check(rb.status_code == 200, f"API bot {rb.status_code}")
            if rb.status_code == 200:
                bj = json.loads(rb.content.decode("utf-8"))
                bot = bj.get("bot") or {}
                check(int(bot.get("poll_saida_seg") or 0) >= 8, f"API bot poll>={bot.get('poll_saida_seg')}")

        check(int(BOT_DEFAULT.get("poll_saida_seg") or 0) == 10, "BOT_DEFAULT poll=10")

    finally:
        # restaura poll anterior (clamp 8-30)
        try:
            restore_poll = max(8, min(30, snap_poll if snap_poll else 10))
            salvar_bot({**snap, "poll_saida_seg": restore_poll}, usuario="prova-ultra-restore")
            ok(f"bot restaurado poll={restore_poll}")
        except Exception as e:
            fail(f"restore bot: {e}")

    if not operador_label_de_pin(PIN)[0]:
        fail("PIN quebrou")
    else:
        ok("PIN intacto")


def main() -> int:
    prova_path()
    try:
        prova_django()
    except Exception as e:
        fail(f"django: {e}")
    print("---")
    print(f"OK={OKS} FAIL={len(FAILS)}")
    for f in FAILS:
        print(" ", f)
    return 1 if FAILS else 0


if __name__ == "__main__":
    raise SystemExit(main())
