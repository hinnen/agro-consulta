# -*- coding: utf-8 -*-
"""WA-PONTE-ULTRA-LEVE — prova: ponte nao engasga PDV/gestao."""
from __future__ import annotations

import os
import sys
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
    print("=== Path ===")
    util = read("produtos/atendimento_whatsapp_util.py")
    cfg = read("produtos/atendimento_whatsapp_bot_config.py")
    views = read("produtos/views_atendimento_whatsapp.py")
    html = read("produtos/templates/produtos/atendimento_whatsapp_bot.html")
    js_bot = read("produtos/static/produtos/js/atendimento_whatsapp_bot.js")
    bridge = read("whatsapp_atendimento/index.js")

    check("25.0" in util or "(agora_m - ultimo) < 25" in util, "heartbeat throttle ~25s")
    check('"midia_b64": ""' in util, "saida sem b64 no poll")
    check("_arquivo_b64(m) if tipo in" not in util, "nao embute b64 imagem/audio no poll")
    check("carregar_bot_leve" in cfg, "carregar_bot_leve")
    check('"poll_saida_seg": 10' in cfg, "default poll 10")
    check("max(8, min(30" in cfg, "salvar clamp 8-30")
    check("carregar_bot_leve" in views, "bridge usa cache leve")
    check("max(8, min(30" in views, "bridge poll clamp 8-30")
    check('min="8"' in html and 'max="30"' in html, "Bot HTML min8 max30")
    check("poll >= 8 && poll <= 30" in js_bot, "Bot JS clamp 8-30")
    check("let pollSegAtual = 10" in bridge, "ponte default poll 10")
    check("Math.max(8, Math.min(30" in bridge, "ponte clamp 8-30")


def prova_django() -> None:
    print(f"=== Django / PIN {PIN} ===")
    import django

    django.setup()
    from django.test import Client, RequestFactory

    from base.models import PerfilUsuario
    from produtos.atendimento_whatsapp_bot_config import (
        carregar_bot_leve,
        salvar_bot,
    )
    from produtos.atendimento_whatsapp_util import listar_saida_pendente, toque_heartbeat
    from produtos.caixa_util import operador_label_de_pin
    from produtos.views_atendimento_whatsapp import api_atendimento_whatsapp_bridge_saida

    ok_pin, label, err = operador_label_de_pin(PIN)
    if not ok_pin:
        fail(f"PIN {PIN}: {err}")
        return
    ok(f"PIN {PIN} -> {label}")

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

    # cache bot
    carregar_bot_leve._cache = {}  # type: ignore[attr-defined]
    d1 = carregar_bot_leve()
    d2 = carregar_bot_leve()
    check(isinstance(d1, dict) and d1 == d2, "cache bot leve")

    # salvar clamp
    limpo = salvar_bot({"poll_saida_seg": 2}, usuario="prova")
    check(int(limpo.get("poll_saida_seg") or 0) >= 8, f"salvar 2 -> {limpo.get('poll_saida_seg')} (>=8)")

    # bridge saida com token
    import json

    from produtos.atendimento_whatsapp_util import token_ponte
    from produtos.views_atendimento_whatsapp import api_atendimento_whatsapp_bridge_saida

    token = token_ponte()
    rf = RequestFactory()
    if token:
        req = rf.get("/api/atendimento-whatsapp/bridge/saida/")
        req.META["HTTP_X_AGRO_WA_TOKEN"] = token
        resp = api_atendimento_whatsapp_bridge_saida(req)
        check(resp.status_code == 200, f"bridge saida {resp.status_code}")
        if resp.status_code == 200:
            data = json.loads(resp.content.decode("utf-8"))
            check(int(data.get("poll_seg") or 0) >= 8, f"bridge poll_seg={data.get('poll_seg')} (>=8)")
            check("saida" in data and "pedidos" in data, "bridge JSON saida+pedidos")
            for item in data.get("saida") or []:
                check(item.get("midia_b64", "X") == "", "bridge item sem b64")
    else:
        ok("bridge token ausente — skip HTTP")

    # Bot page login
    perfil = (
        PerfilUsuario.objects.filter(senha_rapida=PIN, ativo=True).select_related("user").first()
    )
    if perfil:
        c = Client(HTTP_HOST="127.0.0.1")
        c.force_login(perfil.user)
        r = c.get("/atendimento-whatsapp/bot/")
        check(r.status_code == 200, f"Bot page {r.status_code}")
        body = r.content.decode("utf-8", errors="replace")
        check('min="8"' in body and 'max="30"' in body, "Bot page min8 max30")
    else:
        fail("perfil PIN nao achado")

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
