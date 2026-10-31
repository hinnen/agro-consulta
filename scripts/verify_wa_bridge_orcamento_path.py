#!/usr/bin/env python3
"""Prova a rota que recebe o orçamento do botão Loja do PDV."""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
os.chdir(ROOT)
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

import django

django.setup()

from django.test import Client, override_settings

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
    print("=== WA-BRIDGE-ORCAMENTO ===\n")
    urls = (ROOT / "produtos/urls.py").read_text(encoding="utf-8")
    check("url bridge/orcamento", "api/atendimento-whatsapp/bridge/orcamento/" in urls)
    settings_txt = (ROOT / "config/settings.py").read_text(encoding="utf-8")
    check(
        "origem publica do zap vazia por padrao",
        'config("AGRO_WA_PUBLIC_ORIGIN", default="")' in settings_txt
        or "default=\"\"" in settings_txt,
    )

    from produtos import views_atendimento_whatsapp as vw

    chamadas = []

    def fake_enviar(*, conversa_id, texto, autor, **kwargs):
        chamadas.append((conversa_id, texto, autor))
        return object(), ""

    vw.enviar_loja = fake_enviar

    def cfg_off():
        return {"feat_orcamento_zap": False}

    def cfg_on():
        return {"feat_orcamento_zap": True}

    from produtos import atendimento_whatsapp_recursos as rec

    rec.recurso_on = lambda cfg, key: bool(cfg.get(key))

    c = Client(HTTP_HOST="agro-consulta-wa.onrender.com")
    with override_settings(
        AGRO_WA_BRIDGE_TOKEN="segredo-teste",
        AGRO_WA_PUBLIC_ORIGIN="",
        ALLOWED_HOSTS=["agro-consulta-wa.onrender.com", "testserver"],
    ):
        r = c.post(
            "/api/atendimento-whatsapp/bridge/orcamento/",
            data=json.dumps({"texto": "oi"}),
            content_type="application/json",
        )
        check("sem token 403", r.status_code == 403)

        r = c.post(
            "/api/atendimento-whatsapp/bridge/orcamento/",
            data=json.dumps({"texto": "oi"}),
            content_type="application/json",
            HTTP_X_AGRO_WA_TOKEN="errado",
        )
        check("token errado 403", r.status_code == 403)

        vw.carregar_bot = cfg_off
        # a view importa carregar_bot de novo por dentro
        import produtos.atendimento_whatsapp_bot_config as botcfg

        botcfg.carregar_bot = cfg_off
        r = c.post(
            "/api/atendimento-whatsapp/bridge/orcamento/",
            data=json.dumps({"texto": "R$ 10", "conversa_id": 4, "autor": "Renan"}),
            content_type="application/json",
            HTTP_X_AGRO_WA_TOKEN="segredo-teste",
        )
        check("flag off 400", r.status_code == 400 and "Orçamento no Zap" in r.json().get("erro", ""))
        check("flag off nao enfileira", chamadas == [])

        botcfg.carregar_bot = cfg_on
        r = c.post(
            "/api/atendimento-whatsapp/bridge/orcamento/",
            data=json.dumps({"texto": "   ", "conversa_id": 4}),
            content_type="application/json",
            HTTP_X_AGRO_WA_TOKEN="segredo-teste",
        )
        check("texto vazio 400", r.status_code == 400)

        r = c.post(
            "/api/atendimento-whatsapp/bridge/orcamento/",
            data=json.dumps({"texto": "2 sc milho", "conversa_id": 9, "autor": "Caixa"}),
            content_type="application/json",
            HTTP_X_AGRO_WA_TOKEN="segredo-teste",
        )
        body = r.json()
        check("envio ok", r.status_code == 200 and body.get("ok") is True and body.get("conversa_id") == 9)
        check("enfileira texto e autor", chamadas == [(9, "2 sc milho", "Caixa")])

        r = c.get("/api/atendimento-whatsapp/bridge/orcamento/")
        check("get recusado", r.status_code == 405)

    print(f"\n{OK} ok, {FAIL} falha")
    return 0 if FAIL == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
