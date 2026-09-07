# -*- coding: utf-8 -*-
"""
WA-ENVIO-FROMME — prova detalhada do path.

Problema: tela só recebia. Causas:
  1) ponte descartava notify fromMe (eco do celular)
  2) UI de envio frágil (só submit do form)

  python scripts/verify_wa_envio_fromme_path.py
  PIN 9973 (Renan) · AGRO_PIN_TESTE opcional
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
os.chdir(ROOT)
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

PIN = (os.environ.get("AGRO_PIN_TESTE") or "9973").strip()

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
    bridge = read("whatsapp_atendimento/index.js")
    # Nao pode mais pular fromMe no notify ao vivo
    check("Eco celular:" in bridge, "bridge log Eco celular")
    check('fromMe ? "Eco celular:"' in bridge or "Eco celular:" in bridge, "bridge distingue eco")
    # Trecho upsert: notify trata fromMe
    i = bridge.find('sock.ev.on("messages.upsert"')
    check(i > 0, "achou messages.upsert")
    trecho = bridge[i : i + 2200]
    check("type === \"notify\"" in trecho or "type === 'notify'" in trecho, "upsert trata notify")
    check("&& !(m.key && m.key.fromMe)" not in trecho, "notify NAO filtra fromMe fora")
    check("enviarEntrada(m, { historico: false })" in trecho, "notify chama enviarEntrada")
    check("Saida pendente:" in bridge, "bridge log saida pendente")
    check("Enviado ok:" in bridge, "bridge log enviado ok")
    check("function destinosEnvio" in bridge, "bridge destinosEnvio lid+phone")
    check("envio falhou ->" in bridge, "bridge log falha destino")

    js = read("produtos/static/produtos/js/atendimento_whatsapp.js")
    check("function dispararTextoComposer" in js, "JS dispararTextoComposer")
    check("credentials = 'same-origin'" in js or 'credentials = "same-origin"' in js, "fetch credentials")
    check("Sessão/CSRF" in js or "Sessao/CSRF" in js, "erro CSRF legivel")
    check("sendBtn.addEventListener('click'" in js, "click no botao enviar")
    check("keydown" in js and "Enter" in js and "dispararTextoComposer" in js, "Enter no input envia")

    cfg = read("produtos/atendimento_whatsapp_bot_config.py")
    check("max(3, min(15" in cfg, "poll min 3")

    views = read("produtos/views_atendimento_whatsapp.py")
    check("max(3, min(15" in views, "views poll min 3")


def prova_orm_enviar() -> None:
    print("=== orm / Client enviar (PIN %s) ===" % PIN)
    import django

    django.setup()
    from django.contrib.auth import get_user_model
    from django.test import Client, override_settings
    from django.utils import timezone

    from produtos.atendimento_whatsapp_util import enviar_loja, listar_saida_pendente, marcar_enviadas
    from produtos.caixa_util import rotulo_operador_pin
    from produtos.models import WhatsAppConversaAgro, WhatsAppMensagemAgro

    rot = (rotulo_operador_pin(PIN) or "").strip()
    check(bool(rot), f"PIN {PIN} rotulo={rot!r}")

    jid = "5513999990001@s.whatsapp.net"
    conv, _created = WhatsAppConversaAgro.objects.get_or_create(
        jid=jid,
        defaults={
            "telefone": "5513999990001",
            "nome": "Verify Envio",
            "loja": WhatsAppConversaAgro.LOJA_CENTRO,
            "ultima_em": timezone.now(),
        },
    )
    if conv.loja != WhatsAppConversaAgro.LOJA_CENTRO:
        conv.loja = WhatsAppConversaAgro.LOJA_CENTRO
        conv.save(update_fields=["loja"])

    texto = f"VERIFY_ENVIO_{timezone.now().strftime('%H%M%S')}"
    m, err = enviar_loja(conversa_id=int(conv.pk), texto=texto, autor=rot or "Verify")
    check(err == "" and m is not None, f"enviar_loja ok err={err!r}")
    check(bool(m and m.pendente_envio), "msg pendente_envio")
    check((m.direcao if m else "") == "out", "direcao out")

    pend = listar_saida_pendente(limit=50)
    ids = [int(x.get("id") or 0) for x in pend]
    check(int(m.pk) in ids, "listar_saida_pendente ve a msg")
    item = next((x for x in pend if int(x.get("id") or 0) == int(m.pk)), {})
    check(bool(item.get("jid")), "saida tem jid")

    n = marcar_enviadas([int(m.pk)], wa_id="VERIFYWA1")
    check(n == 1, "marcar_enviadas 1")
    m.refresh_from_db()
    check(not m.pendente_envio and m.wa_id == "VERIFYWA1", "msg marcada enviada")

    User = get_user_model()
    u = User.objects.filter(is_superuser=True).first()
    check(u is not None, "superuser Client")
    with override_settings(ALLOWED_HOSTS=["*", "testserver", "127.0.0.1"]):
        cl = Client(HTTP_HOST="127.0.0.1")
        if u:
            cl.force_login(u)
        sess = cl.session
        sess["agro_operador_pin"] = PIN
        sess["agro_operador_rotulo"] = rot or "Renan"
        sess.save()

        texto2 = f"VERIFY_HTTP_{timezone.now().strftime('%H%M%S')}"
        post = cl.post(
            "/api/atendimento-whatsapp/enviar/",
            data=json.dumps({"conversa_id": int(conv.pk), "texto": texto2}),
            content_type="application/json",
        )
        check(post.status_code == 200, f"POST enviar {post.status_code}")
        if post.status_code == 200:
            pj = post.json()
            check(pj.get("ok") is True, "POST enviar ok")
            mid = int(((pj.get("mensagem") or {}).get("id") or 0))
            check(mid > 0, "POST devolve id")
            if mid:
                WhatsAppMensagemAgro.objects.filter(pk=mid).update(
                    pendente_envio=False, wa_id=f"HTTP{mid}"
                )
                ok("limpou msg http de teste")

    # limpa msgs de verify desta conversa (nao apaga conversa)
    WhatsAppMensagemAgro.objects.filter(conversa=conv, texto__startswith="VERIFY_").delete()
    ok("limpeza VERIFY_")


def main() -> int:
    print("=== WA-ENVIO-FROMME path detalhado ===")
    print("PIN teste:", PIN)
    prova_estatico()
    prova_orm_enviar()
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
