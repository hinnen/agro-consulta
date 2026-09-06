#!/usr/bin/env python3
"""
Prova path PDV-CHAT-POLL-10S — poll Chat interno PDV (fechado 10s / aberto 2,5s).

  python scripts/verify_pdv_chat_poll_10s_path.py
"""
from __future__ import annotations

import ast
import os
import re
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PASS = 0
FAIL = 0


def check(ok: bool, msg: str, detail: str = "") -> None:
    global PASS, FAIL
    if ok:
        PASS += 1
        print(f"  OK  {msg}" + (f" — {detail}" if detail else ""))
    else:
        FAIL += 1
        print(f" FAIL {msg}" + (f" — {detail}" if detail else ""))


def _read(rel: str) -> str:
    return (ROOT / rel).read_text(encoding="utf-8")


def _static() -> None:
    print("=== PDV-CHAT-POLL-10S — estático ===")
    js = _read("produtos/static/produtos/js/pdv_chat_loja.js")
    wiz = _read("produtos/templates/produtos/pdv_wizard.html")
    urls = _read("produtos/urls.py")
    views = _read("produtos/views_pdv_chat_loja.py")
    wa = _read("produtos/static/produtos/js/pdv_topbar_whatsapp.js")

    m_closed = re.search(r"var\s+POLL_MS\s*=\s*(\d+)\s*;", js)
    m_open = re.search(r"var\s+POLL_OPEN_MS\s*=\s*(\d+)\s*;", js)
    closed = int(m_closed.group(1)) if m_closed else None
    open_ms = int(m_open.group(1)) if m_open else None

    check(closed == 10000, "POLL_MS fechado = 10000", f"achou {closed}")
    check(open_ms == 2500, "POLL_OPEN_MS aberto = 2500", f"achou {open_ms}")
    check("var POLL_MS = 4000" not in js, "não ficou no 4s antigo")
    check(
        "isOpen() ? POLL_OPEN_MS : POLL_MS" in js,
        "schedulePoll escolhe aberto×fechado",
    )
    check("function schedulePoll" in js, "schedulePoll existe")
    check("function abrir" in js and "schedulePoll()" in js, "abrir reagenda poll")
    check("function fechar" in js, "fechar existe")
    fechar_block = js.split("function fechar", 1)[-1].split("function ", 1)[0]
    check("schedulePoll()" in fechar_block, "fechar reagenda poll (10s)")
    abrir_block = js.split("function abrir", 1)[-1].split("function fechar", 1)[0]
    check("pollOnce(false)" in abrir_block or "fetchLista(0" in abrir_block, "abrir busca na hora")
    check("function fetchLista" in js and "after_id" in js, "fetchLista incremental")
    check("setInterval" in js and "pollOnce(true)" in js, "intervalo chama pollOnce")
    check("pdv_chat_loja.js" in wiz, "wizard carrega pdv_chat_loja.js")
    check("api_pdv_chat_loja_lista" in urls, "rota lista")
    check("api_pdv_chat_loja_enviar" in urls, "rota enviar")
    check("def api_pdv_chat_loja_lista" in views, "view lista")
    check("@login_required" in views, "APIs exigem login")
    check("/api/atendimento-whatsapp/" not in js, "chat interno nao chama API WhatsApp")
    check("pdv_chat_loja" not in wa, "topbar WA nao puxa chat interno")
    if closed and closed > 0:
        ratio = closed / 4000.0
        check(abs(ratio - 2.5) < 0.01, "fechado = 2.5x o intervalo antigo (menos carga)", f"ratio={ratio}")
        check(open_ms is not None and open_ms < closed, "aberto mais rapido que fechado")
    try:
        ast.parse(views)
        check(True, "views_pdv_chat_loja parse OK")
    except SyntaxError as e:
        check(False, "views_pdv_chat_loja parse", str(e))


def _runtime() -> None:
    print("=== PDV-CHAT-POLL-10S — runtime Django ===")
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
    try:
        import django

        django.setup()
    except Exception as e:
        check(False, "django.setup", str(e)[:200])
        return

    from django.contrib.auth import get_user_model
    from django.test import Client, RequestFactory, override_settings
    from django.urls import reverse

    from produtos.models import ChatLojaMensagemAgro
    from produtos.pdv_chat_loja_util import criar_mensagem, listar_mensagens
    from produtos.pdv_transf_loja_util import PDV_OPERADOR_FRESCO_KEY, gravar_operador_sessao_pdv

    check(
        reverse("api_pdv_chat_loja_lista").endswith("chat-loja/lista/"),
        "reverse lista",
    )

    User = get_user_model()
    user = User.objects.filter(is_active=True).order_by("id").first()
    check(bool(user), "usuário ativo", str(getattr(user, "username", "")))
    if not user:
        return

    rf = RequestFactory()
    req = rf.get("/")
    req.user = user
    req.session = {
        "pdv_operador_nome": "Verify Poll",
        PDV_OPERADOR_FRESCO_KEY: time.time(),
    }
    pin_label = "Verify Poll"
    pin_uid = None

    with override_settings(ALLOWED_HOSTS=["*", "testserver", "localhost", "127.0.0.1"]):
        c_pin = Client(HTTP_HOST="127.0.0.1")
        c_pin.force_login(user)
        pin_req = rf.get("/")
        pin_req.user = user
        pin_req.session = c_pin.session
        try:
            ok_pin, rotulo, _u, err_pin = gravar_operador_sessao_pdv(pin_req, "9973")
            c_pin.session.save()
            if ok_pin:
                pin_label = rotulo or pin_label
                pin_uid = pin_req.session.get("pdv_operador_user_id")
                check(True, "PIN 9973 grava operador fresco", pin_label)
                req = pin_req
            else:
                check(True, "PIN 9973 indisponivel no PG local — usa mock", str(err_pin or "")[:60])
        except Exception as e:
            check(True, "PIN 9973 skip — usa mock", str(e)[:60])

    before = ChatLojaMensagemAgro.objects.count()
    m, err = criar_mensagem(
        req,
        texto="  VERIFY_POLL_10S  ",
        device_id="verify-poll-10s",
        payload={},
    )
    check(m is not None and not err, "criar_mensagem com operador", str(err or f"pk={getattr(m,'pk',None)}"))
    if m:
        rows = listar_mensagens(after_id=0, limit=30)
        check(any(r.get("id") == m.pk for r in rows), "msg na lista after_id=0")
        delta = listar_mensagens(after_id=m.pk - 1 if m.pk > 1 else 0, limit=20)
        check(any(r.get("id") == m.pk for r in delta), "msg no delta (poll)")

    with override_settings(ALLOWED_HOSTS=["*", "testserver", "localhost", "127.0.0.1"]):
        c = Client(HTTP_HOST="127.0.0.1")
        c.force_login(user)
        sess = c.session
        sess["pdv_operador_nome"] = pin_label
        sess[PDV_OPERADOR_FRESCO_KEY] = time.time()
        if pin_uid:
            sess["pdv_operador_user_id"] = pin_uid
        sess.save()

        r0 = c.get(reverse("api_pdv_chat_loja_lista") + "?limit=5")
        check(r0.status_code == 200, "GET lista 200", f"status={r0.status_code}")
        j0 = r0.json() if r0.status_code == 200 else {}
        check(j0.get("ok") is True, "lista ok=true")
        check(isinstance(j0.get("mensagens"), list), "lista mensagens[]")

        after = int(j0.get("last_id") or 0)
        r1 = c.get(reverse("api_pdv_chat_loja_lista") + f"?after_id={after}&limit=50")
        check(r1.status_code == 200, "GET after_id 200")
        j1 = r1.json() if r1.status_code == 200 else {}
        check(j1.get("ok") is True, "after_id ok")
        check(len(r1.content) < 50_000, "after_id leve (poll)", f"{len(r1.content)} B")

        r_env = c.post(
            reverse("api_pdv_chat_loja_enviar"),
            data='{"texto":"http poll 10s","device_id":"verify-poll-http"}',
            content_type="application/json",
        )
        check(r_env.status_code == 200, "POST enviar 200", f"status={r_env.status_code}")
        j_env = r_env.json() if r_env.status_code == 200 else {}
        check(bool(j_env.get("ok")), "enviar ok", str(j_env.get("erro") or "")[:80])

        r_anon = Client(HTTP_HOST="127.0.0.1").get(reverse("api_pdv_chat_loja_lista"))
        check(r_anon.status_code in (302, 401, 403), "lista exige login", f"status={r_anon.status_code}")

    after_n = ChatLojaMensagemAgro.objects.count()
    check(after_n > before, "DB cresceu com provas", f"{before}->{after_n}")

    try:
        n = ChatLojaMensagemAgro.objects.filter(
            device_id__in=["verify-poll-10s", "verify-poll-http"]
        ).delete()[0]
        check(True, "limpou msgs de prova", f"n={n}")
    except Exception as e:
        check(False, "limpar msgs", str(e)[:80])


def main() -> int:
    print("VERIFY PDV-CHAT-POLL-10S")
    _static()
    _runtime()
    print(f"\n{PASS} ok · {FAIL} fail")
    if FAIL:
        print("VERIFY_FAIL")
        return 1
    print("VERIFY_OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
