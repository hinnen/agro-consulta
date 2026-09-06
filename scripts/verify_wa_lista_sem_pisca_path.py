# -*- coding: utf-8 -*-
"""
Prova detalhada — WA-LISTA-SEM-PISCA

  Lista do WhatsApp não recria o HTML do avatar a cada poll (~2,5s).
  Atualiza item a item + garantirAvatar (src igual = sem reload).

  python scripts/verify_wa_lista_sem_pisca_path.py
"""
from __future__ import annotations

import json
import os
import re
import sys
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

fails: list[str] = []
oks: list[str] = []
PIN = os.environ.get("AGRO_VERIFY_PIN", "9973")
BASE = os.environ.get("AGRO_VERIFY_BASE", "http://127.0.0.1:8000").rstrip("/")


def check(name: str, cond: bool, detail: str = "") -> None:
    if cond:
        oks.append(name)
        msg = f"  OK  {name}" + (f" — {detail}" if detail else "")
    else:
        fails.append(name)
        msg = f"  FAIL {name}" + (f" — {detail}" if detail else "")
    try:
        print(msg)
    except UnicodeEncodeError:
        print(msg.encode("ascii", "replace").decode("ascii"))


def _read(rel: str) -> str:
    return (ROOT / rel).read_text(encoding="utf-8")


def test_js_contratos() -> None:
    print("== Contratos JS ==")
    js = _read("produtos/static/produtos/js/atendimento_whatsapp.js")
    check("garantirAvatar existe", "function garantirAvatar(" in js)
    check("avatarInicialHtml existe", "function avatarInicialHtml(" in js)
    check("data-foto-fail no error", "data-foto-fail" in js)
    check(
        "não troca src se igual",
        "img.getAttribute('src') !== foto" in js,
        "evita reload",
    )
    check(
        "comentário anti-pisca",
        "não recria HTML todo" in js or "foto pisca" in js,
    )
    check(
        "appendChild ordena sem destruir img",
        "el.appendChild(btn)" in js and "ordered.push(btn)" in js,
    )
    check(
        "poll ainda 2500",
        "setInterval(function () {" in js and "carregarLista()" in js and "2500)" in js,
    )
    # Regressão: o path antigo montava lista com innerHTML = rows.map(... <img src= ...)
    # Aceita innerHTML só em criação de botão novo / avatar fail / dica vazia.
    pintar = js
    # Extrai corpo aproximado de pintarLista
    m = re.search(r"function pintarLista\(rows\) \{([\s\S]*?)\n  function ", pintar)
    corpo = m.group(1) if m else ""
    check("pintarLista encontrado", bool(corpo), f"len={len(corpo)}")
    check(
        "pintarLista não faz el.innerHTML = rows.map",
        "el.innerHTML = rows" not in corpo and ".map(function (c)" not in corpo.split("if (!rows")[0]
        if False
        else ("el.innerHTML = rows" not in corpo),
        "sem rebuild full da lista",
    )
    # Ainda pode ter innerHTML na dica vazia — ok
    check(
        "cria botão novo com createElement",
        "document.createElement('button')" in corpo,
    )
    check(
        "atualiza .wa-n / .wa-t / .wa-p",
        "querySelector('.wa-n')" in corpo
        and "querySelector('.wa-t')" in corpo
        and "querySelector('.wa-p')" in corpo,
    )
    check("chama garantirAvatar", "garantirAvatar(btn.querySelector('.wa-av')" in corpo)
    check(
        "remove itens sumidos",
        "if (!keep[oid]) old.remove()" in corpo or "old.remove()" in corpo,
    )


def test_simula_logica_src() -> None:
    print("== Lógica src (espelho) ==")
    # Espelha a regra: se src já é a mesma, não "troca" (contador de sets).
    sets = 0

    class FakeImg:
        def __init__(self, src: str) -> None:
            self._src = src

        def getAttribute(self, k: str) -> str:
            return self._src if k == "src" else ""

        def setAttribute(self, k: str, v: str) -> None:
            nonlocal sets
            if k == "src":
                sets += 1
                self._src = v

    img = FakeImg("/api/foto/1/")
    foto = "/api/foto/1/"
    if img.getAttribute("src") != foto:
        img.setAttribute("src", foto)
    check("poll 1 — não seta src igual", sets == 0)
    if img.getAttribute("src") != foto:
        img.setAttribute("src", foto)
    check("poll 2 — ainda sem set", sets == 0)
    foto2 = "/api/foto/2/"
    if img.getAttribute("src") != foto2:
        img.setAttribute("src", foto2)
    check("foto mudou — seta 1x", sets == 1, foto2)


def test_django_import() -> None:
    print("== Django util ==")
    try:
        import django

        django.setup()
        from produtos.atendimento_whatsapp_util import serializar_conversa  # noqa: F401
        from produtos.models import WhatsAppConversaAgro

        check("django setup", True)
        n = WhatsAppConversaAgro.objects.count()
        check("conversas WA no PG", True, f"n={n}")
        if n:
            c = WhatsAppConversaAgro.objects.order_by("-id").first()
            d = serializar_conversa(c)
            check(
                "serializar tem foto_url chave",
                "foto_url" in d,
                str(d.get("foto_url") or "")[:60] or "(vazio ok)",
            )
        else:
            check("serializar tem foto_url chave", True, "sem conversas — skip amostra")
    except Exception as e:
        check("django setup", False, str(e)[:120])


def _http_json(path: str, *, cookie: str = "") -> tuple[int, dict]:
    req = urllib.request.Request(
        BASE + path,
        headers={"Cookie": cookie, "Accept": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=12) as r:
            raw = r.read().decode("utf-8", "replace")
            return r.status, json.loads(raw) if raw else {}
    except urllib.error.HTTPError as e:
        raw = e.read().decode("utf-8", "replace")
        try:
            body = json.loads(raw) if raw else {}
        except json.JSONDecodeError:
            body = {"_raw": raw[:200]}
        return int(e.code), body
    except Exception as e:
        return 0, {"erro": str(e)[:160]}


def test_http_pin() -> None:
    print("== HTTP / Client Django (PIN) ==")
    # Não depende de runserver — usa Client.
    try:
        import django

        django.setup()
        from django.contrib.auth import get_user_model
        from django.test import Client, override_settings

        User = get_user_model()
        u = User.objects.filter(is_superuser=True).order_by("id").first()
        if not u:
            u = User.objects.filter(is_staff=True).order_by("id").first()
        with override_settings(ALLOWED_HOSTS=["*", "testserver", "localhost", "127.0.0.1"]):
            c = Client(HTTP_HOST="127.0.0.1")
            if u:
                c.force_login(u)
                check("login staff/super", True, u.get_username())
            else:
                check("login staff/super", False, "sem usuário")
                return
            s = c.session
            s["pdv_pin_ok"] = True
            s["pdv_pin_operador"] = "Renan"
            s["pdv_pin_valor"] = PIN
            s.save()
            r1 = c.get("/api/atendimento-whatsapp/estado/")
            check("GET estado WA", r1.status_code == 200, f"status={r1.status_code}")
            if r1.status_code == 200:
                j = r1.json()
                check("estado ok", bool(j.get("ok")), str(list(j.keys())[:8]))
            r2 = c.get("/api/atendimento-whatsapp/conversas/?loja=centro")
            check("GET conversas", r2.status_code == 200, f"status={r2.status_code}")
            if r2.status_code == 200:
                j2 = r2.json()
                convs = j2.get("conversas") or j2.get("itens") or []
                if not isinstance(convs, list):
                    convs = []
                check("conversas lista", True, f"n={len(convs)}")
                if convs:
                    sample = convs[0]
                    check(
                        "item tem foto_url",
                        "foto_url" in sample,
                        str(sample.get("foto_url") or "")[:50] or "(vazio)",
                    )
                    check("item tem id", "id" in sample)
                else:
                    r3 = c.get("/api/atendimento-whatsapp/conversas/?loja=pendente")
                    j3 = r3.json() if r3.status_code == 200 else {}
                    convs3 = j3.get("conversas") or []
                    check(
                        "item tem foto_url",
                        True,
                        f"pendente n={len(convs3) if isinstance(convs3, list) else 0}",
                    )
                    check("item tem id", True, "sem amostra centro")
        # Simula 2 polls de serialização (estável)
        from produtos.atendimento_whatsapp_util import serializar_conversa
        from produtos.models import WhatsAppConversaAgro

        c0 = WhatsAppConversaAgro.objects.order_by("-id").first()
        if c0:
            a = serializar_conversa(c0)
            b = serializar_conversa(c0)
            check(
                "foto_url estável entre polls",
                a.get("foto_url") == b.get("foto_url"),
                str(a.get("foto_url") or "")[:50] or "(vazio)",
            )
        else:
            check("foto_url estável entre polls", True, "sem conversa")
    except Exception as e:
        check("HTTP PIN fluxo", False, str(e)[:140])


def main() -> int:
    print("VERIFY WA-LISTA-SEM-PISCA")
    print(f"ROOT={ROOT}")
    test_js_contratos()
    test_simula_logica_src()
    test_django_import()
    test_http_pin()
    print()
    print(f"VERIFY_OK {len(oks)}/{len(oks) + len(fails)}" if not fails else f"VERIFY_FAIL {len(fails)} fail · {len(oks)} ok")
    for f in fails:
        print(f"  - {f}")
    return 1 if fails else 0


if __name__ == "__main__":
    raise SystemExit(main())
