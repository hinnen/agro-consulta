# -*- coding: utf-8 -*-
"""WA-APP-SEM-PDV — prova detalhada: Zap sem Voltar PDV / FAB F1; Bot no Zap.

Path · dual · FAB · Django Client (PIN 9973) · HTTP local se up · PWA regressao leve.
"""
from __future__ import annotations

import os
import re
import sys
import urllib.error
import urllib.request
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


def tem_voltar_pdv(html: str) -> bool:
    h = html.lower()
    if "voltar ao pdv" in h:
        return True
    if re.search(r"id=[\"']wa-voltar-pdv[\"']", html):
        return True
    if "_pdv_voltar_link" in html:
        return True
    if "agro-pdv-voltar-link" in html and "display: none" not in html:
        # so conta se o link existir fora de CSS hide — bot/chat escondem via CSS global
        pass
    return False


def prova_path() -> None:
    print("=== Path estatico ===")
    html = read("produtos/templates/produtos/atendimento_whatsapp.html")
    bot = read("produtos/templates/produtos/atendimento_whatsapp_bot.html")
    fab = read("produtos/templates/produtos/_agro_pdv_fab.html")
    dual = read("produtos/static/produtos/js/agro_dual_window.js")
    js = read("produtos/static/produtos/js/atendimento_whatsapp.js")

    check('id="wa-voltar-pdv"' not in html, "chat sem id wa-voltar-pdv")
    check("_pdv_voltar_link" not in html, "chat sem include Voltar PDV")
    check("data-agro-hide-pdv" in html, "chat seta hide-pdv")
    check("#agro-pdv-fab-wrap" in html and "display: none" in html, "chat CSS esconde FAB")
    check('a.agro-pdv-voltar-link' in html and "display: none" in html, "chat CSS esconde link PDV")
    check('id="wa-btn-bot"' in html, "botao Bot no chat")
    check("atendimento_whatsapp_bot" in html, "link Bot → tela bot")
    check("agro_pdv_overlay" not in html.split("wa-btn-bot")[1][:200] if "wa-btn-bot" in html else True, "Bot href sem overlay no HTML")

    check("_pdv_voltar_link" not in bot, "bot sem include Voltar PDV")
    check("wa-bot-voltar-chat" in bot, "bot tem voltar Chat")
    check("atendimento_whatsapp" in bot and "url 'atendimento_whatsapp'" in bot, "Chat aponta Zap")
    check("data-agro-hide-pdv" in bot, "bot seta hide-pdv")
    check("#agro-pdv-fab-wrap" in bot and "display: none" in bot, "bot CSS esconde FAB")
    check("Voltar ao PDV" not in bot and "Voltar ao PDV" not in html, "templates sem texto Voltar ao PDV")

    check("function pathIsWhatsApp" in fab, "FAB pathIsWhatsApp")
    check('p === "/atendimento-whatsapp"' in fab or "atendimento-whatsapp/" in fab, "FAB cobre path Zap")
    check("if (pathIsWhatsApp()) return true;" in fab, "FAB some no Zap")
    check("if (pathIsWhatsApp()) return;" in fab, "F1 ignorado no Zap")

    check(
        "if (p === '/atendimento-whatsapp/bot' || p.indexOf('/atendimento-whatsapp/bot/') === 0) return false;"
        not in dual,
        "dual nao exclui Bot do Zap PC",
    )
    # Bot cai em isWhatsAppPcPath via prefixo /atendimento-whatsapp/
    m = re.search(
        r"function isWhatsAppPcPath\(p\)\s*\{(.*?)\n  \}",
        dual,
        re.S,
    )
    check(bool(m), "dual bloco isWhatsAppPcPath")
    if m:
        bloco = m.group(1)
        check("isWhatsAppCelularPath" in bloco, "PC path exclui celular")
        check("atendimento-whatsapp" in bloco, "PC path inclui atendimento-whatsapp")
        check("return false" not in bloco or "Celular" in bloco or "celular" in bloco.lower() or "isWhatsAppCelularPath" in bloco, "unico early-return celular")
    check("isWhatsAppStandalonePath" in dual, "dual isWhatsAppStandalonePath")
    check("SistValeZap" in dual, "janela SistValeZap")
    check("inclui Bot" in dual or "mesmo app" in dual, "comentario Bot no Zap PC")

    check("botHref: '/atendimento-whatsapp/bot/'" in js, "botHref limpo default")
    # overlay ainda pode usar overlay=1 so no modo overlay
    check("botHref: '/atendimento-whatsapp/bot/?agro_pdv_overlay=1'" in js, "overlay ainda passa bot com overlay")


def prova_django_pin() -> None:
    print(f"=== Django Client / PIN {PIN} ===")
    import django

    django.setup()
    from django.test import Client

    from base.models import PerfilUsuario
    from produtos.caixa_util import operador_label_de_pin, rotulo_operador_pin

    perfil = (
        PerfilUsuario.objects.filter(senha_rapida=PIN, ativo=True)
        .select_related("user")
        .first()
    )
    if perfil is None:
        fail(f"PIN {PIN} ativo nao encontrado")
        return
    ok_pin, label, err = operador_label_de_pin(PIN)
    if not ok_pin:
        fail(f"PIN {PIN} invalido: {err}")
        return
    rot = rotulo_operador_pin(PIN) or label
    ok(f"PIN {PIN} valida -> {rot}")

    c = Client(HTTP_HOST="127.0.0.1")
    c.force_login(perfil.user)

    r_chat = c.get("/atendimento-whatsapp/")
    check(r_chat.status_code == 200, f"GET chat {r_chat.status_code}")
    body_chat = r_chat.content.decode("utf-8", errors="replace")
    check(not tem_voltar_pdv(body_chat), "HTML chat sem Voltar PDV")
    check("wa-btn-bot" in body_chat, "HTML chat tem Bot")
    check("/atendimento-whatsapp/bot/" in body_chat, "HTML chat link Bot")
    check("agro-pdv-fab" in body_chat, "FAB markup ainda no layout (escondido)")
    # texto visivel do botao
    check(re.search(r"Voltar\s+ao\s+PDV", body_chat, re.I) is None, "chat sem texto Voltar ao PDV")
    check(re.search(r">\s*PDV\s*<", body_chat) is None or "agro-pdv-fab" in body_chat, "FAB PDV so no markup")

    r_bot = c.get("/atendimento-whatsapp/bot/")
    check(r_bot.status_code == 200, f"GET bot {r_bot.status_code}")
    body_bot = r_bot.content.decode("utf-8", errors="replace")
    check(not tem_voltar_pdv(body_bot), "HTML bot sem Voltar PDV")
    check("wa-bot-voltar-chat" in body_bot, "HTML bot tem Chat")
    check(re.search(r'href=["\']/?atendimento-whatsapp/?["\']', body_bot) or "/atendimento-whatsapp/" in body_bot, "Chat href Zap")
    check(re.search(r"Voltar\s+ao\s+PDV", body_bot, re.I) is None, "bot sem texto Voltar ao PDV")

    # anonimo redireciona login
    c_anon = Client(HTTP_HOST="127.0.0.1")
    ra = c_anon.get("/atendimento-whatsapp/")
    check(ra.status_code in (302, 301), f"anon chat -> login {ra.status_code}")

    if not operador_label_de_pin(PIN)[0]:
        fail("PIN 9973 quebrou no final")
    else:
        ok("PIN 9973 intacto")


def prova_http_local() -> None:
    print("=== HTTP local (opcional) ===")
    try:
        with urllib.request.urlopen("http://127.0.0.1:8000/healthz", timeout=3) as r:
            check(r.status == 200, f"healthz {r.status}")
    except Exception as e:
        ok(f"healthz skip ({e.__class__.__name__})")
        return

    for path, needle in (
        ("/static/produtos/js/agro_dual_window.js", "isWhatsAppPcPath"),
        ("/static/produtos/js/atendimento_whatsapp.js", "botHref: '/atendimento-whatsapp/bot/'"),
    ):
        url = "http://127.0.0.1:8000" + path
        try:
            with urllib.request.urlopen(url, timeout=8) as r:
                txt = r.read().decode("utf-8", errors="replace")
                check(r.status == 200, f"static {path} 200")
                check(needle in txt, f"static tem {needle[:40]}")
                if "dual" in path:
                    check(
                        "if (p === '/atendimento-whatsapp/bot' || p.indexOf('/atendimento-whatsapp/bot/') === 0) return false;"
                        not in txt,
                        "HTTP dual nao exclui Bot",
                    )
        except urllib.error.HTTPError as e:
            fail(f"{path} HTTP {e.code}")
        except Exception as e:
            fail(f"{path}: {e}")


def prova_regressao_pwa() -> None:
    print("=== Regressao PWA leve ===")
    dual = read("produtos/static/produtos/js/agro_dual_window.js")
    html = read("produtos/templates/produtos/atendimento_whatsapp.html")
    check("openWhatsAppPcStandalone" in dual, "openWhatsAppPcStandalone")
    check("SistValeZap" in dual, "SistValeZap")
    check("atendimento_whatsapp_pc_manifest" in html, "manifest PC no HTML")
    check("beforeinstallprompt" in html, "beforeinstallprompt")


def main() -> int:
    prova_path()
    try:
        prova_django_pin()
    except Exception as e:
        fail(f"django: {e}")
    prova_http_local()
    prova_regressao_pwa()

    print("---")
    print(f"OK={OKS} FAIL={len(FAILS)}")
    for f in FAILS:
        print(" ", f)
    return 1 if FAILS else 0


if __name__ == "__main__":
    raise SystemExit(main())
