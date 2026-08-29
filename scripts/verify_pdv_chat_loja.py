"""
Prova detalhada Chat loja PDV (path PDV-CHAT-LOJA).

  python scripts/verify_pdv_chat_loja.py
"""
from __future__ import annotations

import ast
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
fails: list[str] = []
oks: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    if cond:
        oks.append(name)
        print(f"  OK  {name}" + (f" — {detail}" if detail else ""))
    else:
        fails.append(name)
        print(f"  FAIL {name}" + (f" — {detail}" if detail else ""))


def _read(rel: str) -> str:
    return (ROOT / rel).read_text(encoding="utf-8")


def _static_checks() -> None:
    print("VERIFY PDV-CHAT-LOJA — estático")
    urls = _read("produtos/urls.py")
    html = _read("produtos/templates/produtos/partials/pdv/chat_loja_overlay.html")
    js = _read("produtos/static/produtos/js/pdv_chat_loja.js")
    wiz = _read("produtos/templates/produtos/pdv_wizard.html")
    util = _read("produtos/pdv_chat_loja_util.py")
    views = _read("produtos/views_pdv_chat_loja.py")
    boot = _read("pdv/views.py")
    models = _read("produtos/models.py")
    guard = _read("produtos/static/produtos/js/agro_double_submit_guard.js")
    mig = ROOT / "produtos/migrations/0105_chat_loja_mensagem.py"
    base_pdv = _read("base/templates/layouts/base_pdv.html")

    check("url_lista", "api_pdv_chat_loja_lista" in urls)
    check("url_enviar", "api_pdv_chat_loja_enviar" in urls)
    check("wizard_include", "chat_loja_overlay.html" in wiz)
    check("wizard_js", "pdv_chat_loja.js" in wiz)
    check("boot_urls", "apiPdvChatLojaLista" in boot and "apiPdvChatLojaEnviar" in boot)
    check("overlay_id", 'id="pdv-chat-loja-overlay"' in html)
    check("dock_msn", 'id="pdv-chat-loja-dock"' in html and "bottom: 0" in html)
    check("aba_tela", "border-radius: 0.75rem 0.75rem 0 0" in html and "border-bottom: none" in html)
    check("janela_maior", "min(28rem" in html and "min(32rem" in html)
    check("fab_id", 'id="pdv-chat-loja-fab"' in html)
    check("sem_topbar", "pdv-topbar-chat-loja-btn" not in wiz)
    check("no_double_guard_attr", 'data-agro-no-double-guard="1"' in html)
    check("guard_respeita_attr", 'data-agro-no-double-guard' in guard)
    check("base_pdv_tem_guard", "agro_double_submit_guard" in base_pdv)

    check("js_beep", "clBeep" in js and "AudioContext" in js)
    check("js_beep_outro_pc", "m.device_id !== myDev" in js or 'm.device_id !== myDev' in js)
    check("js_badge_count", "syncBadge" in js and "pdv-chat-loja-count" in js)
    check("js_alerta_pisca", "is-alerta" in js and "is-alerta" in html)
    check("js_seen_ls", "agro_chat_loja_seen_id_v1" in js)
    check("js_device_ls", "agro_device_id_v1" in js)
    check("js_poll", "POLL_MS" in js and "after_id" in js)
    check("js_cache_abrir", "cacheMsgs" in js)
    check("js_reset_enviar", "resetEnviarBtn" in js and "textContent = 'Enviar'" in js)
    check("js_fab", "pdv-chat-loja-fab" in js)

    check("util_criar", "def criar_mensagem" in util)
    check("util_listar", "def listar_mensagens" in util)
    check("util_origem", "def resolver_origem_chat" in util)
    check("texto_max_500", "TEXTO_MAX = 500" in util)
    check("view_lista", "def api_pdv_chat_loja_lista" in views)
    check("view_enviar", "def api_pdv_chat_loja_enviar" in views)
    check("view_login", views.count("@login_required") >= 2)
    check("model", "class ChatLojaMensagemAgro" in models)
    check("model_device", "device_id" in models and "ChatLojaMensagemAgro" in models)
    check("migrate_file", mig.is_file())
    check("migrate_table", "ChatLojaMensagemAgro" in mig.read_text(encoding="utf-8"))

    # AST: views não quebram parse
    try:
        ast.parse(views)
        check("views_parse", True)
    except SyntaxError as e:
        check("views_parse", False, str(e))
    try:
        ast.parse(util)
        check("util_parse", True)
    except SyntaxError as e:
        check("util_parse", False, str(e))


def _runtime_checks() -> None:
    print("VERIFY PDV-CHAT-LOJA — runtime Django")
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
    import django

    django.setup()

    from django.contrib.auth import get_user_model
    from django.test import Client, RequestFactory
    from django.urls import reverse

    from produtos.models import ChatLojaMensagemAgro
    from produtos.pdv_chat_loja_util import criar_mensagem, listar_mensagens, serializar_mensagem

    check("reverse_lista", reverse("api_pdv_chat_loja_lista").endswith("chat-loja/lista/"))
    check("reverse_enviar", reverse("api_pdv_chat_loja_enviar").endswith("chat-loja/enviar/"))

    # Migrate aplicada (tabela existe)
    try:
        ChatLojaMensagemAgro.objects.count()
        check("db_table", True)
    except Exception as e:
        check("db_table", False, str(e)[:120])
        return

    User = get_user_model()
    user = User.objects.filter(is_active=True).order_by("id").first()
    check("user_ativo", user is not None)
    if not user:
        return

    rf = RequestFactory()
    req = rf.get("/")
    req.user = user
    req.session = {}

    before = ChatLojaMensagemAgro.objects.count()
    m, err = criar_mensagem(
        req,
        texto="  verify path chat  ",
        device_id="verify-device-a",
        payload={},
    )
    check("criar_ok", m is not None and err == "", err or f"pk={getattr(m, 'pk', None)}")
    check("criar_trim", bool(m) and m.texto == "verify path chat")
    check("criar_device", bool(m) and m.device_id == "verify-device-a")
    check("criar_autor", bool(m) and bool((m.autor_nome or "").strip()))
    check("criar_origem", bool(m) and bool((m.origem_rotulo or "").strip()))

    ser = serializar_mensagem(m) if m else {}
    check("serial_hora", "hora" in ser and "id" in ser and "texto" in ser)

    rows = listar_mensagens(after_id=0, limit=20)
    check("listar_recente", any(r.get("id") == m.pk for r in rows) if m else False)

    m2, err2 = criar_mensagem(req, texto="segunda", device_id="verify-device-b")
    check("criar_2", m2 is not None and err2 == "")
    if m and m2:
        delta = listar_mensagens(after_id=m.pk, limit=20)
        check("listar_after_id", len(delta) >= 1 and delta[0]["id"] == m2.pk, f"n={len(delta)}")

    vazio, err_v = criar_mensagem(req, texto="   ")
    check("rejeita_vazio", vazio is None and bool(err_v))

    longo = "x" * 600
    m_long, err_long = criar_mensagem(req, texto=longo)
    check("rejeita_longo", m_long is None and bool(err_long))

    # HTTP API
    c = Client(HTTP_HOST="127.0.0.1")
    c.force_login(user)
    r_lista = c.get(reverse("api_pdv_chat_loja_lista") + "?limit=10")
    check("http_lista_200", r_lista.status_code == 200, f"status={r_lista.status_code}")
    data_lista = r_lista.json() if r_lista.status_code == 200 else {}
    check("http_lista_ok", bool(data_lista.get("ok")) and isinstance(data_lista.get("mensagens"), list))

    r_env = c.post(
        reverse("api_pdv_chat_loja_enviar"),
        data='{"texto":"http verify","device_id":"verify-http"}',
        content_type="application/json",
    )
    check("http_enviar_200", r_env.status_code == 200, f"status={r_env.status_code}")
    data_env = r_env.json() if r_env.status_code == 200 else {}
    check("http_enviar_ok", bool(data_env.get("ok")) and bool((data_env.get("mensagem") or {}).get("id")))

    r_anon = Client(HTTP_HOST="127.0.0.1").get(reverse("api_pdv_chat_loja_lista"))
    check(
        "http_lista_login",
        r_anon.status_code in (302, 401, 403),
        f"status={r_anon.status_code}",
    )

    after = ChatLojaMensagemAgro.objects.count()
    check("db_cresceu", after > before, f"{before}->{after}")


def main() -> int:
    _static_checks()
    print()
    try:
        _runtime_checks()
    except Exception as e:
        check("runtime_crash", False, str(e)[:200])
        print(f"  FAIL runtime_crash — {e}")

    print()
    print(f"RESULT {len(oks)}/{len(oks) + len(fails)}")
    if fails:
        print("FAILED:", ", ".join(fails))
        return 1
    print("VERIFY_OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
