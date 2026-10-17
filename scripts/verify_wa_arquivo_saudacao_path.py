# -*- coding: utf-8 -*-
"""
Prova detalhada — WA-SAUDACAO-RICH + WA-ARQUIVO

  Arquivar / Resolvidas / Reabrir / msg desarquiva /
  Bot Saudação + Arquivo auto OFF / códigos {hora}{loja}.

  python scripts/verify_wa_arquivo_saudacao_path.py
  AGRO_VERIFY_PIN=9973 (padrão)
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

fails: list[str] = []
oks: list[str] = []
PIN = os.environ.get("AGRO_VERIFY_PIN", "9973")


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


def test_arquivos_path() -> None:
    print("== Arquivos / contratos ==")
    mig = ROOT / "produtos" / "migrations" / "0126_whatsapp_conversa_arquivada.py"
    check("migrate 0126 existe", mig.is_file())
    models = _read("produtos/models.py")
    check("model arquivada", "arquivada = models.BooleanField" in models)
    check("model arquivada_em", "arquivada_em = models.DateTimeField" in models)
    check("model arquivada_por", "arquivada_por = models.CharField" in models)
    check("index arq+loja+ult", "wa_conv_arq_loja_ult_idx" in models)

    util = _read("produtos/atendimento_whatsapp_util.py")
    check("def arquivar_conversa", "def arquivar_conversa" in util)
    check("def reabrir_conversa", "def reabrir_conversa" in util)
    check("desarquiva por msg", "_desarquivar_por_msg_cliente" in util)
    check("listar arquivadas", '"arquivadas"' in util)
    check("filas excluem arquivada", "arquivada=False" in util)
    check("codes {hora}{loja}", '"{hora}"' in util and '"{loja}"' in util)
    check("saudacao_depois_menu", "saudacao_depois_menu" in util)
    check("saudacao_so_em_horario", "saudacao_so_em_horario" in util)

    cfg = _read("produtos/atendimento_whatsapp_bot_config.py")
    check("arquivo_auto default OFF", '"arquivo_auto_ligado": False' in cfg)
    check("saudacao_atraso_seg", "saudacao_atraso_seg" in cfg)
    check("saudacao_midia_url", "saudacao_midia_url" in cfg)

    urls = _read("produtos/urls.py")
    check("url reabrir", "api/atendimento-whatsapp/reabrir/" in urls)
    check("url concluir (arquiva)", "api/atendimento-whatsapp/concluir/" in urls)

    views = _read("produtos/views_atendimento_whatsapp.py")
    check("views arquivar", "arquivar_conversa" in views)
    check("views reabrir", "reabrir_conversa" in views)

    js = _read("produtos/static/produtos/js/atendimento_whatsapp.js")
    check("js reabrir fetch", "/api/atendimento-whatsapp/reabrir/" in js)
    check("js arquivar via concluir", "/api/atendimento-whatsapp/concluir/" in js)
    check("js pintarAcoesArquivo", "function pintarAcoesArquivo" in js)
    check("js badge arquivadas", "arquivadas" in js)
    check("js limpa chat ao arquivar", "limparChatAberto" in js)

    bot_js = _read("produtos/static/produtos/js/atendimento_whatsapp_bot.js")
    check("bot.js saudacao checks", "saudacao_so_primeira_do_dia" in bot_js)
    check("bot.js arquivo checks", "arquivo_auto_ligado" in bot_js)
    check("bot.js previa saudacao", "atualizarPreviaSaudacao" in bot_js)

    bot_html = _read("produtos/templates/produtos/atendimento_whatsapp_bot.html")
    check("aba Saudação", 'data-panel="saudacao"' in bot_html)
    check("aba Arquivo", 'data-panel="arquivo"' in bot_html)
    check("msg_boas só em Saudação", bot_html.count('name="msg_boas_vindas"') == 1)
    check("menu sem checkbox boas", 'name="enviar_boas_vindas"' not in bot_html.split('data-panel="menu"')[1].split("</section>")[0])

    web = _read("produtos/templates/produtos/atendimento_whatsapp.html")
    cel = _read("produtos/templates/produtos/atendimento_whatsapp_celular.html")
    head = _read("produtos/templates/produtos/_wa_chat_head.html")
    check("tab Resolvidas web", 'data-loja="arquivadas"' in web)
    check("tab Resolvidas cel", 'data-loja="arquivadas"' in cel)
    check("btn Reabrir head", 'id="wa-reabrir"' in head)
    check("✓ title Arquivar", "Arquivar" in head)


def test_defaults_bot() -> None:
    print("== Defaults Bot ==")
    import django

    django.setup()
    from produtos.atendimento_whatsapp_bot_config import BOT_DEFAULT, cfg_flag

    check("enviar_boas_vindas True", BOT_DEFAULT.get("enviar_boas_vindas") is True)
    check("saudacao_depois_menu True", BOT_DEFAULT.get("saudacao_depois_menu") is True)
    check("arquivo_auto OFF", BOT_DEFAULT.get("arquivo_auto_ligado") is False)
    check("nunca_com_nao_lidas True", BOT_DEFAULT.get("arquivo_auto_nunca_com_nao_lidas") is True)
    check("saudacao_atraso 0", int(BOT_DEFAULT.get("saudacao_atraso_seg") or 0) == 0)
    check("cfg_flag arquivo OFF", cfg_flag(BOT_DEFAULT, "arquivo_auto_ligado") is False)


def test_orm_arquivo() -> None:
    print("== ORM arquivar / reabrir / desarquiva ==")
    import django

    django.setup()
    from produtos.atendimento_whatsapp_util import (
        arquivar_conversa,
        contar_nao_lidas,
        listar_conversas,
        processar_entrada,
        reabrir_conversa,
        serializar_conversa,
    )
    from produtos.models import WhatsAppConversaAgro

    jid = "5513999000126@s.whatsapp.net"
    WhatsAppConversaAgro.objects.filter(jid=jid).delete()

    m, err = processar_entrada(jid=jid, texto="1", nome="ProvaArq", wa_id="verify-arq-1")
    check("entrada cria chat", err == "" and m is not None, err or "")
    conv = WhatsAppConversaAgro.objects.get(jid=jid)
    check("loja centro apos 1", conv.loja == "centro", conv.loja)
    check("serializar arquivada False", serializar_conversa(conv).get("arquivada") is False)

    ok, e = arquivar_conversa(conv.pk, operador="Renan")
    check("arquivar ok", ok, e)
    conv.refresh_from_db()
    check("flag arquivada", conv.arquivada is True)
    check("arquivada_por Renan", (conv.arquivada_por or "") == "Renan")
    check("nao_lidas zeradas", int(conv.nao_lidas or 0) == 0)
    check("espera zerada", conv.aguardando_loja is False)
    ids_c = {c["id"] for c in listar_conversas(loja="centro")}
    ids_a = {c["id"] for c in listar_conversas(loja="arquivadas")}
    check("some da fila ativa", conv.pk not in ids_c)
    check("aparece em Resolvidas", conv.pk in ids_a)
    bag = contar_nao_lidas()
    check("badge arquivadas >=1", int(bag.get("arquivadas") or 0) >= 1, str(bag.get("arquivadas")))

    ok2, e2 = reabrir_conversa(conv.pk)
    check("reabrir ok", ok2, e2)
    conv.refresh_from_db()
    check("desarquivada", conv.arquivada is False)
    check("loja preservada centro", conv.loja == "centro")
    check("volta na fila centro", conv.pk in {c["id"] for c in listar_conversas(loja="centro")})

    arquivar_conversa(conv.pk, operador="Loja")
    processar_entrada(jid=jid, texto="oi de novo", nome="ProvaArq", wa_id="verify-arq-reopen")
    conv.refresh_from_db()
    check("msg cliente desarquiva", conv.arquivada is False)
    check("volta loja apos msg", conv.loja == "centro")

    WhatsAppConversaAgro.objects.filter(jid=jid).delete()
    check("limpeza prova", WhatsAppConversaAgro.objects.filter(jid=jid).count() == 0)


def test_http_pin() -> None:
    print("== HTTP Client + PIN ==")
    try:
        import django

        django.setup()
        from django.contrib.auth import get_user_model
        from django.test import Client, override_settings

        from produtos.atendimento_whatsapp_util import processar_entrada
        from produtos.models import WhatsAppConversaAgro

        User = get_user_model()
        u = User.objects.filter(is_superuser=True).order_by("id").first()
        if not u:
            u = User.objects.filter(is_staff=True).order_by("id").first()
        with override_settings(ALLOWED_HOSTS=["*", "testserver", "localhost", "127.0.0.1"]):
            c = Client(HTTP_HOST="127.0.0.1")
            if not u:
                check("login staff", False, "sem usuario")
                return
            c.force_login(u)
            check("login staff", True, u.get_username())
            s = c.session
            s["pdv_pin_ok"] = True
            s["pdv_pin_operador"] = "Renan"
            s["pdv_pin_valor"] = PIN
            s.save()
            check("PIN sessao", s.get("pdv_pin_valor") == PIN, PIN)

            r_bot = c.get("/api/atendimento-whatsapp/bot/")
            check("GET bot", r_bot.status_code == 200, str(r_bot.status_code))
            if r_bot.status_code == 200:
                jb = r_bot.json()
                bot = jb.get("bot") or {}
                check("bot ok", bool(jb.get("ok")))
                check("bot arquivo_auto False", bot.get("arquivo_auto_ligado") is False)
                check("bot tem msg_boas", "msg_boas_vindas" in bot)
                check("bot saudacao_depois_menu", "saudacao_depois_menu" in bot)

            r_est = c.get("/api/atendimento-whatsapp/estado/")
            check("GET estado", r_est.status_code == 200)
            if r_est.status_code == 200:
                je = r_est.json()
                nl = je.get("nao_lidas") or {}
                check("estado tem badge arquivadas", "arquivadas" in nl, str(list(nl.keys())))

            jid = "5513999000997@s.whatsapp.net"
            WhatsAppConversaAgro.objects.filter(jid=jid).delete()
            processar_entrada(jid=jid, texto="2", nome="HttpArq", wa_id="verify-http-1")
            conv = WhatsAppConversaAgro.objects.get(jid=jid)
            check("chat vila", conv.loja == "vila", conv.loja)

            r_ok = c.post(
                "/api/atendimento-whatsapp/concluir/",
                data=json.dumps({"conversa_id": conv.pk}),
                content_type="application/json",
            )
            check("POST concluir=arquivar", r_ok.status_code == 200, str(r_ok.status_code))
            if r_ok.status_code == 200:
                check("concluir ok", bool(r_ok.json().get("ok")), str(r_ok.json())[:80])
            conv.refresh_from_db()
            check("apos ✓ arquivada", conv.arquivada is True)

            r_list = c.get("/api/atendimento-whatsapp/conversas/?loja=arquivadas")
            check("GET resolvidas", r_list.status_code == 200)
            ids = {x["id"] for x in (r_list.json().get("conversas") or [])}
            check("chat nas resolvidas API", conv.pk in ids)

            r_vila = c.get("/api/atendimento-whatsapp/conversas/?loja=vila")
            ids_v = {x["id"] for x in (r_vila.json().get("conversas") or [])}
            check("sumiu da vila API", conv.pk not in ids_v)

            r_reb = c.post(
                "/api/atendimento-whatsapp/reabrir/",
                data=json.dumps({"conversa_id": conv.pk}),
                content_type="application/json",
            )
            check("POST reabrir", r_reb.status_code == 200, str(r_reb.status_code))
            conv.refresh_from_db()
            check("apos reabrir ativa", conv.arquivada is False)
            check("loja vila apos reabrir", conv.loja == "vila")

            # páginas HTML
            for path, label in (
                ("/atendimento-whatsapp/", "pagina WA"),
                ("/atendimento-whatsapp/celular/", "pagina WA cel"),
                ("/atendimento-whatsapp/bot/", "pagina Bot"),
            ):
                rp = c.get(path)
                ok_html = rp.status_code == 200 and (
                    "arquivadas" in rp.content.decode("utf-8", "replace")
                    or "saudacao" in rp.content.decode("utf-8", "replace")
                    or "wa-reabrir" in rp.content.decode("utf-8", "replace")
                )
                # Bot page needs saudacao; chat pages need arquivadas/reabrir
                body = rp.content.decode("utf-8", "replace")
                if "bot" in path:
                    ok_html = rp.status_code == 200 and 'data-panel="saudacao"' in body and 'data-panel="arquivo"' in body
                else:
                    ok_html = (
                        rp.status_code == 200
                        and 'data-loja="arquivadas"' in body
                        and 'id="wa-reabrir"' in body
                    )
                check(label, ok_html, f"status={rp.status_code}")

            WhatsAppConversaAgro.objects.filter(jid=jid).delete()
    except Exception as e:
        check("HTTP PIN fluxo", False, str(e)[:180])


def main() -> int:
    print("VERIFY WA-SAUDACAO-RICH + WA-ARQUIVO")
    print(f"ROOT={ROOT} PIN={PIN}")
    test_arquivos_path()
    test_defaults_bot()
    test_orm_arquivo()
    test_http_pin()
    print()
    total = len(oks) + len(fails)
    if fails:
        print(f"VERIFY_FAIL {len(fails)} fail · {len(oks)} ok / {total}")
        for f in fails:
            print(f"  - {f}")
        return 1
    print(f"VERIFY_OK {len(oks)}/{total}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
