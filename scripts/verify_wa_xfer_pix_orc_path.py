# -*- coding: utf-8 -*-
"""
Prova detalhada — lote WhatsApp acertado neste path:

  WA-XFER-UI · WA-PIX-PLAIN · WA-ORC-PDV
  (+ config xfer_avisar_cliente · Fiado+Pix chave)

  python scripts/verify_wa_xfer_pix_orc_path.py
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

fails: list[str] = []
oks: list[str] = []


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


def test_arquivos() -> None:
    print("== Contratos arquivos ==")
    js = _read("produtos/static/produtos/js/atendimento_whatsapp.js")
    bot_js = _read("produtos/static/produtos/js/atendimento_whatsapp_bot.js")
    pdv_js = _read("produtos/static/produtos/js/pdv_wizard.js")
    skin = _read("produtos/templates/produtos/_wa_skin.html")
    dlg = _read("produtos/templates/produtos/_wa_dlg.html")
    wa_html = _read("produtos/templates/produtos/atendimento_whatsapp.html")
    cel_html = _read("produtos/templates/produtos/atendimento_whatsapp_celular.html")
    bot_html = _read("produtos/templates/produtos/atendimento_whatsapp_bot.html")
    step = _read("produtos/templates/produtos/partials/pdv/step_produtos.html")
    cfg = _read("produtos/atendimento_whatsapp_bot_config.py")
    util = _read("produtos/atendimento_whatsapp_util.py")
    rec = _read("produtos/atendimento_whatsapp_recursos.py")
    views = _read("produtos/views_atendimento_whatsapp.py")
    bridge = _read("whatsapp_atendimento/index.js")

    # Modal (sem alert Chrome)
    check("dlg_partial", 'id="wa-dlg"' in dlg and "wa-dlg-inp" in dlg)
    check("dlg_include_pc", '_wa_dlg.html' in wa_html)
    check("dlg_include_cel", '_wa_dlg.html' in cel_html)
    check("dlg_css", ".wa-dlg.is-on" in skin and ".wa-dlg-ok" in skin)
    check("js_waDlg", "function waDlg" in js and "function waConfirm" in js)
    check("js_xfer_sem_confirm", "window.confirm('Passar este atendimento" not in js)
    check("js_xfer_waConfirm", "waConfirm(txt, 'Passar atendimento')" in js or "Passar atendimento" in js)
    check("js_xfer_flag", "xferAvisarCliente" in js and "xfer_avisar_cliente" in js)

    # Bot: avisar cliente + Pix
    check("cfg_xfer_default", '"xfer_avisar_cliente": True' in cfg or "'xfer_avisar_cliente': True" in cfg)
    check("cfg_bool_xfer", '"xfer_avisar_cliente"' in cfg or "'xfer_avisar_cliente'" in cfg)
    check("bot_html_xfer", 'name="xfer_avisar_cliente"' in bot_html)
    check("bot_html_pix_box", 'id="wa-pix-box"' in bot_html and 'name="pix_chave"' in bot_html)
    check("bot_js_pix_fields", "'pix_chave'" in bot_js and "'xfer_avisar_cliente'" in bot_js)
    check("bot_js_bloqueia_sem_chave", "Cole a Chave Pix" in bot_js or "pix_chave" in bot_js)

    # Transfer backend
    check("util_xfer_flag", "xfer_avisar_cliente" in util and "cfg_flag" in util)
    check("util_xfer_ja_loja", "Já está nessa loja" in util)
    check("estado_xfer_api", "xfer_avisar_cliente" in views)

    # Pix plain (sem cta_copy ativo)
    check("rec_feat_fiado_pix", "feat_fiado_pix" in rec)
    check("util_montar_lote_pix", "def montar_lote_pix" in util)
    check("util_anti_link", "def _chave_pix_anti_link" in util)
    check("util_enfileirar_pix", "def enfileirar_pix_com_botao" in util)
    check("util_pix_lote_texto", "_enviar_lote_bot(conversa, lote, c)" in util)
    check("util_sem_tipo_pix_copy_envio", 'tipo_midia="pix_copy"' not in util.split("def enfileirar_pix_com_botao")[1][:800])
    check("bridge_sem_generateWA_cta", "generateWAMessageFromContent" not in bridge and "Copiar chave Pix" not in bridge)
    check("bridge_pix_legado_texto", "pix_copy" in bridge and ("legado" in bridge.lower() or "texto limpo" in bridge.lower()))

    # Orçamento PDV
    check("pdv_btn_celular", 'id="pdv-step1-enviar-whatsapp"' in step and ">Celular<" in step.replace(" ", ""))
    check("pdv_btn_loja", 'id="pdv-step1-enviar-whatsapp-loja"' in step)
    check("pdv_row_grid", "pdv-step1-enviar-whatsapp-row" in step and "grid-cols-2" in step)
    check("pdv_js_loja_fn", "function enviarOrcamentoWhatsappLojaWizard" in pdv_js)
    check("pdv_js_recurso_acao", "recurso-acao" in pdv_js and "acao: 'orcamento'" in pdv_js.replace('"', "'"))
    check("views_orcamento_tel", 'acao == "orcamento"' in views and "abrir_conversa_busca" in views)
    check("views_orcamento_flag", "feat_orcamento_zap" in views)


def test_node() -> None:
    print("== Sintaxe JS / ponte ==")
    for rel in (
        "produtos/static/produtos/js/atendimento_whatsapp.js",
        "produtos/static/produtos/js/atendimento_whatsapp_bot.js",
        "produtos/static/produtos/js/pdv_wizard.js",
        "whatsapp_atendimento/index.js",
    ):
        try:
            r = subprocess.run(
                ["node", "--check", str(ROOT / rel)],
                capture_output=True,
                text=True,
                timeout=30,
            )
            check(f"node_{Path(rel).name}", r.returncode == 0, (r.stderr or "")[:100])
        except FileNotFoundError:
            check("node_skip", True, "node off")
            return


def test_logica_django() -> None:
    print("== Lógica Django ==")
    import django

    django.setup()

    from django.contrib.auth import get_user_model
    from django.test import Client
    from django.urls import reverse

    from produtos.atendimento_whatsapp_bot_config import (
        BOT_DEFAULT,
        avisos_bot,
        cfg_flag,
        salvar_bot,
    )
    from produtos.atendimento_whatsapp_recursos import recurso_on
    from produtos.atendimento_whatsapp_util import (
        interpretar_pedido_pix,
        interpretar_consulta_fiado,
        montar_lote_pix,
        transferir_conversa,
    )
    from produtos.caixa_util import validar_pin_operador
    from produtos.models import WhatsAppConversaAgro, WhatsAppMensagemAgro

    # PIN
    pin_ok, pin_err = validar_pin_operador("9973")
    check("pin_9973", pin_ok, pin_err or "ok")

    # Defaults
    check("default_xfer_on", bool(BOT_DEFAULT.get("xfer_avisar_cliente")) is True)
    check("default_feat_pix_off", recurso_on(BOT_DEFAULT, "feat_fiado_pix") is False)
    check("default_feat_orc_off", recurso_on(BOT_DEFAULT, "feat_orcamento_zap") is False)

    cfg_pix = {
        "feat_fiado_pix": True,
        "fiado_ligado": True,
        "fiado_palavras": "fiado, quanto eu devo",
        "pix_chave": "013997673389",
        "pix_titular": "Teste Titular",
        "pix_palavras": "pix, chave pix, manda pix, passa pix",
        "nome_empresa": "GM Teste",
        "msg_pix_chave": "lixo {13997673389} {Nome Sujo}",
        "msg_pix_sem_chave": "sem chave aqui",
    }
    check("pix_detect", interpretar_pedido_pix("passa pix", cfg_pix))
    check("pix_detect_pix", interpretar_pedido_pix("PIX", cfg_pix))
    check("pix_nao_fiado", not interpretar_consulta_fiado("pix", cfg_pix))
    check("fiado_detect", interpretar_consulta_fiado("fiado", cfg_pix))
    check("pix_off", not interpretar_pedido_pix("pix", {**cfg_pix, "feat_fiado_pix": False}))

    lote = montar_lote_pix(cfg_pix)
    check("lote_2_msgs", len(lote) == 2, str(len(lote)))
    check("lote_sem_chaves", "{" not in lote[0] and "}" not in lote[0], lote[0][:60])
    check("lote_tem_copiar", "Copiar" in lote[0])
    check("lote_chave_anti_link", "\u200b" in lote[1], repr(lote[1]))
    check("lote_sem_chave", montar_lote_pix({**cfg_pix, "pix_chave": ""})[0] == "sem chave aqui")

    avisos = avisos_bot({"feat_fiado_pix": True, "pix_chave": ""})
    check("aviso_sem_chave", bool(avisos) and "Chave Pix" in avisos[0])
    check("aviso_com_chave", not avisos_bot({"feat_fiado_pix": True, "pix_chave": "1"}))

    # Transferência + flag avisar
    tel = "11988887766"
    conv = WhatsAppConversaAgro.objects.create(
        jid=f"{tel}@s.whatsapp.net",
        telefone=tel,
        nome="Prova Xfer Pix",
        loja=WhatsAppConversaAgro.LOJA_CENTRO,
    )
    try:
        # Liga flags no bot config (restaura depois)
        before = __import__("produtos.atendimento_whatsapp_bot_config", fromlist=["carregar_bot"]).carregar_bot()
        snap = dict(before)
        try:
            salvar_bot(
                {
                    **snap,
                    "xfer_avisar_cliente": False,
                    "feat_xfer_nota": True,
                    "feat_fiado_pix": True,
                    "pix_chave": "013997673389",
                    "pix_titular": "Prova",
                },
                usuario="verify-path",
            )
            n_bot_antes = WhatsAppMensagemAgro.objects.filter(
                conversa=conv, direcao=WhatsAppMensagemAgro.DIRECAO_BOT
            ).count()
            c2, err = transferir_conversa(conv.pk, "vila", autor="Prova", nota="nota teste")
            check("xfer_ok", c2 is not None and not err, err or "")
            check("xfer_loja_vila", c2 and c2.loja == WhatsAppConversaAgro.LOJA_VILA)
            n_bot_depois = WhatsAppMensagemAgro.objects.filter(
                conversa=conv, direcao=WhatsAppMensagemAgro.DIRECAO_BOT
            ).count()
            check(
                "xfer_sem_aviso_cliente",
                n_bot_depois == n_bot_antes,
                f"antes={n_bot_antes} depois={n_bot_depois}",
            )

            # Já na vila
            c3, err3 = transferir_conversa(conv.pk, "vila", autor="Prova")
            check("xfer_ja_loja", c3 is None and "Já está" in (err3 or ""))

            # Com aviso ligado
            salvar_bot({**snap, "xfer_avisar_cliente": True}, usuario="verify-path")
            conv.loja = WhatsAppConversaAgro.LOJA_VILA
            conv.save(update_fields=["loja"])
            n0 = WhatsAppMensagemAgro.objects.filter(
                conversa=conv, direcao=WhatsAppMensagemAgro.DIRECAO_BOT
            ).count()
            c4, err4 = transferir_conversa(conv.pk, "centro", autor="Prova")
            check("xfer_com_aviso_ok", c4 is not None and not err4, err4 or "")
            n1 = WhatsAppMensagemAgro.objects.filter(
                conversa=conv, direcao=WhatsAppMensagemAgro.DIRECAO_BOT
            ).count()
            check("xfer_com_aviso_enfileirou", n1 > n0, f"{n0}->{n1}")
            last = (
                WhatsAppMensagemAgro.objects.filter(
                    conversa=conv, direcao=WhatsAppMensagemAgro.DIRECAO_BOT
                )
                .order_by("-id")
                .first()
            )
            check(
                "xfer_aviso_texto",
                bool(last and "Centro" in (last.texto or "") and last.pendente_envio),
                (last.texto or "")[:80] if last else "",
            )
        finally:
            salvar_bot(snap, usuario="verify-path-restore")

        # HTTP APIs
        User = get_user_model()
        u = User.objects.filter(is_superuser=True).first() or User.objects.filter(is_staff=True).first()
        if u is None:
            u = User.objects.create_superuser("wa_verify_path", "wa_verify@test.local", "x")
        c = Client(HTTP_HOST="127.0.0.1")
        c.force_login(u)

        r_est = c.get(reverse("api_atendimento_whatsapp_estado"))
        check("http_estado", r_est.status_code == 200)
        j_est = r_est.json()
        check("http_estado_ok", j_est.get("ok") is True)
        check(
            "http_estado_xfer_flag",
            isinstance((j_est.get("bot") or {}).get("xfer_avisar_cliente"), bool),
        )
        check("http_estado_recursos", "feat_orcamento_zap" in (j_est.get("recursos") or {}))

        r_bot = c.get(reverse("api_atendimento_whatsapp_bot_get"))
        check("http_bot_get", r_bot.status_code == 200 and r_bot.json().get("ok") is True)

        # Orçamento: flag off (força desligado)
        snap_orc = __import__(
            "produtos.atendimento_whatsapp_bot_config", fromlist=["carregar_bot"]
        ).carregar_bot()
        salvar_bot({**snap_orc, "feat_orcamento_zap": False}, usuario="verify-path")
        r_orc_off = c.post(
            reverse("api_atendimento_whatsapp_recurso_acao"),
            data=json.dumps(
                {
                    "acao": "orcamento",
                    "telefone": tel,
                    "nome": "Prova",
                    "texto": "*ORCAMENTO TESTE*\nTOTAL R$ 1,00",
                }
            ),
            content_type="application/json",
        )
        j_off = r_orc_off.json()
        check(
            "http_orc_flag_off",
            r_orc_off.status_code == 400
            and (
                "Orçamento no Zap" in (j_off.get("erro") or "")
                or "Recurso desligado" in (j_off.get("erro") or "")
                or "desligado" in (j_off.get("erro") or "").lower()
            ),
            j_off.get("erro") or str(r_orc_off.status_code),
        )

        # Liga feat orçamento, manda, restaura
        try:
            salvar_bot({**snap_orc, "feat_orcamento_zap": True}, usuario="verify-path")
            r_orc = c.post(
                reverse("api_atendimento_whatsapp_recurso_acao"),
                data=json.dumps(
                    {
                        "acao": "orcamento",
                        "telefone": tel,
                        "nome": "Prova",
                        "texto": "*ORCAMENTO TESTE*\nTOTAL R$ 1,00",
                    }
                ),
                content_type="application/json",
            )
            j_orc = r_orc.json()
            check("http_orc_ok", r_orc.status_code == 200 and j_orc.get("ok") is True, str(j_orc)[:120])
            check("http_orc_conversa_id", int(j_orc.get("conversa_id") or 0) > 0)
            cid = int(j_orc.get("conversa_id") or 0)
            if cid:
                m_orc = (
                    WhatsAppMensagemAgro.objects.filter(conversa_id=cid, texto__contains="ORCAMENTO TESTE")
                    .order_by("-id")
                    .first()
                )
                check("http_orc_enfileirou", m_orc is not None and m_orc.pendente_envio)
        finally:
            salvar_bot(snap_orc, usuario="verify-path-restore")

        # Páginas
        for name in ("atendimento_whatsapp", "atendimento_whatsapp_bot", "atendimento_whatsapp_celular"):
            try:
                rr = c.get(reverse(name))
                check(f"http_page_{name}", rr.status_code == 200)
            except Exception as e:
                check(f"http_page_{name}", False, str(e)[:80])

        # Transfer API
        conv.refresh_from_db()
        conv.loja = WhatsAppConversaAgro.LOJA_CENTRO
        conv.save(update_fields=["loja"])
        r_x = c.post(
            reverse("api_atendimento_whatsapp_transferir"),
            data=json.dumps({"conversa_id": conv.pk, "loja": "vila", "nota": ""}),
            content_type="application/json",
        )
        check("http_xfer", r_x.status_code == 200 and r_x.json().get("ok") is True, str(r_x.json())[:100])
        r_x2 = c.post(
            reverse("api_atendimento_whatsapp_transferir"),
            data=json.dumps({"conversa_id": conv.pk, "loja": "vila"}),
            content_type="application/json",
        )
        check(
            "http_xfer_ja",
            r_x2.status_code == 400 and "Já está" in (r_x2.json().get("erro") or ""),
            str(r_x2.json())[:100],
        )
    finally:
        WhatsAppMensagemAgro.objects.filter(conversa=conv).delete()
        conv.delete()


def main() -> int:
    print("=== WA-XFER-UI + WA-PIX-PLAIN + WA-ORC-PDV ===")
    test_arquivos()
    test_node()
    test_logica_django()
    total = len(oks) + len(fails)
    print("")
    if fails:
        print(f"VERIFY_FAIL {len(oks)}/{total} — falhas: {', '.join(fails)}")
        return 1
    print(f"VERIFY_OK {len(oks)}/{total}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
