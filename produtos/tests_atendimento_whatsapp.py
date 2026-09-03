"""Testes — atendimento WhatsApp (bot Vila/Centro)."""
from __future__ import annotations

from datetime import datetime

from django.contrib.auth import get_user_model
from django.test import Client, SimpleTestCase, TestCase
from django.urls import reverse
from django.utils import timezone

from produtos.atendimento_whatsapp_bot_config import BOT_DEFAULT, fora_do_horario
from produtos.atendimento_whatsapp_util import interpretar_loja, processar_entrada
from produtos.models import WhatsAppConversaAgro, WhatsAppMensagemAgro


class BotConfigPadraoTests(SimpleTestCase):
    def test_horario_e_mensagens(self):
        self.assertTrue(BOT_DEFAULT["horario_ativo"])
        self.assertFalse(BOT_DEFAULT["ainda_atende_fora"])
        self.assertTrue(BOT_DEFAULT["aviso_fora_ligado"])
        self.assertTrue(BOT_DEFAULT["separar_lojas"])
        self.assertTrue(BOT_DEFAULT["enviar_boas_vindas"])
        self.assertTrue(BOT_DEFAULT["ausencia_ligada"])
        domingo = timezone.make_aware(datetime(2026, 9, 6, 10, 0, 0))
        self.assertTrue(fora_do_horario(BOT_DEFAULT, domingo))
        segunda = timezone.make_aware(datetime(2026, 9, 7, 10, 0, 0))
        self.assertFalse(fora_do_horario(BOT_DEFAULT, segunda))


class InterpretarLojaTests(SimpleTestCase):
    def test_centro_e_vila(self):
        self.assertEqual(interpretar_loja("1"), "centro")
        self.assertEqual(interpretar_loja("Centro"), "centro")
        self.assertEqual(interpretar_loja("2"), "vila")
        self.assertEqual(interpretar_loja("vila elias"), "vila")
        self.assertEqual(interpretar_loja("oi"), "")


class BotRoteamentoTests(TestCase):
    def test_menu_depois_escolha(self):
        m, err = processar_entrada(jid="5513999999999@s.whatsapp.net", texto="Oi", nome="Ana")
        self.assertEqual(err, "")
        self.assertIsNotNone(m)
        conv = WhatsAppConversaAgro.objects.get()
        self.assertEqual(conv.loja, "pendente")
        self.assertTrue(conv.menu_enviado)
        self.assertEqual(WhatsAppMensagemAgro.objects.filter(direcao="bot", pendente_envio=True).count(), 1)

        processar_entrada(jid="5513999999999@s.whatsapp.net", texto="2")
        conv.refresh_from_db()
        self.assertEqual(conv.loja, "vila")
        bots = list(WhatsAppMensagemAgro.objects.filter(direcao="bot").order_by("id"))
        self.assertGreaterEqual(len(bots), 2)
        self.assertIn("Vila", bots[-1].texto)

    def test_eco_do_zap_nao_duplica(self):
        processar_entrada(jid="5513999111222@s.whatsapp.net", texto="1", nome="Ana")
        conv = WhatsAppConversaAgro.objects.get()
        from produtos.atendimento_whatsapp_util import enviar_loja

        enviar_loja(conversa_id=conv.pk, texto="oi", autor="Renan")
        n = WhatsAppMensagemAgro.objects.filter(texto="oi").count()
        _m, err = processar_entrada(
            jid="5513999111222@s.whatsapp.net",
            texto="oi",
            de_mim=True,
            wa_id="eco-oi-1",
        )
        self.assertEqual(err, "duplicada")
        self.assertEqual(WhatsAppMensagemAgro.objects.filter(texto="oi").count(), n)

    def test_ja_diz_centro_na_primeira(self):
        processar_entrada(jid="5513888888888@s.whatsapp.net", texto="quero o centro")
        conv = WhatsAppConversaAgro.objects.get()
        self.assertEqual(conv.loja, "centro")

    def test_status_zap_nao_vira_conversa(self):
        m, err = processar_entrada(
            jid="status@broadcast",
            texto="Almoço diferente hoje",
            nome="Rafa",
            telefone="5547999999999",
            wa_id="status-1",
        )
        self.assertIsNone(m)
        self.assertEqual(err, "ignorado")
        self.assertEqual(WhatsAppConversaAgro.objects.count(), 0)

    def test_status_salva_separado(self):
        from produtos.atendimento_whatsapp_util import listar_status, processar_status
        from produtos.models import WhatsAppStatusAgro

        st, err = processar_status(
            jid="5513999000888@s.whatsapp.net",
            texto="Bom dia loja",
            nome="Ana Status",
            wa_id="st-ana-1",
            tipo_midia="text",
        )
        self.assertEqual(err, "")
        self.assertIsNotNone(st)
        self.assertEqual(WhatsAppConversaAgro.objects.count(), 0)
        self.assertEqual(WhatsAppStatusAgro.objects.count(), 1)
        rows = listar_status()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["nome"], "Ana Status")
        self.assertEqual(len(rows[0]["itens"]), 1)
        self.assertEqual(rows[0]["itens"][0]["texto"], "Bom dia loja")


class ConsultaFiadoWhatsAppTests(TestCase):
    def test_fiado_pelo_zap(self):
        from datetime import date
        from decimal import Decimal

        from produtos.models import ClienteAgro, FiadoTituloAgro

        cli = ClienteAgro.objects.create(nome="Maria Zap", whatsapp="13988887777")
        FiadoTituloAgro.objects.create(
            chave_unica="test-wa-fiado-1",
            cliente_agro=cli,
            cliente_nome="Maria Zap",
            vencimento=date(2026, 9, 15),
            valor_bruto=Decimal("80.50"),
            valor_pago=Decimal("0"),
        )
        processar_entrada(jid="5513988887777@s.whatsapp.net", texto="fiado")
        bots = list(WhatsAppMensagemAgro.objects.filter(direcao="bot").order_by("id"))
        self.assertGreaterEqual(len(bots), 1)
        self.assertIn("80,50", bots[0].texto.replace(".", ","))
        self.assertIn("Maria", bots[0].texto)
        self.assertIn("Fiado", bots[0].texto)

    def test_fiado_sem_cadastro(self):
        processar_entrada(jid="5513999000111@s.whatsapp.net", texto="quanto eu devo")
        bot = WhatsAppMensagemAgro.objects.filter(direcao="bot").order_by("id").first()
        self.assertIsNotNone(bot)
        self.assertIn("Não achamos cadastro", bot.texto)

    def test_fiado_fora_do_horario(self):
        from unittest.mock import patch

        with patch("produtos.atendimento_whatsapp_bot_config.fora_do_horario", return_value=True):
            processar_entrada(jid="5513999000111@s.whatsapp.net", texto="fiado")
        bot = WhatsAppMensagemAgro.objects.filter(direcao="bot").order_by("id").last()
        self.assertIsNotNone(bot)
        self.assertNotIn("fora do horário", bot.texto.lower())
        self.assertTrue("cadastro" in bot.texto.lower() or "fiado" in bot.texto.lower())

    def test_horario_desligado_nao_avisa(self):
        from unittest.mock import patch

        from produtos.atendimento_whatsapp_bot_config import BOT_DEFAULT

        cfg = dict(BOT_DEFAULT)
        cfg["horario_ativo"] = False
        with patch("produtos.atendimento_whatsapp_bot_config.carregar_bot", return_value=cfg):
            processar_entrada(jid="5513999000333@s.whatsapp.net", texto="Oi")
        textos = " ".join(m.texto.lower() for m in WhatsAppMensagemAgro.objects.filter(direcao="bot"))
        self.assertNotIn("fora do horário", textos)

    def test_aviso_fora_respeita_intervalo(self):
        from unittest.mock import patch

        from produtos.atendimento_whatsapp_bot_config import BOT_DEFAULT

        cfg = dict(BOT_DEFAULT)
        cfg["aviso_fora_minutos"] = 60
        cfg["ainda_atende_fora"] = False
        with patch("produtos.atendimento_whatsapp_bot_config.carregar_bot", return_value=cfg):
            with patch("produtos.atendimento_whatsapp_bot_config.fora_do_horario", return_value=True):
                processar_entrada(jid="5513999000666@s.whatsapp.net", texto="Oi", wa_id="fora-1")
                processar_entrada(jid="5513999000666@s.whatsapp.net", texto="Oi de novo", wa_id="fora-2")
        n = WhatsAppMensagemAgro.objects.filter(direcao="bot").count()
        self.assertEqual(n, 1)

    def test_saudacao_sem_separar_lojas(self):
        from unittest.mock import patch

        from produtos.atendimento_whatsapp_bot_config import BOT_DEFAULT

        cfg = dict(BOT_DEFAULT)
        cfg["separar_lojas"] = False
        cfg["horario_ativo"] = False
        cfg["enviar_boas_vindas"] = True
        cfg["msg_boas_vindas"] = "Oi da {empresa}"
        with patch("produtos.atendimento_whatsapp_bot_config.carregar_bot", return_value=cfg):
            processar_entrada(jid="5513999000777@s.whatsapp.net", texto="Oi", nome="Ana")
        textos = " ".join(m.texto for m in WhatsAppMensagemAgro.objects.filter(direcao="bot"))
        self.assertIn("GM Agro", textos)
        self.assertNotIn("Responda *1*", textos)

    def test_nome_olha_perfil_primeiro(self):
        from unittest.mock import patch

        from produtos.atendimento_whatsapp_bot_config import BOT_DEFAULT
        from produtos.models import ClienteAgro

        ClienteAgro.objects.create(nome="Maria Cadastro", whatsapp="13988887777")
        cfg = dict(BOT_DEFAULT)
        cfg["nome_fontes"] = "perfil,cadastro,agenda,telefone"
        cfg["horario_ativo"] = False
        cfg["separar_lojas"] = False
        cfg["enviar_boas_vindas"] = False
        with patch("produtos.atendimento_whatsapp_bot_config.carregar_bot", return_value=cfg):
            processar_entrada(jid="5513988887777@s.whatsapp.net", texto="Oi", nome="ZapNome")
        conv = WhatsAppConversaAgro.objects.get()
        self.assertEqual(conv.nome, "ZapNome")
        from unittest.mock import patch

        from produtos.atendimento_whatsapp_bot_config import BOT_DEFAULT

        cfg = dict(BOT_DEFAULT)
        cfg["aviso_fora_ligado"] = False
        with patch("produtos.atendimento_whatsapp_bot_config.carregar_bot", return_value=cfg):
            with patch("produtos.atendimento_whatsapp_bot_config.fora_do_horario", return_value=True):
                processar_entrada(jid="5513999000444@s.whatsapp.net", texto="Oi")
        textos = " ".join(m.texto.lower() for m in WhatsAppMensagemAgro.objects.filter(direcao="bot"))
        self.assertNotIn("fora do horário", textos)

    def test_sem_separar_lojas(self):
        from unittest.mock import patch

        from produtos.atendimento_whatsapp_bot_config import BOT_DEFAULT

        cfg = dict(BOT_DEFAULT)
        cfg["separar_lojas"] = False
        cfg["horario_ativo"] = False
        with patch("produtos.atendimento_whatsapp_bot_config.carregar_bot", return_value=cfg):
            processar_entrada(jid="5513999000555@s.whatsapp.net", texto="Oi")
        conv = WhatsAppConversaAgro.objects.get()
        self.assertEqual(conv.loja, "centro")
        textos = " ".join(m.texto.lower() for m in WhatsAppMensagemAgro.objects.filter(direcao="bot"))
        self.assertNotIn("responda *1*", textos)

    def test_salvar_desliga_flags(self):
        from produtos.atendimento_whatsapp_bot_config import carregar_bot, salvar_bot

        salvar_bot(
            {
                "horario_ativo": False,
                "aviso_fora_ligado": False,
                "separar_lojas": False,
                "ausencia_ligada": False,
            }
        )
        b = carregar_bot()
        self.assertFalse(b["horario_ativo"])
        self.assertFalse(b["aviso_fora_ligado"])
        self.assertFalse(b["separar_lojas"])
        self.assertFalse(b["ausencia_ligada"])

    def test_nome_do_cadastro(self):
        from produtos.models import ClienteAgro

        ClienteAgro.objects.create(nome="Maria Cadastro", whatsapp="13988887777")
        processar_entrada(jid="5513988887777@s.whatsapp.net", texto="Oi", nome="ZapNome")
        conv = WhatsAppConversaAgro.objects.get()
        self.assertEqual(conv.nome, "Maria Cadastro")

    def test_lid_e_telefone_viram_um_chat(self):
        lid = "201812074319879@lid"
        processar_entrada(jid=lid, texto="Oi", nome="Renan", wa_id="lid-1")
        processar_entrada(
            jid="5513997851403@s.whatsapp.net",
            texto="Oi de novo",
            nome="Renan",
            wa_id="pn-1",
            telefone="5513997851403",
            jid_lid=lid,
        )
        self.assertEqual(WhatsAppConversaAgro.objects.count(), 1)
        conv = WhatsAppConversaAgro.objects.get()
        self.assertTrue(conv.jid.endswith("@s.whatsapp.net"))
        self.assertEqual(conv.jid_lid, lid)
        self.assertEqual(WhatsAppMensagemAgro.objects.filter(conversa=conv, direcao="in").count(), 2)

    def test_fiado_pelo_lid_depois_do_numero(self):
        from produtos.models import ClienteAgro

        ClienteAgro.objects.create(nome="Renan Hinnen", whatsapp="13997851403")
        lid = "201812074319879@lid"
        processar_entrada(jid=lid, texto="Oi", wa_id="lid-fiado-1")
        processar_entrada(
            jid=lid,
            texto="fiado",
            wa_id="lid-fiado-2",
            telefone="5513997851403",
            jid_lid=lid,
        )
        conv = WhatsAppConversaAgro.objects.get()
        self.assertIn("13997851403", conv.telefone or conv.jid)
        bot = WhatsAppMensagemAgro.objects.filter(direcao="bot").order_by("id").last()
        self.assertIsNotNone(bot)
        self.assertNotIn("Não achamos cadastro", bot.texto)
        self.assertIn("Renan", bot.texto)

    def test_fiado_no_lid_com_chat_gemeo(self):
        from produtos.models import ClienteAgro

        ClienteAgro.objects.create(nome="Renan Hinnen 1403", whatsapp="13997851403")
        lid = "201812074319879@lid"
        processar_entrada(jid=lid, texto="Oi", nome="Renan Hinnen", wa_id="gemeo-1")
        processar_entrada(
            jid="5513997851403@s.whatsapp.net",
            texto="Oi",
            nome="Renan Hinnen 1403",
            wa_id="gemeo-2",
        )
        self.assertEqual(WhatsAppConversaAgro.objects.count(), 2)
        processar_entrada(jid=lid, texto="fiado", nome="Renan Hinnen", wa_id="gemeo-3")
        self.assertEqual(WhatsAppConversaAgro.objects.count(), 1)
        bot = WhatsAppMensagemAgro.objects.filter(direcao="bot").order_by("id").last()
        self.assertIsNotNone(bot)
        self.assertNotIn("Não achamos cadastro", bot.texto)

    def test_pairing_numero_curto(self):
        from produtos.atendimento_whatsapp_util import pedir_codigo_pareamento

        p, err = pedir_codigo_pareamento("123")
        self.assertIsNone(p)
        self.assertTrue(err)

    def test_excluir_conversa(self):
        from produtos.atendimento_whatsapp_util import excluir_conversa

        processar_entrada(jid="5513999000222@s.whatsapp.net", texto="Oi")
        conv = WhatsAppConversaAgro.objects.get()
        ok, err = excluir_conversa(conv.pk)
        self.assertTrue(ok)
        self.assertEqual(err, "")
        self.assertEqual(WhatsAppConversaAgro.objects.count(), 0)

    def test_excluir_todas_conversas(self):
        from produtos.atendimento_whatsapp_util import excluir_todas_conversas

        processar_entrada(jid="5513999000222@s.whatsapp.net", texto="Oi", wa_id="limpar-1")
        processar_entrada(jid="5513999000444@s.whatsapp.net", texto="Oi", wa_id="limpar-2")
        self.assertEqual(WhatsAppConversaAgro.objects.count(), 2)
        n = excluir_todas_conversas()
        self.assertEqual(n, 2)
        self.assertEqual(WhatsAppConversaAgro.objects.count(), 0)

    def test_mapa_lid_nao_cria_conversa_vazia(self):
        from produtos.atendimento_whatsapp_util import aplicar_mapa_lid, excluir_todas_conversas

        aplicar_mapa_lid({"201812074319879@lid": "5513997851403@s.whatsapp.net"})
        self.assertEqual(WhatsAppConversaAgro.objects.count(), 0)
        processar_entrada(jid="5513997851403@s.whatsapp.net", texto="Oi", wa_id="mapa-1")
        self.assertEqual(WhatsAppConversaAgro.objects.count(), 1)
        excluir_todas_conversas()
        aplicar_mapa_lid({"201812074319879@lid": "5513997851403@s.whatsapp.net"})
        self.assertEqual(WhatsAppConversaAgro.objects.count(), 0)

    def test_apagar_mensagem_pendente_local(self):
        from produtos.atendimento_whatsapp_util import pedir_apagar_mensagem

        processar_entrada(jid="5513999000333@s.whatsapp.net", texto="Oi")
        conv = WhatsAppConversaAgro.objects.get()
        m = WhatsAppMensagemAgro.objects.create(
            conversa=conv,
            direcao=WhatsAppMensagemAgro.DIRECAO_OUT,
            texto="erro sem querer",
            pendente_envio=True,
            autor_nome="Loja",
        )
        ok, err = pedir_apagar_mensagem(m.pk)
        self.assertTrue(ok)
        self.assertEqual(err, "")
        m.refresh_from_db()
        self.assertTrue(m.apagada)
        self.assertFalse(m.pendente_envio)
        self.assertEqual(m.texto, "Mensagem apagada")

    def test_apagar_mensagem_cliente_bloqueado(self):
        from produtos.atendimento_whatsapp_util import pedir_apagar_mensagem

        processar_entrada(jid="5513999000444@s.whatsapp.net", texto="Oi cliente")
        m = WhatsAppMensagemAgro.objects.filter(direcao="in").first()
        ok, err = pedir_apagar_mensagem(m.pk)
        self.assertFalse(ok)
        self.assertIn("loja", err.lower())


class UrlAtendimentoTests(TestCase):
    def setUp(self):
        User = get_user_model()
        self.user = User.objects.create_user("waop", password="x")
        self.client = Client()

    def test_pagina_pede_login(self):
        r = self.client.get(reverse("atendimento_whatsapp"))
        self.assertEqual(r.status_code, 302)

    def test_pagina_ok_logado(self):
        self.client.force_login(self.user)
        r = self.client.get(reverse("atendimento_whatsapp"))
        self.assertEqual(r.status_code, 200)

    def test_pagina_bot_ok_logado(self):
        self.client.force_login(self.user)
        r = self.client.get(reverse("atendimento_whatsapp_bot"))
        self.assertEqual(r.status_code, 200)

    def test_bridge_sem_token(self):
        r = self.client.post(
            reverse("api_atendimento_whatsapp_bridge_entrada"),
            data="{}",
            content_type="application/json",
        )
        self.assertEqual(r.status_code, 403)


class ChamarHistoricoWhatsAppTests(TestCase):
    def test_ignora_grupo_falso(self):
        m, err = processar_entrada(
            jid="120363162264887116@s.whatsapp.net",
            texto="Link de fofoca",
            wa_id="grp-1",
        )
        self.assertEqual(err, "ignorado")
        self.assertIsNone(m)
        self.assertEqual(WhatsAppConversaAgro.objects.count(), 0)

    def test_historico_nao_dispara_bot(self):
        from datetime import timedelta

        from django.utils import timezone

        ts = int((timezone.now() - timedelta(days=1)).timestamp())
        processar_entrada(jid="5513999000111@s.whatsapp.net", texto="Oi", wa_id="hist-0")
        m, err = processar_entrada(
            jid="5513999000111@s.whatsapp.net",
            texto="Oi antigo",
            wa_id="hist-1",
            historico=True,
            ts=ts,
        )
        self.assertEqual(err, "")
        self.assertIsNotNone(m)
        self.assertEqual(WhatsAppMensagemAgro.objects.filter(direcao="bot").count(), 0)
        conv = WhatsAppConversaAgro.objects.get()
        self.assertEqual(conv.nao_lidas, 0)

    def test_msg_recente_cria_chat_depois_limpar(self):
        from produtos.atendimento_whatsapp_util import excluir_todas_conversas

        processar_entrada(jid="5513999000777@s.whatsapp.net", texto="Oi", wa_id="nova-0")
        excluir_todas_conversas()
        self.assertEqual(WhatsAppConversaAgro.objects.count(), 0)
        m, err = processar_entrada(
            jid="5513999000777@s.whatsapp.net",
            texto="Oi de novo",
            wa_id="nova-1",
            historico=False,
        )
        self.assertEqual(err, "")
        self.assertIsNotNone(m)
        self.assertEqual(WhatsAppConversaAgro.objects.count(), 1)

    def test_msg_antiga_como_ao_vivo_nao_dispara_bot(self):
        """Reconnect do Zap não pode mandar boas-vindas sozinho."""
        from datetime import timedelta

        from django.utils import timezone

        ts = int((timezone.now() - timedelta(minutes=30)).timestamp())
        processar_entrada(jid="5513999000888@s.whatsapp.net", texto="Oi", wa_id="replay-0")
        m, err = processar_entrada(
            jid="5513999000888@s.whatsapp.net",
            texto="Oi velho replay",
            wa_id="replay-1",
            historico=False,
            ts=ts,
        )
        self.assertEqual(err, "")
        self.assertIsNotNone(m)
        self.assertEqual(WhatsAppMensagemAgro.objects.filter(direcao="bot").count(), 0)

    def test_aguardando_e_concluir(self):
        from datetime import timedelta

        from django.utils import timezone

        from produtos.atendimento_whatsapp_util import (
            concluir_atendimento,
            enviar_loja,
            marcar_lidas,
            serializar_conversa,
        )

        ts_hist = int((timezone.now() - timedelta(hours=2)).timestamp())
        processar_entrada(
            jid="5513999000999@s.whatsapp.net",
            texto="Oi antigo",
            wa_id="ag-1",
            historico=True,
            ts=ts_hist,
        )
        conv = WhatsAppConversaAgro.objects.get()
        self.assertFalse(conv.aguardando_loja)
        processar_entrada(jid="5513999000999@s.whatsapp.net", texto="Preciso de ração", wa_id="ag-2")
        conv.refresh_from_db()
        self.assertTrue(conv.aguardando_loja)
        self.assertEqual(serializar_conversa(conv)["status"], "nova")
        marcar_lidas(conv.pk)
        conv.refresh_from_db()
        self.assertEqual(serializar_conversa(conv)["status"], "espera")
        ok, err = concluir_atendimento(conv.pk)
        self.assertTrue(ok)
        self.assertEqual(err, "")
        conv.refresh_from_db()
        self.assertFalse(conv.aguardando_loja)
        self.assertEqual(serializar_conversa(conv)["status"], "ok")
        processar_entrada(jid="5513999000999@s.whatsapp.net", texto="Oi de novo", wa_id="ag-3")
        conv.refresh_from_db()
        self.assertTrue(conv.aguardando_loja)
        marcar_lidas(conv.pk)
        enviar_loja(conversa_id=conv.pk, texto="Já te ajudo", autor="Loja")
        conv.refresh_from_db()
        self.assertFalse(conv.aguardando_loja)
        self.assertEqual(serializar_conversa(conv)["status"], "ok")

    def test_telefone_jid_e_novo(self):
        from produtos.atendimento_whatsapp_util import abrir_conversa_saida, telefone_para_jid

        self.assertEqual(telefone_para_jid("13988887777"), "5513988887777@s.whatsapp.net")
        m, err = abrir_conversa_saida(
            telefone="13988887777",
            loja="centro",
            texto="Oi da loja",
            nome="Maria",
        )
        self.assertEqual(err, "")
        self.assertIsNotNone(m)
        conv = WhatsAppConversaAgro.objects.get()
        self.assertEqual(conv.loja, "centro")
        self.assertEqual(conv.origem_abertura, "loja")
        self.assertTrue(m.pendente_envio)

    def test_importar_agenda_vcard(self):
        from produtos.atendimento_whatsapp_util import buscar_contatos_envio, importar_agenda_vcard
        from produtos.models import WhatsAppAgendaContatoAgro

        vcf = (
            "BEGIN:VCARD\nVERSION:2.1\n"
            "FN:Esposa My life\n"
            "TEL;CELL;PREF:+5513996911723\n"
            "END:VCARD\n"
            "BEGIN:VCARD\nVERSION:2.1\n"
            "FN;CHARSET=UTF-8;ENCODING=QUOTED-PRINTABLE:=41=6E=64=72=C3=A9\n"
            "TEL;X-WhatsApp:+5547999906228\n"
            "END:VCARD\n"
        )
        out = importar_agenda_vcard(vcf)
        self.assertEqual(out["lidos"], 2)
        self.assertGreaterEqual(out["gravados"], 2)
        self.assertTrue(WhatsAppAgendaContatoAgro.objects.filter(nome="Esposa My life").exists())
        rows = buscar_contatos_envio("Esposa")
        self.assertTrue(any("Esposa" in (r.get("nome") or "") for r in rows))

    def test_busca_agenda_nome_lid(self):
        from produtos.atendimento_whatsapp_util import buscar_contatos_envio, gravar_agenda_zap
        from produtos.models import WhatsAppAgendaContatoAgro

        n = gravar_agenda_zap(
            [{"jid": "201812074319879@lid", "jid_lid": "201812074319879@lid", "nome": "Joao da Agenda", "telefone": ""}]
        )
        self.assertEqual(n, 1)
        self.assertTrue(WhatsAppAgendaContatoAgro.objects.filter(nome="Joao da Agenda").exists())
        rows = buscar_contatos_envio("Joao")
        self.assertTrue(any(r.get("nome") == "Joao da Agenda" for r in rows))

    def test_abrir_busca_sem_mensagem(self):
        from produtos.atendimento_whatsapp_util import abrir_conversa_busca, enviar_loja

        conv, err = abrir_conversa_busca(telefone="13977776666", nome="Joao Zap")
        self.assertEqual(err, "")
        self.assertEqual(conv.loja, "pendente")
        self.assertEqual(conv.origem_abertura, "loja")
        m, err2 = enviar_loja(conversa_id=conv.pk, texto="Oi", autor="Loja")
        self.assertEqual(err2, "")
        self.assertIsNotNone(m)

    def test_abrir_busca_nao_cria_gemeo_lid(self):
        from produtos.atendimento_whatsapp_util import abrir_conversa_busca, buscar_contatos_envio

        lid = "201812074319879@lid"
        processar_entrada(
            jid=lid,
            texto="Oi do lid",
            nome="",
            wa_id="lid-abrir-1",
            telefone="5513997851403",
            jid_lid=lid,
        )
        conv, err = abrir_conversa_busca(telefone="13997851403", nome="Renan")
        self.assertEqual(err, "")
        self.assertEqual(WhatsAppConversaAgro.objects.count(), 1)
        self.assertEqual(conv.pk, WhatsAppConversaAgro.objects.get().pk)
        rows = buscar_contatos_envio("13997851403")
        self.assertTrue(any(int(r.get("conversa_id") or 0) == conv.pk for r in rows))

    def test_transferir_centro_vila(self):
        from produtos.atendimento_whatsapp_util import transferir_conversa

        processar_entrada(jid="5513999000333@s.whatsapp.net", texto="1")
        conv = WhatsAppConversaAgro.objects.get()
        self.assertEqual(conv.loja, "centro")
        dest, err = transferir_conversa(conv.pk, "vila", autor="Renan")
        self.assertEqual(err, "")
        self.assertEqual(dest.loja, "vila")
        self.assertGreaterEqual(dest.nao_lidas, 1)
        self.assertTrue(
            WhatsAppMensagemAgro.objects.filter(conversa=dest, direcao="bot", texto__icontains="Vila").exists()
        )
