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

    def test_ja_diz_centro_na_primeira(self):
        processar_entrada(jid="5513888888888@s.whatsapp.net", texto="quero o centro")
        conv = WhatsAppConversaAgro.objects.get()
        self.assertEqual(conv.loja, "centro")


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

    def test_aviso_fora_desligado(self):
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

    def test_abrir_busca_sem_mensagem(self):
        from produtos.atendimento_whatsapp_util import abrir_conversa_busca, enviar_loja

        conv, err = abrir_conversa_busca(telefone="13977776666", nome="Joao Zap")
        self.assertEqual(err, "")
        self.assertEqual(conv.loja, "pendente")
        self.assertEqual(conv.origem_abertura, "loja")
        m, err2 = enviar_loja(conversa_id=conv.pk, texto="Oi", autor="Loja")
        self.assertEqual(err2, "")
        self.assertIsNotNone(m)

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
