"""Testes — atendimento WhatsApp (bot Vila/Centro)."""
from __future__ import annotations

from django.contrib.auth import get_user_model
from django.test import Client, SimpleTestCase, TestCase
from django.urls import reverse

from produtos.atendimento_whatsapp_util import interpretar_loja, processar_entrada
from produtos.models import WhatsAppConversaAgro, WhatsAppMensagemAgro


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

    def test_bridge_sem_token(self):
        r = self.client.post(
            reverse("api_atendimento_whatsapp_bridge_entrada"),
            data="{}",
            content_type="application/json",
        )
        self.assertEqual(r.status_code, 403)
