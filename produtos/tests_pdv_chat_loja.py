"""Testes unitários — chat loja PDV."""
from __future__ import annotations

from django.contrib.auth import get_user_model
from django.test import RequestFactory, SimpleTestCase, TestCase
from django.urls import reverse

from produtos.models import ChatLojaMensagemAgro
from produtos.pdv_chat_loja_util import criar_mensagem, listar_mensagens, serializar_mensagem


class ChatLojaUtilTests(TestCase):
    def setUp(self):
        self.rf = RequestFactory()
        User = get_user_model()
        self.user = User.objects.create_user(username="chatop", password="x")

    def _req(self):
        req = self.rf.get("/")
        req.user = self.user
        req.session = {}
        return req

    def test_criar_e_listar(self):
        req = self._req()
        m, err = criar_mensagem(req, texto="  oi loja  ", device_id="dev-a")
        self.assertEqual(err, "")
        self.assertIsNotNone(m)
        self.assertEqual(m.texto, "oi loja")
        rows = listar_mensagens(after_id=0, limit=10)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["texto"], "oi loja")
        self.assertEqual(rows[0]["device_id"], "dev-a")

    def test_texto_vazio(self):
        m, err = criar_mensagem(self._req(), texto="   ")
        self.assertIsNone(m)
        self.assertTrue(err)

    def test_after_id(self):
        req = self._req()
        m1, _ = criar_mensagem(req, texto="um", device_id="d1")
        m2, _ = criar_mensagem(req, texto="dois", device_id="d2")
        rows = listar_mensagens(after_id=m1.pk, limit=10)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["id"], m2.pk)

    def test_serializar(self):
        req = self._req()
        m, _ = criar_mensagem(req, texto="x")
        d = serializar_mensagem(m)
        self.assertIn("hora", d)
        self.assertEqual(d["texto"], "x")


class ChatLojaUrlTests(SimpleTestCase):
    def test_urls(self):
        self.assertIn("chat-loja/lista", reverse("api_pdv_chat_loja_lista"))
        self.assertIn("chat-loja/enviar", reverse("api_pdv_chat_loja_enviar"))
