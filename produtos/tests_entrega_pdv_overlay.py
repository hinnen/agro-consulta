"""Overlay PDV: entrega a pagar não some quando o caixa original fecha."""

from datetime import timedelta

from django.contrib.auth import get_user_model
from django.test import Client, TestCase
from django.urls import reverse
from django.utils import timezone

from produtos.entrega_pdv_pendente_util import (
    adiar_entrega_caixa_um_dia,
    data_hoje_loja,
    listar_entregas_bloqueando_fechamento_caixa,
    listar_entregas_pendentes_pdv,
)
from produtos.models import PedidoEntrega, SessaoCaixa


class EntregaPdvOverlayCaixaFechadoTests(TestCase):
    def setUp(self):
        User = get_user_model()
        self.user = User.objects.create_user(username="op_overlay", password="x")
        self.sess = SessaoCaixa.objects.create(
            usuario=self.user, ponto_caixa="gaveta", valor_abertura=0
        )
        self.ent = PedidoEntrega.objects.create(
            cliente_nome="Robert Bichao",
            aguarda_pagamento_pdv=True,
            status=PedidoEntrega.Status.PENDENTE,
            sessao_caixa=self.sess,
            loja_entrega="centro",
            loja_pagamento="centro",
            forma_pagamento="Dinheiro",
            origem="pdv",
            pdv_wizard_state={"itens": []},
        )

    def _ids_overlay(self, loja):
        return {r["id"] for r in listar_entregas_pendentes_pdv(loja=loja)}

    def test_overlay_mostra_depois_de_fechar_caixa(self):
        self.assertIn(self.ent.pk, self._ids_overlay("centro"))
        self.sess.fechado_em = timezone.now()
        self.sess.save(update_fields=["fechado_em"])
        self.assertIn(
            self.ent.pk,
            self._ids_overlay("centro"),
            "A pagar some do PDV se filtrar só caixa aberto",
        )
        self.assertNotIn(self.ent.pk, self._ids_overlay("vila"))

    def test_caixa_novo_trava_pendencia_do_turno_fechado(self):
        self.sess.fechado_em = timezone.now()
        self.sess.save(update_fields=["fechado_em"])
        novo = SessaoCaixa.objects.create(
            usuario=self.user, ponto_caixa="gaveta", valor_abertura=0
        )
        bloq = listar_entregas_bloqueando_fechamento_caixa(
            sessao_ids=[novo.pk], loja="centro"
        )
        self.assertTrue(any(r["id"] == self.ent.pk for r in bloq))
        bloq_vila = listar_entregas_bloqueando_fechamento_caixa(
            sessao_ids=[novo.pk], loja="vila"
        )
        self.assertFalse(any(r["id"] == self.ent.pk for r in bloq_vila))

    def test_adiar_solta_hoje_e_overlay_continua(self):
        out, err = adiar_entrega_caixa_um_dia(
            self.ent.pk, loja="centro", pin="", quem="Prova overlay"
        )
        self.assertIsNone(err)
        self.assertIsNotNone(out)
        self.sess.fechado_em = timezone.now()
        self.sess.save(update_fields=["fechado_em"])
        self.assertIn(self.ent.pk, self._ids_overlay("centro"))
        bloq = listar_entregas_bloqueando_fechamento_caixa(
            sessao_ids=[self.sess.pk], loja="centro"
        )
        self.assertFalse(any(r["id"] == self.ent.pk for r in bloq))

        self.ent.caixa_adiada_para = data_hoje_loja()
        self.ent.save(update_fields=["caixa_adiada_para", "atualizado_em"])
        novo = SessaoCaixa.objects.create(
            usuario=self.user, ponto_caixa="gaveta", valor_abertura=0
        )
        bloq2 = listar_entregas_bloqueando_fechamento_caixa(
            sessao_ids=[novo.pk], loja="centro"
        )
        self.assertTrue(any(r["id"] == self.ent.pk for r in bloq2))

    def test_api_lista_com_caixa_fechado(self):
        self.sess.fechado_em = timezone.now() - timedelta(hours=2)
        self.sess.save(update_fields=["fechado_em"])
        c = Client()
        c.force_login(self.user)
        resp = c.get(
            reverse("api_pdv_entregas_pendentes") + "?loja=centro",
            HTTP_HOST="127.0.0.1",
        )
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        ids = {r.get("id") for r in (data.get("itens") or [])}
        self.assertTrue(data.get("ok"))
        self.assertIn(self.ent.pk, ids)
