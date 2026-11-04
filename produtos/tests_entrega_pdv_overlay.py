"""Overlay PDV: entrega a pagar não some quando o caixa original fecha."""

import json
from datetime import timedelta

from django.contrib.auth import get_user_model
from django.test import Client, TestCase
from django.urls import reverse
from django.utils import timezone

from base.models import PerfilUsuario
from produtos.entrega_pdv_pendente_util import (
    data_hoje_loja,
    listar_entregas_bloqueando_fechamento_caixa,
    listar_entregas_pagas_loja_pdv,
    listar_entregas_pendentes_pdv,
)
from produtos.models import PedidoEntrega, SessaoCaixa


class EntregaPdvOverlayCaixaFechadoTests(TestCase):
    def setUp(self):
        User = get_user_model()
        self.user = User.objects.create_user(
            username="op_overlay", password="x", first_name="Renan"
        )
        PerfilUsuario.objects.create(
            user=self.user,
            codigo_vendedor="9973",
            senha_rapida="9973",
            primeiro_acesso=False,
            ativo=True,
        )
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
        self.client = Client()
        self.client.force_login(self.user)

    def _ids_overlay(self, loja, **kwargs):
        return {r["id"] for r in listar_entregas_pendentes_pdv(loja=loja, **kwargs)}

    def _fechar(self, sess=None):
        sess = sess or self.sess
        sess.fechado_em = timezone.now()
        sess.save(update_fields=["fechado_em"])

    def test_overlay_mostra_depois_de_fechar_caixa(self):
        self.assertIn(self.ent.pk, self._ids_overlay("centro"))
        self._fechar()
        self.assertIn(
            self.ent.pk,
            self._ids_overlay("centro"),
            "A pagar some do PDV se filtrar só caixa aberto",
        )
        self.assertNotIn(
            self.ent.pk,
            self._ids_overlay("centro", apenas_caixas_abertos=True),
        )
        self.assertNotIn(self.ent.pk, self._ids_overlay("vila"))

    def test_caixa_novo_trava_pendencia_do_turno_fechado(self):
        self._fechar()
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

    def test_caixa_teste_fechado_nao_trava_gaveta(self):
        teste = SessaoCaixa.objects.create(
            usuario=self.user, ponto_caixa="teste", valor_abertura=0
        )
        prova = PedidoEntrega.objects.create(
            cliente_nome="Prova caixa teste",
            aguarda_pagamento_pdv=True,
            status=PedidoEntrega.Status.PENDENTE,
            sessao_caixa=teste,
            loja_entrega="centro",
            loja_pagamento="centro",
            origem="pdv",
        )
        teste.fechado_em = timezone.now()
        teste.save(update_fields=["fechado_em"])
        novo = SessaoCaixa.objects.create(
            usuario=self.user, ponto_caixa="gaveta", valor_abertura=0
        )
        bloq = listar_entregas_bloqueando_fechamento_caixa(
            sessao_ids=[novo.pk], loja="centro"
        )
        self.assertFalse(any(r["id"] == prova.pk for r in bloq))
        self.assertIn(prova.pk, self._ids_overlay("centro"))

    def test_dia_futuro_nao_trava_hoje(self):
        self._fechar()
        self.ent.data_prevista = data_hoje_loja() + timedelta(days=1)
        self.ent.save(update_fields=["data_prevista", "atualizado_em"])
        novo = SessaoCaixa.objects.create(
            usuario=self.user, ponto_caixa="gaveta", valor_abertura=0
        )
        bloq = listar_entregas_bloqueando_fechamento_caixa(
            sessao_ids=[novo.pk], loja="centro"
        )
        self.assertFalse(any(r["id"] == self.ent.pk for r in bloq))
        self.assertIn(self.ent.pk, self._ids_overlay("centro"))

    def test_adiar_com_pin_solta_hoje_e_overlay_continua(self):
        self._fechar()
        url = reverse("api_pdv_entrega_pendente_adiar_caixa", args=[self.ent.pk])
        ruim = self.client.post(
            url,
            data=json.dumps({"loja": "centro", "pin": "0000"}),
            content_type="application/json",
            HTTP_HOST="127.0.0.1",
        )
        self.assertEqual(ruim.status_code, 403)
        novo = SessaoCaixa.objects.create(
            usuario=self.user, ponto_caixa="gaveta", valor_abertura=0
        )
        bloq_antes = listar_entregas_bloqueando_fechamento_caixa(
            sessao_ids=[novo.pk], loja="centro"
        )
        self.assertTrue(any(r["id"] == self.ent.pk for r in bloq_antes))

        ok = self.client.post(
            url,
            data=json.dumps({"loja": "centro", "pin": "9973"}),
            content_type="application/json",
            HTTP_HOST="127.0.0.1",
        )
        self.assertEqual(ok.status_code, 200)
        self.assertTrue(ok.json().get("ok"))
        self.ent.refresh_from_db()
        self.assertEqual(self.ent.caixa_adiada_para, data_hoje_loja() + timedelta(days=1))
        self.assertIn(self.ent.pk, self._ids_overlay("centro"))
        bloq = listar_entregas_bloqueando_fechamento_caixa(
            sessao_ids=[novo.pk], loja="centro"
        )
        self.assertFalse(any(r["id"] == self.ent.pk for r in bloq))

    def test_api_lista_com_caixa_fechado(self):
        self._fechar()
        resp = self.client.get(
            reverse("api_pdv_entregas_pendentes") + "?loja=centro",
            HTTP_HOST="127.0.0.1",
        )
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        ids = {r.get("id") for r in (data.get("itens") or [])}
        self.assertTrue(data.get("ok"))
        self.assertIn(self.ent.pk, ids)
        resp_vila = self.client.get(
            reverse("api_pdv_entregas_pendentes") + "?loja=vila",
            HTTP_HOST="127.0.0.1",
        )
        ids_vila = {r.get("id") for r in (resp_vila.json().get("itens") or [])}
        self.assertNotIn(self.ent.pk, ids_vila)

    def test_api_grava_amanha_e_recusa_passado(self):
        amanha = (data_hoje_loja() + timedelta(days=1)).isoformat()
        ontem = (data_hoje_loja() - timedelta(days=1)).isoformat()
        url = reverse("api_entrega_registrar")
        base = {
            "cliente_nome": "Prova dia",
            "forma_pagamento": "Pix",
            "pin": "9973",
            "aguarda_pagamento_pdv": True,
            "loja_entrega": "centro",
            "loja_pagamento": "centro",
            "origem": "pdv",
        }
        ruim = self.client.post(
            url,
            data=json.dumps({**base, "data_prevista": ontem, "cliente_nome": "Prova passado"}),
            content_type="application/json",
            HTTP_HOST="127.0.0.1",
        )
        self.assertEqual(ruim.status_code, 400)
        self.assertIn("passado", (ruim.json().get("erro") or "").lower())

        pin_ruim = self.client.post(
            url,
            data=json.dumps({**base, "data_prevista": amanha, "pin": "0000"}),
            content_type="application/json",
            HTTP_HOST="127.0.0.1",
        )
        self.assertEqual(pin_ruim.status_code, 403)

        ok = self.client.post(
            url,
            data=json.dumps({**base, "data_prevista": amanha}),
            content_type="application/json",
            HTTP_HOST="127.0.0.1",
        )
        self.assertEqual(ok.status_code, 200, ok.content)
        body = ok.json()
        self.assertTrue(body.get("ok"))
        novo = PedidoEntrega.objects.get(pk=body["id"])
        self.assertEqual(novo.data_prevista.isoformat(), amanha)
        self.assertIn(novo.pk, self._ids_overlay("centro"))
        bloq = listar_entregas_bloqueando_fechamento_caixa(
            sessao_ids=[self.sess.pk], loja="centro"
        )
        self.assertFalse(any(r["id"] == novo.pk for r in bloq))

    def test_cancelada_some_da_lista_e_do_caixa(self):
        self._fechar()
        resp = self.client.post(
            reverse("api_pdv_entrega_pendente_cancelar", args=[self.ent.pk]),
            data=json.dumps({"motivo": "Prova overlay"}),
            content_type="application/json",
            HTTP_HOST="127.0.0.1",
        )
        self.assertEqual(resp.status_code, 200)
        self.assertNotIn(self.ent.pk, self._ids_overlay("centro"))
        novo = SessaoCaixa.objects.create(
            usuario=self.user, ponto_caixa="gaveta", valor_abertura=0
        )
        bloq = listar_entregas_bloqueando_fechamento_caixa(
            sessao_ids=[novo.pk], loja="centro"
        )
        self.assertFalse(any(r["id"] == self.ent.pk for r in bloq))

    def test_paga_na_loja_fica_na_lista_e_nao_trava(self):
        paga = PedidoEntrega.objects.create(
            cliente_nome="Prova paga na loja",
            paga_na_loja=True,
            aguarda_pagamento_pdv=False,
            pdv_lista_concluida=False,
            status=PedidoEntrega.Status.PENDENTE,
            loja_entrega="centro",
            loja_pagamento="centro",
            data_prevista=data_hoje_loja(),
            origem="pdv",
        )
        ids_pagas = {r["id"] for r in listar_entregas_pagas_loja_pdv(loja="centro")}
        self.assertIn(paga.pk, ids_pagas)
        ids_pagas_vila = {r["id"] for r in listar_entregas_pagas_loja_pdv(loja="vila")}
        self.assertNotIn(paga.pk, ids_pagas_vila)
        bloq = listar_entregas_bloqueando_fechamento_caixa(
            sessao_ids=[self.sess.pk], loja="centro"
        )
        self.assertFalse(any(r["id"] == paga.pk for r in bloq))
        self.assertNotIn(paga.pk, self._ids_overlay("vila"))
