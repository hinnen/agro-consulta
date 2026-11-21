"""Cartão do cliente: folha em branco, bip e página do celular."""
from __future__ import annotations

from datetime import timedelta
from decimal import Decimal

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.utils import timezone

from produtos.cliente_cartao_util import (
    digito_ean13,
    eh_codigo_cartao,
    emitir_cartoes,
    montar_ean13,
    numero_curto,
    vincular,
)
from produtos.models import ClienteAgro, FiadoTituloAgro, ItemVendaAgro, VendaAgro


class CartaoCodigoTest(TestCase):
    def test_ean_981_fecha_digito(self):
        codigo = montar_ean13(1)
        self.assertEqual(len(codigo), 13)
        self.assertTrue(codigo.startswith("981"))
        self.assertEqual(digito_ean13(codigo[:12]), codigo[12])
        self.assertTrue(eh_codigo_cartao(codigo))
        self.assertFalse(eh_codigo_cartao("2300000000017"))


class CartaoFluxoTest(TestCase):
    def setUp(self):
        self.cliente = ClienteAgro.objects.create(nome="Maria Silva", whatsapp="13999998888")
        self.outro = ClienteAgro.objects.create(nome="João Souza", whatsapp="13988887777")
        User = get_user_model()
        self.user = User.objects.create_user("caixa", password="x")

    def test_folha_e_bip(self):
        self.client.force_login(self.user)
        resp = self.client.post("/caixa/cartao-cliente/", {"qtd": "2"})
        self.assertEqual(resp.status_code, 200)
        self.assertContains(resp, "981")
        self.assertContains(resp, "width: 60%")
        self.assertContains(resp, "margin: 12")
        self.assertContains(resp, "left: 66%")
        cartao = emitir_cartoes(1)[0]
        livre = self.client.post(
            "/api/pdv/cartao-cliente/",
            data={"codigo": cartao.codigo},
            content_type="application/json",
        )
        self.assertEqual(livre.json()["acao"], "livre")
        vinculo = self.client.post(
            "/api/pdv/cartao-cliente/",
            data={"codigo": cartao.codigo, "cliente_agro_pk": self.cliente.pk},
            content_type="application/json",
        )
        self.assertEqual(vinculo.json()["acao"], "vinculou")
        abriu = self.client.post(
            "/api/pdv/cartao-cliente/",
            data={"codigo": cartao.codigo},
            content_type="application/json",
        )
        self.assertEqual(abriu.json()["cliente"]["nome"], "Maria Silva")
        segundo = emitir_cartoes(1)[0]
        vincular(segundo, self.cliente)
        cartao.refresh_from_db()
        self.assertFalse(cartao.ativo)
        troca = self.client.post(
            "/api/pdv/cartao-cliente/",
            data={"codigo": cartao.codigo},
            content_type="application/json",
        )
        self.assertEqual(troca.status_code, 409)

    def test_nao_rouba_cartao_de_outro(self):
        cartao = emitir_cartoes(1)[0]
        vincular(cartao, self.cliente)
        self.client.force_login(self.user)
        resp = self.client.post(
            "/api/pdv/cartao-cliente/",
            data={"codigo": cartao.codigo, "cliente_agro_pk": self.outro.pk},
            content_type="application/json",
        )
        self.assertEqual(resp.status_code, 409)

    def test_portal_pede_o_whatsapp_do_cadastro(self):
        cartao = emitir_cartoes(1)[0]
        vincular(cartao, self.cliente)
        VendaAgro.objects.create(
            cliente_nome="Maria Silva",
            cliente_id_erp=f"local:{self.cliente.pk}",
            total=Decimal("10.00"),
        )
        venda = VendaAgro.objects.get()
        ItemVendaAgro.objects.create(
            venda=venda,
            descricao="Ração",
            quantidade=1,
            valor_unitario=Decimal("10"),
            valor_total=Decimal("10"),
        )
        pagina = self.client.get(f"/cartao/{cartao.token}/")
        self.assertContains(pagina, "WhatsApp")
        self.assertNotContains(pagina, "Ração")
        errado = self.client.post(f"/cartao/{cartao.token}/", {"whatsapp": "11911112222"})
        self.assertContains(errado, "Falar com a loja")
        self.assertContains(errado, numero_curto(cartao.codigo))
        self.assertNotContains(errado, "Ração")
        certo = self.client.post(f"/cartao/{cartao.token}/", {"whatsapp": "13999998888"})
        self.assertContains(certo, "Minhas compras")
        self.assertContains(certo, "1 compra")
        self.assertNotContains(certo, "R$ 10")
        self.assertContains(certo, "Ração")
        self.assertContains(certo, "Cashback")
        self.assertContains(certo, numero_curto(cartao.codigo))
        self.assertContains(certo, "hoje")
        self.assertContains(certo, "O que você mais leva")
        lista = self.client.get(f"/cartao/{cartao.token}/?tela=compras")
        self.assertContains(lista, "R$ 10")
        niver = self.client.post(
            f"/cartao/{cartao.token}/",
            {"aniversario_dia": "19", "aniversario_mes": "11"},
        )
        self.assertContains(niver, "19 de novembro")
        self.cliente.refresh_from_db()
        self.assertEqual(self.cliente.aniversario_dia, 19)
        self.assertEqual(self.cliente.aniversario_mes, 11)
        ruim = self.client.post(
            f"/cartao/{cartao.token}/",
            {"aniversario_dia": "31", "aniversario_mes": "2"},
        )
        self.assertContains(ruim, "Essa data não existe")
        VendaAgro.objects.filter(pk=venda.pk).update(
            criado_em=timezone.now() - timedelta(days=40)
        )
        ItemVendaAgro.objects.filter(venda=venda).update(descricao="Ração 15kg")
        FiadoTituloAgro.objects.create(
            chave_unica=f"teste-cartao-{self.cliente.pk}",
            cliente_agro=self.cliente,
            cliente_nome=self.cliente.nome,
            vencimento=timezone.localdate() - timedelta(days=3),
            valor_bruto=Decimal("40.00"),
        )
        pagina = self.client.get(f"/cartao/{cartao.token}/")
        self.assertContains(pagina, "já passou do prazo habitual")
        self.assertContains(pagina, "40.00")
        self.assertContains(pagina, "venceu em")
