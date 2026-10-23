"""Testes do laboratorio de credito (shadow_v1)."""
from __future__ import annotations

from datetime import date, datetime, timedelta
from decimal import Decimal

from unittest import skipUnless

from django.contrib.auth import get_user_model
from django.db import connection
from django.test import Client, SimpleTestCase, TestCase, override_settings
from django.urls import reverse
from django.utils import timezone

from produtos.credito_score_shadow import (
    _fator_pontualidade,
    _multiplicador_limite,
    _pontos_frequencia,
    _pontos_relacionamento,
    _pontos_situacao_atraso,
    analisar_cliente,
    analisar_clientes,
    persistir_analise,
)
from produtos.models import (
    ClienteAgro,
    ClienteAnaliseCreditoAgro,
    FiadoBaixaAgro,
    FiadoTituloAgro,
    VendaAgro,
)

_DB_OK = connection.vendor == "postgresql"


class CreditoScorePurezaTests(SimpleTestCase):
    def test_fator_pontualidade_faixas(self):
        self.assertEqual(_fator_pontualidade(0), Decimal("1.00"))
        self.assertEqual(_fator_pontualidade(3), Decimal("0.90"))
        self.assertEqual(_fator_pontualidade(10), Decimal("0.50"))
        self.assertEqual(_fator_pontualidade(40), Decimal("0.00"))

    def test_situacao_atraso_progressivo(self):
        self.assertEqual(_pontos_situacao_atraso(0), 25)
        self.assertEqual(_pontos_situacao_atraso(20), 3)
        self.assertEqual(_pontos_situacao_atraso(40), 0)

    def test_frequencia_relacionamento_mult(self):
        self.assertEqual(_pontos_frequencia(6), 10)
        self.assertEqual(_pontos_relacionamento(400), 10)
        self.assertEqual(_multiplicador_limite(89), Decimal("1.20"))


def _titulo(cli, *, bruto, pago, situacao, vencimento, chave):
    return FiadoTituloAgro.objects.create(
        chave_unica=chave,
        cliente_agro=cli,
        cliente_nome=cli.nome,
        cliente_codigo=str(cli.pk),
        numero_documento=chave[:40],
        parcela_num=1,
        parcela_total=1,
        vencimento=vencimento,
        valor_bruto=Decimal(bruto),
        valor_pago=Decimal(pago),
        situacao=situacao,
        origem=FiadoTituloAgro.Origem.PDV,
    )


def _baixa(titulo, valor, quando):
    b = FiadoBaixaAgro.objects.create(
        titulo=titulo, valor=Decimal(valor), forma_pagamento="Dinheiro", usuario="teste"
    )
    FiadoBaixaAgro.objects.filter(pk=b.pk).update(criado_em=quando)
    b.refresh_from_db()
    return b


@skipUnless(_DB_OK, "Postgres (migrate 0039 quebra no SQLite)")
class CreditoScoreShadowTests(TestCase):
    def setUp(self):
        self.hoje = date(2026, 10, 5)

    def test_sem_historico_score_null(self):
        cli = ClienteAgro.objects.create(nome="Novo Sem Hist", limite_fiado_local=Decimal("0.01"))
        r = analisar_cliente(cli, hoje=self.hoje)
        self.assertIsNone(r.score)
        self.assertEqual(r.classificacao, ClienteAnaliseCreditoAgro.Classificacao.SEM_HISTORICO)
        self.assertEqual(cli.limite_fiado_local, Decimal("0.01"))

    def test_atrasos_progressivos_pioram_score(self):
        scores = []
        for dias, tag in ((0, "d0"), (3, "d3"), (10, "d10"), (40, "d40")):
            cli = ClienteAgro.objects.create(nome=f"Atraso {tag}", limite_fiado_local=Decimal("300"))
            venc = self.hoje - timedelta(days=60)
            t = _titulo(cli, bruto="200.00", pago="200.00", situacao=FiadoTituloAgro.Situacao.QUITADO, vencimento=venc, chave=f"atr-{tag}-{cli.pk}")
            pag = venc + timedelta(days=dias)
            _baixa(t, "200.00", timezone.make_aware(datetime(pag.year, pag.month, pag.day, 10, 0, 0)))
            venc2 = self.hoje - timedelta(days=200)
            t2 = _titulo(cli, bruto="50.00", pago="50.00", situacao=FiadoTituloAgro.Situacao.QUITADO, vencimento=venc2, chave=f"atr2-{tag}-{cli.pk}")
            _baixa(t2, "50.00", timezone.make_aware(datetime(venc2.year, venc2.month, venc2.day, 10, 0, 0)))
            scores.append(analisar_cliente(cli, hoje=self.hoje, media_3m=Decimal("100")).score)
        self.assertTrue(all(s is not None for s in scores))
        self.assertGreaterEqual(scores[0], scores[1])
        self.assertGreater(scores[1], scores[2])
        self.assertGreater(scores[2], scores[3])

    def test_vencido_atual_derruba_situacao(self):
        cli = ClienteAgro.objects.create(nome="Tem Vencido", limite_fiado_local=Decimal("400"))
        for i in range(4):
            venc = self.hoje - timedelta(days=120 + 30 * i)
            t = _titulo(cli, bruto="80.00", pago="80.00", situacao=FiadoTituloAgro.Situacao.QUITADO, vencimento=venc, chave=f"bom-{cli.pk}-{i}")
            _baixa(t, "80.00", timezone.make_aware(datetime(venc.year, venc.month, venc.day, 9, 0, 0)))
        _titulo(cli, bruto="90.00", pago="0", situacao=FiadoTituloAgro.Situacao.ABERTO, vencimento=self.hoje - timedelta(days=20), chave=f"venc-{cli.pk}")
        r = analisar_cliente(cli, hoje=self.hoje, media_3m=Decimal("100"))
        self.assertTrue(r.tem_vencido)
        self.assertEqual(r.indicadores["situacao_atual_pontos"], 3)

    def test_pagamento_parcial_cancelado_devolucao(self):
        cli = ClienteAgro.objects.create(nome="Parcial", limite_fiado_local=Decimal("200"))
        venc = self.hoje - timedelta(days=10)
        t = _titulo(cli, bruto="100.00", pago="40.00", situacao=FiadoTituloAgro.Situacao.PARCIAL, vencimento=venc, chave=f"par-{cli.pk}")
        _baixa(t, "40.00", timezone.make_aware(datetime(venc.year, venc.month, venc.day, 12, 0, 0)))
        r = analisar_cliente(cli, hoje=self.hoje)
        self.assertEqual(r.indicadores["titulos_quitados_avaliaveis"], 0)
        cli2 = ClienteAgro.objects.create(nome="Cancelado", limite_fiado_local=Decimal("100"))
        _titulo(cli2, bruto="999.00", pago="0", situacao=FiadoTituloAgro.Situacao.CANCELADO, vencimento=self.hoje - timedelta(days=5), chave=f"can-{cli2.pk}")
        r2 = analisar_cliente(cli2, hoje=self.hoje)
        self.assertIsNone(r2.score)

    def test_limite_001_e_zero_legado(self):
        c1 = ClienteAgro.objects.create(nome="Lim 001", limite_fiado_local=Decimal("0.01"))
        persistir_analise(analisar_cliente(c1, hoje=self.hoje))
        c1.refresh_from_db()
        self.assertEqual(c1.limite_fiado_local, Decimal("0.01"))
        c0 = ClienteAgro.objects.create(nome="Lim Zero", limite_fiado_local=Decimal("0"))
        r = analisar_cliente(c0, hoje=self.hoje)
        self.assertEqual(r.limite_cadastrado, Decimal("0.00"))
        self.assertGreater(r.limite_efetivo, Decimal("0"))
        c0.refresh_from_db()
        self.assertEqual(c0.limite_fiado_local, Decimal("0"))

    def test_mesmo_nome_nao_mistura(self):
        a = ClienteAgro.objects.create(nome="Maria Silva", limite_fiado_local=Decimal("200"))
        b = ClienteAgro.objects.create(nome="Maria Silva", limite_fiado_local=Decimal("200"))
        venc = self.hoje - timedelta(days=40)
        t = _titulo(a, bruto="100.00", pago="100.00", situacao=FiadoTituloAgro.Situacao.QUITADO, vencimento=venc, chave=f"mix-a-{a.pk}")
        _baixa(t, "100.00", timezone.make_aware(datetime(venc.year, venc.month, venc.day, 11, 0, 0)))
        ra = analisar_cliente(a, hoje=self.hoje, nomes_duplicados={"maria silva"})
        rb = analisar_cliente(b, hoje=self.hoje, nomes_duplicados={"maria silva"})
        self.assertIsNotNone(ra.score)
        self.assertIsNone(rb.score)

    def test_persistencia_historico_invariancia(self):
        cli = ClienteAgro.objects.create(nome="Invar", limite_fiado_local=Decimal("123.45"))
        venc = self.hoje - timedelta(days=8)
        t = _titulo(cli, bruto="70.00", pago="20.00", situacao=FiadoTituloAgro.Situacao.PARCIAL, vencimento=venc, chave=f"inv-{cli.pk}")
        _baixa(t, "20.00", timezone.make_aware(datetime(venc.year, venc.month, venc.day, 9, 0, 0)))
        v = VendaAgro.objects.create(cliente_nome=cli.nome, cliente_id_erp=f"agro:{cli.pk}", total=Decimal("70.00"), forma_pagamento="Fiado", pagamentos_json=[{"forma": "Fiado", "valor": 70}])
        snap = {"limite": cli.limite_fiado_local, "bruto": t.valor_bruto, "pago": t.valor_pago, "sit": t.situacao, "nb": FiadoBaixaAgro.objects.filter(titulo=t).count(), "vt": v.total, "nv": VendaAgro.objects.count(), "nt": FiadoTituloAgro.objects.count()}
        analisar_clientes(cliente_ids=[cli.pk], persist=True, hoje=self.hoje)
        persistir_analise(analisar_cliente(cli, hoje=self.hoje))
        cli.refresh_from_db(); t.refresh_from_db(); v.refresh_from_db()
        self.assertEqual(cli.limite_fiado_local, snap["limite"])
        self.assertEqual(t.valor_bruto, snap["bruto"])
        self.assertEqual(t.valor_pago, snap["pago"])
        self.assertEqual(t.situacao, snap["sit"])
        self.assertEqual(FiadoBaixaAgro.objects.filter(titulo=t).count(), snap["nb"])
        self.assertEqual(v.total, snap["vt"])
        self.assertEqual(VendaAgro.objects.count(), snap["nv"])
        self.assertEqual(FiadoTituloAgro.objects.count(), snap["nt"])
        self.assertEqual(ClienteAnaliseCreditoAgro.objects.filter(cliente=cli).count(), 2)


@skipUnless(_DB_OK, "Postgres (migrate 0039 quebra no SQLite)")
@override_settings(AGRO_CREDITO_SCORE_SHADOW_ENABLED=True, AGRO_CREDITO_SCORE_SHADOW_USERNAMES="labuser")
class CreditoScoreAcessoTests(TestCase):
    def setUp(self):
        User = get_user_model()
        self.superu = User.objects.create_superuser("sup", "s@t.local", "x")
        self.lab = User.objects.create_user("labuser", "l@t.local", "x")
        self.comum = User.objects.create_user("operador", "o@t.local", "x")
        self.staff = User.objects.create_user("staffer", "st@t.local", "x", is_staff=True)

    def test_acessos(self):
        c = Client()
        c.force_login(self.superu)
        self.assertEqual(c.get(reverse("credito_score_laboratorio")).status_code, 200)
        c.force_login(self.lab)
        self.assertEqual(c.get(reverse("credito_score_laboratorio")).status_code, 200)
        c.force_login(self.comum)
        self.assertEqual(c.get(reverse("credito_score_laboratorio")).status_code, 404)
        c.force_login(self.staff)
        self.assertEqual(c.get(reverse("credito_score_laboratorio")).status_code, 404)


@skipUnless(_DB_OK, "Postgres (migrate 0039 quebra no SQLite)")
@override_settings(AGRO_CREDITO_SCORE_SHADOW_ENABLED=False)
class CreditoScoreFlagOffTests(TestCase):
    def test_flag_off_404(self):
        User = get_user_model()
        u = User.objects.create_superuser("sup2", "s2@t.local", "x")
        c = Client()
        c.force_login(u)
        self.assertEqual(c.get(reverse("credito_score_laboratorio")).status_code, 404)
