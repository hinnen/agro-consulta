from datetime import date
from decimal import Decimal

from django.contrib.auth.models import User
from django.test import TestCase

from base.models import Empresa
from produtos.models import ClienteAgro
from rh.models import (
    AjusteManualFolha,
    FechamentoFolhaSimplificado,
    Funcionario,
    HistoricoSalarial,
    ItemFechamentoFolha,
    PagamentoSalarioFuncionario,
)
from rh.services.fechamento import recalcular_fechamento


class AjustesManuaisFolhaTests(TestCase):
    def setUp(self):
        empresa = Empresa.objects.create(nome_fantasia="Loja teste")
        pessoa = ClienteAgro.objects.create(nome="Vitor")
        self.func = Funcionario.objects.create(
            cliente_agro=pessoa,
            empresa=empresa,
            nome_cache="Vitor",
        )
        HistoricoSalarial.objects.create(
            funcionario=self.func,
            salario_base=Decimal("2000.00"),
            data_inicio_vigencia=date(2026, 1, 1),
        )
        self.fech = FechamentoFolhaSimplificado.objects.create(
            funcionario=self.func,
            empresa=empresa,
            competencia=date(2026, 10, 1),
            salario_base_na_competencia=Decimal("2000.00"),
            outros_descontos=Decimal("210.23"),
            outros_proventos=Decimal("164.52"),
            observacoes="+100,00 bônus meta\n- 191,48 contribuição assistencial + inss",
            valor_pago=Decimal("0"),
            ajustes_manuais_inicializados=False,
        )
        self.empresa = empresa

    def test_folha_antiga_mantem_valor_observacao_e_pagamento(self):
        PagamentoSalarioFuncionario.objects.create(
            funcionario=self.func,
            fechamento=self.fech,
            empresa=self.empresa,
            data=date(2026, 10, 5),
            valor=Decimal("500.00"),
            tipo_origem=PagamentoSalarioFuncionario.TipoOrigem.CAIXAS,
        )
        recalcular_fechamento(self.fech)
        self.fech.refresh_from_db()
        self.assertEqual(self.fech.outros_proventos, Decimal("164.52"))
        self.assertEqual(self.fech.outros_descontos, Decimal("210.23"))
        self.assertEqual(self.fech.valor_liquido_previsto, Decimal("1954.29"))
        self.assertEqual(self.fech.valor_pago, Decimal("500.00"))
        self.assertIn("bônus meta", self.fech.observacoes)
        self.assertEqual(
            list(self.fech.ajustes_manuais.order_by("ordem").values_list("descricao", "valor")),
            [("Outros proventos", Decimal("164.52")), ("Outros descontos", Decimal("210.23"))],
        )
        tipos = list(self.fech.itens.values_list("tipo", flat=True))
        self.assertIn(ItemFechamentoFolha.Tipo.PAGAMENTO_SALARIO, tipos)
        self.assertIn(ItemFechamentoFolha.Tipo.DESCONTO, tipos)
        recalcular_fechamento(self.fech)
        self.assertEqual(self.fech.ajustes_manuais.count(), 2)

    def test_salvar_separa_linhas_e_nao_apaga_observacao(self):
        self.fech.ajustes_manuais_inicializados = True
        self.fech.outros_descontos = Decimal("0")
        self.fech.outros_proventos = Decimal("0")
        self.fech.save()
        user = User.objects.create_user("renan", password="x")
        self.client.force_login(user)
        resp = self.client.post(
            f"/rh/gestao/fechamentos/{self.fech.pk}/salvar/",
            {
                "observacoes": "+100 bônus\n-18,75 fiado",
                "valor_pago": "0",
                "status": "ABERTO",
                "form-TOTAL_FORMS": "3",
                "form-INITIAL_FORMS": "0",
                "form-MIN_NUM_FORMS": "0",
                "form-MAX_NUM_FORMS": "1000",
                "form-0-id": "",
                "form-0-tipo": "ACRESCIMO",
                "form-0-descricao": "Bônus meta",
                "form-0-valor": "100.00",
                "form-1-id": "",
                "form-1-tipo": "DESCONTO",
                "form-1-descricao": "Fiado",
                "form-1-valor": "18.75",
                "form-2-id": "",
                "form-2-tipo": "ACRESCIMO",
                "form-2-descricao": "",
                "form-2-valor": "",
            },
        )
        self.assertEqual(resp.status_code, 302)
        self.fech.refresh_from_db()
        self.assertEqual(self.fech.outros_proventos, Decimal("100.00"))
        self.assertEqual(self.fech.outros_descontos, Decimal("18.75"))
        self.assertEqual(self.fech.observacoes, "+100 bônus\n-18,75 fiado")
        self.assertEqual(self.fech.ajustes_manuais.count(), 2)
