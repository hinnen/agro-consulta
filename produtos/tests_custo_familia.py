"""Testes: custo família (saco → pacote/granel)."""
from __future__ import annotations

from decimal import Decimal

from django.contrib.auth import get_user_model
from django.test import Client, TestCase

from produtos.custo_familia_util import (
    calcular_custo_filho,
    extrair_custo_familia,
    mesclar_custo_familia_no_extras,
    normalizar_custo_familia,
    propagar_custo_familia_de_pai,
)
from produtos.models import ProdutoGestaoOverlayAgro


class CustoFamiliaUtilTests(TestCase):
    def test_calcular_proporcional(self):
        # saco 47 kg a R$ 94 → 5 kg = 10
        self.assertEqual(
            calcular_custo_filho(Decimal("94"), Decimal("47"), Decimal("5")),
            Decimal("10.00"),
        )
        # granel 1 kg
        self.assertEqual(
            calcular_custo_filho(Decimal("94"), Decimal("47"), Decimal("1")),
            Decimal("2.00"),
        )
        # 24 kg → 2 kg
        self.assertEqual(
            calcular_custo_filho(Decimal("48"), Decimal("24"), Decimal("2")),
            Decimal("4.00"),
        )

    def test_normalizar_rejeita_self(self):
        self.assertIsNone(
            normalizar_custo_familia(
                {
                    "ativo": True,
                    "pai_produto_id": "ABC",
                    "kg_pai": 47,
                    "kg_filho": 5,
                },
                filho_id="ABC",
            )
        )

    def test_mesclar_desliga(self):
        ex = {
            "custo_familia": {
                "ativo": True,
                "pai_produto_id": "P1",
                "kg_pai": 47,
                "kg_filho": 5,
                "auto_sync": True,
            }
        }
        mesclar_custo_familia_no_extras(ex, {"ativo": False})
        self.assertNotIn("custo_familia", ex)

    def test_propagar_atualiza_filhos(self):
        ProdutoGestaoOverlayAgro.objects.create(
            produto_externo_id="saco47",
            nome="Milho saco 47kg",
            cadastro_extras={"preco_custo_overlay": 94.0},
        )
        ProdutoGestaoOverlayAgro.objects.create(
            produto_externo_id="pct5",
            nome="Milho 5kg",
            cadastro_extras={
                "custo_familia": {
                    "ativo": True,
                    "pai_produto_id": "saco47",
                    "kg_pai": 47,
                    "kg_filho": 5,
                    "auto_sync": True,
                }
            },
        )
        ProdutoGestaoOverlayAgro.objects.create(
            produto_externo_id="granel",
            nome="Milho granel",
            cadastro_extras={
                "custo_familia": {
                    "ativo": True,
                    "pai_produto_id": "saco47",
                    "kg_pai": 47,
                    "kg_filho": 1,
                    "auto_sync": True,
                }
            },
        )
        ProdutoGestaoOverlayAgro.objects.create(
            produto_externo_id="pct2_off",
            nome="Milho 2kg manual",
            cadastro_extras={
                "custo_familia": {
                    "ativo": True,
                    "pai_produto_id": "saco47",
                    "kg_pai": 47,
                    "kg_filho": 2,
                    "auto_sync": False,
                },
                "preco_custo_overlay": 9.99,
            },
        )
        out = propagar_custo_familia_de_pai("saco47", Decimal("94"))
        self.assertTrue(out["ok"])
        self.assertEqual(out["atualizados"], 2)

        ov5 = ProdutoGestaoOverlayAgro.objects.get(produto_externo_id="pct5")
        self.assertEqual(float(ov5.cadastro_extras["preco_custo_overlay"]), 10.0)
        cf5 = extrair_custo_familia(ov5.cadastro_extras)
        self.assertEqual(cf5["ultimo_custo_calculado"], 10.0)

        ovg = ProdutoGestaoOverlayAgro.objects.get(produto_externo_id="granel")
        self.assertEqual(float(ovg.cadastro_extras["preco_custo_overlay"]), 2.0)

        ov2 = ProdutoGestaoOverlayAgro.objects.get(produto_externo_id="pct2_off")
        self.assertEqual(float(ov2.cadastro_extras["preco_custo_overlay"]), 9.99)

        # muda custo do saco
        out2 = propagar_custo_familia_de_pai("saco47", Decimal("141"))
        self.assertEqual(out2["atualizados"], 2)
        ov5b = ProdutoGestaoOverlayAgro.objects.get(produto_externo_id="pct5")
        self.assertEqual(float(ov5b.cadastro_extras["preco_custo_overlay"]), 15.0)


class CustoFamiliaApiTests(TestCase):
    def setUp(self):
        User = get_user_model()
        self.user = User.objects.create_user("cf_user", password="x")
        self.client = Client()
        self.client.force_login(self.user)
        ProdutoGestaoOverlayAgro.objects.create(
            produto_externo_id="pai_api",
            nome="Saco",
            cadastro_extras={"preco_custo_overlay": 100.0},
        )
        ProdutoGestaoOverlayAgro.objects.create(
            produto_externo_id="filho_api",
            nome="Pct",
            cadastro_extras={
                "custo_familia": {
                    "ativo": True,
                    "pai_produto_id": "pai_api",
                    "kg_pai": 50,
                    "kg_filho": 5,
                    "auto_sync": True,
                }
            },
        )

    def test_api_propagar(self):
        r = self.client.post(
            "/api/produtos/custo-familia/propagar/",
            data='{"produto_id":"pai_api"}',
            content_type="application/json",
        )
        self.assertEqual(r.status_code, 200)
        body = r.json()
        self.assertTrue(body.get("ok"))
        self.assertEqual(body.get("atualizados"), 1)
        ov = ProdutoGestaoOverlayAgro.objects.get(produto_externo_id="filho_api")
        self.assertEqual(float(ov.cadastro_extras["preco_custo_overlay"]), 10.0)
