"""PROMO-BUSCA-PG — busca etapa 2 (Postgres, sem depender de Mongo)."""
from __future__ import annotations

from unittest.mock import patch

from django.test import SimpleTestCase, override_settings


class PromocoesBuscaPgTests(SimpleTestCase):
    def test_q_curto_vazio(self):
        from produtos.promocoes_util import buscar_produtos_para_promocao

        self.assertEqual(buscar_produtos_para_promocao(""), [])
        self.assertEqual(buscar_produtos_para_promocao("a"), [])
        self.assertEqual(buscar_produtos_para_promocao("  x  "), [])

    @override_settings(AGRO_FONTE_CATALOGO="agro_pg", AGRO_PDV_CATALOGO_SOMENTE_POSTGRES=False)
    def test_agro_pg_nao_chama_mongo(self):
        from produtos.promocoes_util import buscar_produtos_para_promocao

        fake = [
            {
                "Id": "abc123",
                "Codigo": "GM1507-30",
                "CodigoNFe": "GM1507-30",
                "Nome": "Racao farelo de trigo 30kg",
                "ValorVenda": 60,
            }
        ]
        with (
            patch(
                "produtos.busca_produtos_mongo.buscar_produtos_motor_pdv",
                return_value=fake,
            ) as motor,
            patch("produtos.views.obter_conexao_mongo") as mongo,
        ):
            out = buscar_produtos_para_promocao("GM1507-30", limit=24)
        mongo.assert_not_called()
        motor.assert_called_once()
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0]["produto_externo_id"], "abc123")
        self.assertEqual(out[0]["codigo"], "GM1507-30")
        self.assertEqual(out[0]["nome_produto"], "Racao farelo de trigo 30kg")
        self.assertEqual(out[0]["preco_padrao"], 60.0)

    @override_settings(AGRO_FONTE_CATALOGO="legacy", AGRO_PDV_CATALOGO_SOMENTE_POSTGRES=False)
    def test_legado_mongo_indisponivel_vazio(self):
        from produtos.promocoes_util import buscar_produtos_para_promocao

        with patch("produtos.views.obter_conexao_mongo", return_value=(None, None)):
            self.assertEqual(buscar_produtos_para_promocao("GM1507-30"), [])

    def test_descarta_id_invalido(self):
        from produtos.promocoes_util import buscar_produtos_para_promocao

        fake = [
            {"Id": "", "Nome": "X", "CodigoNFe": "GM1", "ValorVenda": 1},
            {"Id": "none", "Nome": "Y", "CodigoNFe": "GM2", "ValorVenda": 2},
            {"Id": "ok1", "Nome": "Z", "CodigoNFe": "GM3", "ValorVenda": 3.5},
        ]
        with (
            patch("produtos.agro_fonte_config.agro_catalogo_usa_postgres", return_value=True),
            patch(
                "produtos.agro_fonte_config.agro_pdv_catalogo_somente_postgres",
                return_value=False,
            ),
            patch(
                "produtos.busca_produtos_mongo.buscar_produtos_motor_pdv",
                return_value=fake,
            ),
        ):
            out = buscar_produtos_para_promocao("trigo", limit=24)
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0]["produto_externo_id"], "ok1")
        self.assertEqual(out[0]["preco_padrao"], 3.5)

    def test_motor_pg_nao_abre_mongo(self):
        from produtos.busca_produtos_mongo import buscar_produtos_motor_pdv

        with (
            patch("produtos.agro_fonte_config.agro_catalogo_usa_postgres", return_value=True),
            patch(
                "produtos.agro_fonte_config.agro_pdv_catalogo_somente_postgres",
                return_value=False,
            ),
            patch(
                "produtos.catalogo_agro.prods_mongo_style_busca_pdv",
                return_value=[{"Id": "1", "Nome": "A"}],
            ) as pg,
            patch("produtos.views.obter_conexao_mongo") as mongo,
        ):
            out = buscar_produtos_motor_pdv("GM1507-30", limit=40)
        mongo.assert_not_called()
        pg.assert_called_once()
        self.assertEqual(out[0]["Id"], "1")


class PromocoesMapaPdvTests(SimpleTestCase):
    def test_aplica_valor_direto_por_gm_quando_id_diferente(self):
        from produtos.promocoes_util import aplicar_promocao_em_produto_dict

        promo = {
            "id": 9,
            "nome": "farelo de trigo",
            "tipo": "valor_direto",
            "qtd_x": 0,
            "preco_y": 0,
            "preco_produto_promo": 54.9,
        }
        promo_map = {"mongo-id": promo, "GM1507-30": promo}
        row = {"id": "outro-id", "codigo_nfe": "GM1507-30", "preco_venda": 60}
        out = aplicar_promocao_em_produto_dict(row, promo_map)
        self.assertAlmostEqual(out["preco_venda"], 54.9)
        self.assertEqual(out["promocao"]["tipo"], "valor_direto")

    def test_aplica_valor_direto_por_id(self):
        from produtos.promocoes_util import aplicar_promocao_em_produto_dict

        promo = {
            "id": 9,
            "nome": "farelo de trigo",
            "tipo": "valor_direto",
            "qtd_x": 0,
            "preco_y": 0,
            "preco_produto_promo": 54.9,
        }
        out = aplicar_promocao_em_produto_dict(
            {"id": "mongo-id", "preco_venda": 60},
            {"mongo-id": promo},
        )
        self.assertAlmostEqual(out["preco_venda"], 54.9)

    def test_mapa_pdv_expõe_codigo_gm(self):
        util = __import__("pathlib").Path(__file__).with_name("promocoes_util.py").read_text(encoding="utf-8")
        self.assertIn("out[codigo] = d", util)
        self.assertIn("promo_map.get(codigo)", util)
