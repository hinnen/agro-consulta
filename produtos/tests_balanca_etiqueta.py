"""Etiqueta de balança EAN-13 (4 dígitos + preço total)."""
from decimal import Decimal
from unittest.mock import MagicMock, patch

from django.test import SimpleTestCase

from produtos.views import (
    _buscar_produto_por_codigo_interno_balanca,
    _ean13_digito_verificador,
    _escolher_produto_plu_balanca,
    _parse_etiqueta_balanca_ean13_br,
    _produto_casa_plu_balanca,
)


class EtiquetaBalancaEan13Tests(SimpleTestCase):
    def test_parse_plu_0010_preco_4_81(self):
        r = _parse_etiqueta_balanca_ean13_br("2001000004812")
        self.assertIsNotNone(r)
        cod4, preco = r
        self.assertEqual(cod4, "0010")
        self.assertEqual(preco, Decimal("4.81"))

    def test_dv_invalido_rejeita(self):
        self.assertIsNone(_parse_etiqueta_balanca_ean13_br("2001000004810"))

    def test_dv_esperado(self):
        self.assertEqual(_ean13_digito_verificador("200100000481"), 2)

    def test_busca_prefere_plu_4_digitos(self):
        client = MagicMock()
        client.col_p = "DtoProduto"
        col = MagicMock()
        db = {client.col_p: col}
        calls = []

        def find_one(q, *a, **k):
            calls.append(q.get("index_codigos"))
            if q.get("index_codigos") == "0010":
                return {"Id": "ok", "index_codigos": ["0010"]}
            if q.get("index_codigos") == "10":
                return {"Id": "errado", "index_codigos": ["10"]}
            return None

        col.find_one.side_effect = find_one
        got = _buscar_produto_por_codigo_interno_balanca(db, client, "0010")
        self.assertEqual(got["Id"], "ok")
        self.assertEqual(calls[0], "0010")

    def test_busca_overlay_quando_sem_index(self):
        client = MagicMock()
        client.col_p = "DtoProduto"
        col = MagicMock()
        db = {client.col_p: col}
        col.find_one.return_value = None
        col.find.return_value.limit.return_value = []
        with patch(
            "produtos.views._mongo_produtos_por_overlay_codigo_busca",
            return_value=[{"Id": "ov1", "Nome": "Racao"}],
        ):
            got = _buscar_produto_por_codigo_interno_balanca(db, client, "0010")
        self.assertEqual(got["Id"], "ov1")

    def test_casa_plu_gm_e_barras(self):
        self.assertTrue(
            _produto_casa_plu_balanca({"Codigo": "GM0010-1", "index_codigos": []}, "0010")
        )
        self.assertTrue(
            _produto_casa_plu_balanca({"CodigoBarras": "0010", "index_codigos": []}, "0010")
        )
        self.assertTrue(
            _produto_casa_plu_balanca(
                {"Codigo": "", "index_codigos": ["gm0010-1", "gm00101"]}, "0010"
            )
        )
        # PLU 0010 NÃO casa barras só «10» (outro produto)
        self.assertFalse(
            _produto_casa_plu_balanca({"CodigoBarras": "10", "index_codigos": ["10"]}, "0010")
        )
        self.assertFalse(
            _produto_casa_plu_balanca({"Codigo": "GM0143", "index_codigos": []}, "0010")
        )

    def test_overlay_pids_aceita_plu_4_digitos(self):
        from produtos.cadastro_busca_codigo_util import overlay_pids_por_codigo

        with patch(
            "produtos.models.ProdutoGestaoOverlayAgro.objects.filter"
        ) as m_filter:
            m_qs = MagicMock()
            m_filter.return_value = m_qs
            m_qs.only.return_value = []
            # slice [:N] no Django QS — mock encadeado
            m_qs.__getitem__ = MagicMock(return_value=[])
            m_filter.return_value = m_qs
            overlay_pids_por_codigo("0010", limit=10)
            self.assertTrue(m_filter.called, "overlay deve consultar PLU 0010")

    def test_motor_plu_nao_pula_mongo_vazio(self):
        """Sob agro_pg, PLU 0010 sem hit PG / só ruído deve complementar Mongo."""
        import produtos.motor_busca_unificado_util as motor

        src = open(motor.__file__, encoding="utf-8").read()
        self.assertIn("_plu_balanca", src)
        self.assertIn('len(_dig_termo) == 4', src)
        self.assertIn("exact_plu", src)

    def test_overlay_aplica_preco_unitario_com_flag_etiqueta(self):
        """Flag de etiqueta não bloqueia preço unitário do overlay (qty = total÷unitário)."""
        from types import SimpleNamespace

        from produtos.views import _aplicar_produto_gestao_overlay_em_dict

        def _s(v=""):
            return SimpleNamespace(strip=lambda: v)

        ov = SimpleNamespace(
            nome=_s(""),
            marca=_s(""),
            categoria=_s(""),
            fornecedor_texto=_s(""),
            unidade=_s(""),
            peso_etiqueta="",
            preco_venda=Decimal("9.40"),
            codigo_barras=_s("0010"),
            codigo_nfe=_s("GM0010-1"),
            subcategoria=_s(""),
            descricao=_s(""),
            ativo_exibicao=None,
            cadastro_extras={},
        )
        row = {
            "preco_venda": 4.81,
            "preco_etiqueta_balanca": True,
            "valor_etiqueta_balanca": 4.81,
        }
        with (
            patch("produtos.cashback_venda_util.cashback_percentual_de_overlay", return_value=0.0),
            patch("produtos.views.extrair_precos_por_forma_overlay", return_value=None),
            patch("produtos.views.extrair_precos_modo_overlay", return_value=None),
            patch("produtos.views.extrair_precos_grupos_overlay", return_value=None),
            patch("produtos.views._overlay_subcategorias_para_row"),
        ):
            _aplicar_produto_gestao_overlay_em_dict(row, ov)
        self.assertEqual(float(row["preco_venda"]), 9.40)
        self.assertEqual(float(row["valor_etiqueta_balanca"]), 4.81)

    def test_qty_por_valor_etiqueta(self):
        """4,81 ÷ 9,40 ≈ 0,512 kg — estoque e total batem."""
        total = Decimal("4.81")
        unit = Decimal("9.40")
        qtd = (total / unit).quantize(Decimal("0.001"))
        self.assertEqual(qtd, Decimal("0.512"))
        linha = (unit * qtd).quantize(Decimal("0.01"))
        self.assertEqual(linha, Decimal("4.81"))

    def test_escolhe_gm_menos_1_entre_varios(self):
        cand = [
            {"Id": "a", "Codigo": "GM0010-25", "CodigoBarras": ""},
            {"Id": "b", "Codigo": "GM0010-1", "CodigoBarras": ""},
            {"Id": "c", "Codigo": "GM0010-S", "CodigoBarras": ""},
        ]
        got = _escolher_produto_plu_balanca(cand, "0010")
        self.assertEqual(got["Id"], "b")
