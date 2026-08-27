import copy
from decimal import Decimal
from unittest.mock import patch

from django.test import SimpleTestCase

from produtos.nfe_entrada_util import (
    _titulos_entrada_nfe_ids_do_rascunho,
    sanear_carimbo_financeiro_falso_rascunho,
    validar_vinculo_financeiro_entrada_nfe,
)
from produtos.tests_entrada_nf_reabertura_estoque import FakeCollection, RID, _doc


def nota(ids=None):
    d = _doc({
        "financeiro_lancado": True,
        "financeiro_ids": list(ids or ["fin-16266-1", "fin-16266-2"]),
        "financeiro_lancado_em": "2026-08-27T12:00:00+00:00",
        "financeiro_lote": "AGABCDEF12",
        "financeiro_ui": {
            "parcelas_manual": [
                {"data_vencimento": "2026-09-09", "valor": "1070.20", "boleto_codigo_barras": "B1"},
                {"data_vencimento": "2026-09-16", "valor": "1070.20", "boleto_codigo_barras": "B2"},
            ]
        },
        "estoque_agro_ajuste_ids": [901, 902],
        "estoque_aplicado_em": "2026-08-27T11:00:00+00:00",
    }, status="estoque_aplicado")
    d["cabecalho"].update({
        "numero": "16266", "serie": "2", "emit_nome": "IBUNA ALIMENTOS",
        "emit_fornecedor_id": "forn-ibuna", "emit_cnpj": "12345678000199",
        "chave": "35260812345678000199550020000162661234567890",
        "valor_total": "2140.40",
    })
    return d


def titulo(tid, nf="16266", valor="1070.20", venc="2026-09-09", cliente_id="forn-ibuna", cliente="IBUNA ALIMENTOS"):
    return {
        "_id": tid, "Cliente": cliente, "ClienteID": cliente_id,
        "Descricao": f"Compra mercadoria NF {nf} (parcela)",
        "Observacao": "Entrada NF-e Agro",
        "NumeroDocumento": "AGABCDEF12-01",
        "ValorBruto": valor, "DataVencimento": venc, "Despesa": True,
    }


class EntradaNfFinanceiroVinculoTests(SimpleTestCase):
    def test_flag_verdadeira_com_id_inexistente(self):
        out = validar_vinculo_financeiro_entrada_nfe(nota(["sumiu"]), [], ["sumiu"])
        self.assertFalse(out["valido"])
        self.assertEqual(out["motivo"], "id_inexistente")

    def test_id_existente_de_outra_nf_e_fornecedor_igual_nao_batem(self):
        doc = nota(["outro"])
        out = validar_vinculo_financeiro_entrada_nfe(doc, [titulo("outro", nf="15789")], ["outro"])
        self.assertFalse(out["valido"])

    def test_id_correto_da_mesma_nf_com_assinatura_exata(self):
        doc = nota()
        titulos = [titulo("fin-16266-1"), titulo("fin-16266-2", venc="2026-09-16")]
        out = validar_vinculo_financeiro_entrada_nfe(doc, titulos, doc["extra"]["financeiro_ids"])
        self.assertTrue(out["valido"])
        self.assertTrue(out["parcelas_ok"])

    def test_numero_parecido_contendo_digitos_nao_bate(self):
        doc = nota(["parecido"])
        for nf in ("116266", "162660", "2162669"):
            with self.subTest(nf=nf):
                self.assertFalse(validar_vinculo_financeiro_entrada_nfe(doc, [titulo("parecido", nf=nf)], ["parecido"])["valido"])

    def test_limpeza_somente_vinculo_falso_preserva_ui_estoque_e_titulo(self):
        doc = nota(["6a109ba2177a722ee5928a56"])
        col = FakeCollection(doc)
        antes_ui = copy.deepcopy(doc["extra"]["financeiro_ui"])
        antes_estoque = {k: copy.deepcopy(doc["extra"][k]) for k in ("estoque_agro_ajuste_ids", "estoque_aplicado_em")}
        titulo_alheio = titulo("6a109ba2177a722ee5928a56", nf="15789", valor="999.99")
        with (
            patch("produtos.nfe_entrada_util._entrada_nota_rascunho_store", return_value=col),
            patch("produtos.nfe_entrada_util._object_id_rascunho", return_value=RID),
            patch("produtos.nfe_entrada_util._entrada_nfe_financeiro_titulos_por_ids", return_value=[titulo_alheio]),
        ):
            out = sanear_carimbo_financeiro_falso_rascunho(None, doc, usuario="teste")
        for campo in ("financeiro_lancado", "financeiro_ids", "financeiro_lancado_em", "financeiro_lote"):
            self.assertNotIn(campo, out["extra"])
        self.assertEqual(out["extra"]["financeiro_ui"], antes_ui)
        for k, v in antes_estoque.items():
            self.assertEqual(out["extra"][k], v)
        self.assertEqual(titulo_alheio["Descricao"], "Compra mercadoria NF 15789 (parcela)")
        audit = out["extra"]["financeiro_vinculo_saneado_auditoria"][-1]
        self.assertEqual(audit["ids_removidos"], ["6a109ba2177a722ee5928a56"])

    def test_recarga_mantem_ja_gerada_somente_com_titulos_corretos(self):
        doc = nota()
        corretos = [titulo("fin-16266-1"), titulo("fin-16266-2", venc="2026-09-16")]
        with patch("produtos.nfe_entrada_util._entrada_nfe_financeiro_titulos_por_ids", return_value=corretos):
            self.assertEqual(_titulos_entrada_nfe_ids_do_rascunho(None, doc), doc["extra"]["financeiro_ids"])
        with (
            patch("produtos.nfe_entrada_util._entrada_nfe_financeiro_titulos_por_ids", return_value=[titulo("fin-16266-1", nf="15789")]),
            patch("produtos.nfe_entrada_util._entrada_nfe_financeiro_titulos_por_rastro", return_value=[]),
        ):
            self.assertEqual(_titulos_entrada_nfe_ids_do_rascunho(None, doc), [])

    def test_duas_parcelas_exatas_formam_assinatura_idempotente(self):
        doc = nota()
        titulos = [titulo("fin-16266-1"), titulo("fin-16266-2", venc="2026-09-16")]
        primeira = validar_vinculo_financeiro_entrada_nfe(doc, titulos, doc["extra"]["financeiro_ids"])
        segunda = validar_vinculo_financeiro_entrada_nfe(doc, titulos, doc["extra"]["financeiro_ids"])
        self.assertTrue(primeira["valido"] and segunda["valido"])
        self.assertEqual(Decimal("1070.20") + Decimal("1070.20"), Decimal("2140.40"))