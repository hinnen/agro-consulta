import copy
import json
from datetime import date
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import patch

from django.test import RequestFactory, SimpleTestCase

from produtos.nfe_entrada_util import (
    _titulos_entrada_nfe_ids_do_rascunho,
    obter_rascunho_entrada,
    sanear_carimbo_financeiro_falso_rascunho,
    sincronizar_financeiro_rascunho_entrada_nfe,
    validar_vinculo_financeiro_entrada_nfe,
)
from produtos.tests_entrada_nf_reabertura_estoque import FakeCollection, RID, _doc
from produtos.views import api_entrada_nota_financeiro


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
    def _nota_manual(self):
        d = _doc({
            "financeiro_ui": {
                "parcelas_manual": [
                    {"data_vencimento": "2026-09-10", "valor": "318.59"},
                    {"data_vencimento": "2026-09-21", "valor": "318.59"},
                    {"data_vencimento": "2026-09-30", "valor": "318.59"},
                ]
            },
        }, status="encerrada")
        d["extra"].pop("financeiro_lancado", None)
        d["extra"].pop("financeiro_ids", None)
        d["extra"].pop("financeiro_lote", None)
        d["cabecalho"].update({
            "numero": "51832423432",
            "serie": "",
            "emit_nome": "Sn - Pajaro",
            "emit_fornecedor_id": "",
            "emit_cnpj": "",
            "chave": "",
        })
        titulos = [
            titulo(
                "pg-1",
                nf="51832423432",
                valor="318.59",
                venc=date(2026, 9, 10),
                cliente_id="",
                cliente="Sn - Pajaro",
            ),
            titulo(
                "pg-2",
                nf="51832423432",
                valor="318.59",
                venc=date(2026, 9, 21),
                cliente_id="",
                cliente="Sn - Pajaro",
            ),
            titulo(
                "pg-3",
                nf="51832423432",
                valor="318.59",
                venc=date(2026, 9, 30),
                cliente_id="",
                cliente="Sn - Pajaro",
            ),
        ]
        return d, titulos, ["pg-1", "pg-2", "pg-3"]

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

    def test_nota_manual_sem_chave_casa_por_nf_e_nome(self):
        """Bug loja: CP já lançado, etapa 7 laranja — nota manual perde chave/lote/flag."""
        d, titulos, ids = self._nota_manual()
        out = validar_vinculo_financeiro_entrada_nfe(d, titulos, ids)
        self.assertTrue(out["valido"], out)
        with (
            patch("produtos.nfe_entrada_util._entrada_nfe_financeiro_titulos_por_ids", return_value=[]),
            patch("produtos.nfe_entrada_util._entrada_nfe_financeiro_titulos_por_rastro", return_value=titulos),
        ):
            self.assertEqual(_titulos_entrada_nfe_ids_do_rascunho(None, d), ids)

    def test_sincronizar_religa_flag_sem_duplicar(self):
        d, titulos, ids = self._nota_manual()
        col = FakeCollection(d)
        with (
            patch("produtos.nfe_entrada_util._entrada_nota_rascunho_store", return_value=col),
            patch("produtos.nfe_entrada_util._object_id_rascunho", return_value=RID),
            patch("produtos.nfe_entrada_util._entrada_nfe_financeiro_titulos_por_ids", return_value=[]),
            patch("produtos.nfe_entrada_util._entrada_nfe_financeiro_titulos_por_rastro", return_value=titulos),
        ):
            out = sincronizar_financeiro_rascunho_entrada_nfe(None, RID, usuario="teste")
        self.assertTrue(out["ok"])
        self.assertTrue(out["sincronizado"])
        self.assertEqual(out["ids"], ids)
        self.assertTrue(col.doc["extra"].get("financeiro_lancado"))
        self.assertEqual(col.doc["extra"].get("financeiro_ids"), ids)
        with (
            patch("produtos.nfe_entrada_util._entrada_nota_rascunho_store", return_value=col),
            patch("produtos.nfe_entrada_util._object_id_rascunho", return_value=RID),
            patch("produtos.nfe_entrada_util._entrada_nfe_financeiro_titulos_por_ids", return_value=titulos),
            patch("produtos.nfe_entrada_util._entrada_nfe_financeiro_titulos_por_rastro", return_value=titulos),
        ):
            segunda = sincronizar_financeiro_rascunho_entrada_nfe(None, RID, usuario="teste")
        self.assertTrue(segunda["ok"])
        self.assertTrue(segunda["ja_marcado"])
        self.assertFalse(segunda["sincronizado"])

    def test_obter_rascunho_expõe_financeiro_lancado_apos_religa(self):
        d, titulos, ids = self._nota_manual()
        col = FakeCollection(d)
        with (
            patch("produtos.nfe_entrada_util._entrada_nota_rascunho_store", return_value=col),
            patch("produtos.nfe_entrada_util._object_id_rascunho", return_value=RID),
            patch("produtos.nfe_entrada_util._entrada_nfe_financeiro_titulos_por_ids", return_value=[]),
            patch("produtos.nfe_entrada_util._entrada_nfe_financeiro_titulos_por_rastro", return_value=titulos),
            patch("produtos.nfe_entrada_util._enriquecer_linhas_gm_ean_catalogo", side_effect=lambda linhas: linhas),
        ):
            out = obter_rascunho_entrada(None, RID)
        self.assertTrue(out["entrada_financeiro_lancado"])
        self.assertEqual(out["extra"]["financeiro_ids"], ids)

    def test_api_financeiro_religa_200_nao_insere(self):
        d, titulos, ids = self._nota_manual()
        col = FakeCollection(d)
        factory = RequestFactory()
        request = factory.post(
            "/api/entrada-nota/financeiro/",
            data=json.dumps(
                {
                    "rascunho_id": RID,
                    "cabecalho": d["cabecalho"],
                    "linhas": d["linhas"],
                    "financeiro": {"data_competencia": "2026-09-03", "data_vencimento": "2026-09-10"},
                }
            ),
            content_type="application/json",
        )
        request.user = SimpleNamespace(
            is_authenticated=True, email="teste@local", pk=1, get_username=lambda: "teste"
        )
        with (
            patch("produtos.views._entrada_nfe_conexao", return_value=(SimpleNamespace(col_c="DtoPessoa"), object())),
            patch("produtos.views._entrada_nfe_rascunho_db_ok", return_value=True),
            patch("produtos.views._entrada_nota_rascunho_store", return_value=col),
            patch("produtos.views._object_id_rascunho", return_value=RID),
            patch("produtos.views.normalizar_cabecalho_emit_fornecedor_entrada_nfe", side_effect=lambda db, colp, cab: cab),
            patch("produtos.nfe_entrada_util._entrada_nota_rascunho_store", return_value=col),
            patch("produtos.nfe_entrada_util._object_id_rascunho", return_value=RID),
            patch("produtos.nfe_entrada_util._entrada_nfe_financeiro_titulos_por_ids", return_value=[]),
            patch("produtos.nfe_entrada_util._entrada_nfe_financeiro_titulos_por_rastro", return_value=titulos),
            patch("produtos.lancamentos_financeiro_pg_write_util.inserir_lancamentos_manual_lote_dispatch") as inserir,
        ):
            response = api_entrada_nota_financeiro(request)
        self.assertEqual(response.status_code, 200, response.content)
        body = json.loads(response.content)
        self.assertTrue(body["ok"])
        self.assertTrue(body["financeiro"]["ok"])
        self.assertEqual(body["financeiro"]["ids"], ids)
        inserir.assert_not_called()

    def test_api_financeiro_sem_titulo_continua_403(self):
        d, _titulos, _ids = self._nota_manual()
        col = FakeCollection(d)
        factory = RequestFactory()
        request = factory.post(
            "/api/entrada-nota/financeiro/",
            data=json.dumps(
                {
                    "rascunho_id": RID,
                    "cabecalho": d["cabecalho"],
                    "linhas": d["linhas"],
                    "financeiro": {"data_competencia": "2026-09-03", "data_vencimento": "2026-09-10"},
                }
            ),
            content_type="application/json",
        )
        request.user = SimpleNamespace(
            is_authenticated=True, email="teste@local", pk=1, get_username=lambda: "teste"
        )
        with (
            patch("produtos.views._entrada_nfe_conexao", return_value=(SimpleNamespace(col_c="DtoPessoa"), object())),
            patch("produtos.views._entrada_nfe_rascunho_db_ok", return_value=True),
            patch("produtos.views._entrada_nota_rascunho_store", return_value=col),
            patch("produtos.views._object_id_rascunho", return_value=RID),
            patch("produtos.views.normalizar_cabecalho_emit_fornecedor_entrada_nfe", side_effect=lambda db, colp, cab: cab),
            patch("produtos.nfe_entrada_util._entrada_nota_rascunho_store", return_value=col),
            patch("produtos.nfe_entrada_util._object_id_rascunho", return_value=RID),
            patch("produtos.nfe_entrada_util._entrada_nfe_financeiro_titulos_por_ids", return_value=[]),
            patch("produtos.nfe_entrada_util._entrada_nfe_financeiro_titulos_por_rastro", return_value=[]),
        ):
            response = api_entrada_nota_financeiro(request)
        self.assertEqual(response.status_code, 403)
        self.assertIn("não achei conta a pagar", json.loads(response.content)["erro"])
