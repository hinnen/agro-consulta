from unittest.mock import patch

from django.test import SimpleTestCase, TestCase

from produtos.dfe_inbox_util import dfe_executar_consulta_e_gravar, dfe_inbox_listar
from produtos.sefaz_dfe_client import dfe_cnpjs_interessados


CNPJ_CENTRO = "48900774000103"
CNPJ_VILA = "48900774000286"


class DfeCnpjsInteressadosTests(SimpleTestCase):
    def test_inclui_filial_vila_mesma_raiz(self):
        with patch(
            "produtos.sefaz_dfe_client._cfg_dist_dfe",
            return_value={"cnpj": CNPJ_CENTRO},
        ):
            cnpjs = dfe_cnpjs_interessados()
        self.assertIn(CNPJ_CENTRO, cnpjs)
        self.assertIn(CNPJ_VILA, cnpjs)

    def test_nao_mistura_raiz_diferente_sem_env(self):
        with (
            patch(
                "produtos.sefaz_dfe_client._cfg_dist_dfe",
                return_value={"cnpj": "12345678000199"},
            ),
            patch("produtos.sefaz_dfe_client.config", return_value=""),
        ):
            cnpjs = dfe_cnpjs_interessados()
        self.assertEqual(cnpjs, ["12345678000199"])

    def test_buscar_consulta_centro_e_vila(self):
        chamados: list[str] = []

        def interesse(_ult, cnpj=None):
            chamados.append(str(cnpj or ""))
            return {
                "ok": True,
                "c_stat": 137,
                "x_motivo": "Nenhum documento",
                "ult_nsu": "000000000000001",
                "max_nsu": "000000000000001",
                "notas_xml": [],
            }

        with (
            patch("produtos.sefaz_dfe_client.distribuicao_dfe_configurada", return_value=True),
            patch(
                "produtos.sefaz_dfe_client._cfg_dist_dfe",
                return_value={"cnpj": CNPJ_CENTRO},
            ),
            patch(
                "produtos.sefaz_dfe_client.nfe_distribuicao_dfe_interesse",
                side_effect=interesse,
            ),
            patch("produtos.nfe_entrada_util.obter_ult_nsu", return_value="0"),
            patch("produtos.nfe_entrada_util.gravar_ult_nsu"),
            patch("produtos.dfe_inbox_util.dfe_completar_resumos_pendentes", return_value={}),
            patch(
                "produtos.dfe_inbox_util.dfe_inbox_upsert_xmls",
                return_value={"novas": 0, "atualizadas": 0, "resumos": 0},
            ),
            patch("produtos.dfe_inbox_util.dfe_inbox_listar", return_value=[]),
        ):
            out = dfe_executar_consulta_e_gravar(origem="manual")

        self.assertTrue(out["ok"])
        self.assertEqual(chamados, [CNPJ_CENTRO, CNPJ_VILA])
        lojas = {p["cnpj"] for p in out.get("por_cnpj") or []}
        self.assertEqual(lojas, {CNPJ_CENTRO, CNPJ_VILA})


class DfeInboxListaFiliaisTests(TestCase):
    def test_lista_junta_notas_das_duas_lojas(self):
        from produtos.models import AgroNfeDistDfeDocumento
        AgroNfeDistDfeDocumento.objects.create(
            cnpj=CNPJ_CENTRO,
            chave="35260848900774000103550010000012341000012345",
            schema=AgroNfeDistDfeDocumento.Schema.NFE,
            xml="<nfeProc/>",
            emit_nome="Fornecedor Centro",
            numero="1",
        )
        AgroNfeDistDfeDocumento.objects.create(
            cnpj=CNPJ_VILA,
            chave="35260848900774000286550010000012341000012345",
            schema=AgroNfeDistDfeDocumento.Schema.NFE,
            xml="<nfeProc/>",
            emit_nome="Fornecedor Vila",
            numero="2",
        )
        itens = dfe_inbox_listar([CNPJ_CENTRO, CNPJ_VILA], aba="pendentes")
        nomes = {it["emit_nome"] for it in itens}
        self.assertEqual(nomes, {"Fornecedor Centro", "Fornecedor Vila"})
        lojas = {it["loja"] for it in itens}
        self.assertEqual(lojas, {"Centro", "Vila Elias"})
