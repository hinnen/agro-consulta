from types import SimpleNamespace
from unittest.mock import patch

from django.test import SimpleTestCase, TestCase
from django.urls import reverse

from produtos.dfe_inbox_util import dfe_manifestar_ciencia_e_baixar
from produtos.models import AgroNfeDistDfeDocumento
from produtos.sefaz_dfe_client import nfe_manifestar_ciencia_operacao


CNPJ = "48900774000103"
CHAVE = "35260848900774000103550010000012341000012345"


class DfeManifestacaoClientTests(SimpleTestCase):
    def test_ciencia_monta_evento_210210_e_aceita_135(self):
        enviado = {}

        def assinar(evento, _path, _password, _id):
            from lxml import etree

            return etree.tostring(evento, encoding="unicode"), None

        resposta = """
        <soap:Envelope xmlns:soap="http://www.w3.org/2003/05/soap-envelope">
          <soap:Body><retEnvEvento xmlns="http://www.portalfiscal.inf.br/nfe">
            <cStat>128</cStat><xMotivo>Lote processado</xMotivo>
            <retEvento><infEvento>
              <cStat>135</cStat>
              <xMotivo>Evento registrado e vinculado a NF-e</xMotivo>
              <nProt>135260000000001</nProt>
            </infEvento></retEvento>
          </retEnvEvento></soap:Body>
        </soap:Envelope>
        """

        def postar(_url, **kwargs):
            enviado["url"] = _url
            enviado["xml"] = kwargs["data"].decode("utf-8")
            return SimpleNamespace(status_code=200, text=resposta)

        cfg = {
            "cert_path": "certificado.pfx",
            "cert_password": "senha",
            "cnpj": CNPJ,
            "uf": "SP",
            "tp_amb": 1,
        }
        with (
            patch("produtos.sefaz_dfe_client.distribuicao_dfe_configurada", return_value=True),
            patch("produtos.sefaz_dfe_client.dfe_bloqueio_pc_local", return_value=None),
            patch("produtos.sefaz_dfe_client._cfg_dist_dfe", return_value=cfg),
            patch("produtos.nfce_sp_emissao_util._assinar_evento_xml", side_effect=assinar),
            patch(
                "produtos.nfce_sp_emissao_util._cert_pem_temporario",
                return_value=("cert.pem", "key.pem", []),
            ),
            patch("produtos.sefaz_dfe_client.requests.post", side_effect=postar),
        ):
            out = nfe_manifestar_ciencia_operacao(CHAVE)

        self.assertTrue(out["ok"])
        self.assertEqual(out["c_stat"], "135")
        self.assertEqual(out["protocolo"], "135260000000001")
        self.assertIn("www.nfe.fazenda.gov.br/NFeRecepcaoEvento4", enviado["url"])
        self.assertIn("<tpEvento>210210</tpEvento>", enviado["xml"])
        self.assertIn("<cOrgao>91</cOrgao>", enviado["xml"])
        self.assertIn(f"<CNPJ>{CNPJ}</CNPJ>", enviado["xml"])
        self.assertIn(f"<chNFe>{CHAVE}</chNFe>", enviado["xml"])
        self.assertIn("<descEvento>Ciencia da Operacao</descEvento>", enviado["xml"])


class DfeManifestacaoFluxoTests(TestCase):
    def setUp(self):
        self.row = AgroNfeDistDfeDocumento.objects.create(
            cnpj=CNPJ,
            chave=CHAVE,
            schema=AgroNfeDistDfeDocumento.Schema.RESUMO,
            xml="<resNFe/>",
            emit_nome="Fornecedor Teste",
        )

    def test_registra_uma_vez_e_tenta_baixar_xml(self):
        def download(_chave):
            AgroNfeDistDfeDocumento.objects.filter(pk=self.row.pk).update(
                schema=AgroNfeDistDfeDocumento.Schema.NFE,
                xml="<nfeProc/>",
            )
            return {"ok": True, "c_stat": 138}

        ciencia = {
            "ok": True,
            "c_stat": "135",
            "x_motivo": "Evento registrado e vinculado a NF-e",
            "protocolo": "135260000000001",
        }
        with (
            patch(
                "produtos.sefaz_dfe_client._cfg_dist_dfe",
                return_value={"cnpj": CNPJ},
            ),
            patch(
                "produtos.sefaz_dfe_client.nfe_manifestar_ciencia_operacao",
                return_value=ciencia,
            ) as manifestar,
            patch(
                "produtos.dfe_inbox_util.dfe_executar_download_por_chave",
                side_effect=download,
            ),
            patch("produtos.dfe_inbox_util.dfe_inbox_listar", return_value=[]),
        ):
            out = dfe_manifestar_ciencia_e_baixar(self.row.pk)

        self.row.refresh_from_db()
        self.assertTrue(out["ok"])
        self.assertTrue(out["xml_completo"])
        self.assertEqual(self.row.manifestacao_status, "ciencia")
        self.assertEqual(self.row.manifestacao_protocolo, "135260000000001")
        self.assertIsNotNone(self.row.manifestacao_em)
        manifestar.assert_called_once_with(CHAVE)

    def test_ciencia_ja_registrada_nao_reenvia_evento(self):
        self.row.manifestacao_status = "ciencia"
        self.row.save(update_fields=["manifestacao_status"])
        with (
            patch(
                "produtos.sefaz_dfe_client._cfg_dist_dfe",
                return_value={"cnpj": CNPJ},
            ),
            patch(
                "produtos.sefaz_dfe_client.nfe_manifestar_ciencia_operacao"
            ) as manifestar,
            patch(
                "produtos.dfe_inbox_util.dfe_executar_download_por_chave",
                return_value={"ok": False, "aguardar_segundos": 120},
            ),
            patch("produtos.dfe_inbox_util.dfe_inbox_listar", return_value=[]),
        ):
            out = dfe_manifestar_ciencia_e_baixar(self.row.pk)

        self.assertTrue(out["ok"])
        self.assertFalse(out["xml_completo"])
        self.assertTrue(out["ciencia_reutilizada"])
        manifestar.assert_not_called()

    def test_rota_ciencia_existe(self):
        self.assertEqual(
            reverse("api_entrada_nota_dfe_inbox_ciencia", args=[self.row.pk]),
            f"/api/entrada-nota/dfe-inbox/{self.row.pk}/ciencia/",
        )
