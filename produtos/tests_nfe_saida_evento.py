"""Cancelamento e CC-e NF-e 55 — sem transmissão real."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from django.conf import settings
from django.test import SimpleTestCase


class _DisableMigrations(dict):
    def __contains__(self, item):
        return True

    def __getitem__(self, item):
        return None


settings.MIGRATION_MODULES = _DisableMigrations()

from produtos.nfe_saida_evento_util import (
    FAIXA_24H,
    FAIXA_24_480,
    FAIXA_480,
    TP_CANCEL,
    TP_CCE,
    URL_RECEPCAO_EVENTO_NFE_SP,
    aplicar_evento_no_doc,
    cnpj_da_chave,
    faixa_cancelamento_sp,
    nota_ja_cancelada,
    preparar_cancelamento,
    preparar_cce,
    transmitir_evento,
    validar_evento_xsd,
    _montar_evento,
    _xml_evento_str,
)

_CHAVE = "35261048900774000103550010000000011123456780"
_NPROT = "135260000000001"
_CFG = {"tp_amb": 2, "cnpj": "48900774000103", "cert_path": "/tmp/x.pfx", "cert_password": ""}


def _doc_aut(*, horas=1, status="autorizada"):
    dh = datetime(2026, 10, 27, 10, 0, tzinfo=timezone.utc) - timedelta(hours=horas)
    xml = (
        f'<nfeProc xmlns="http://www.portalfiscal.inf.br/nfe">'
        f"<protNFe><infProt><nProt>{_NPROT}</nProt>"
        f"<dhRecbto>{dh.strftime('%Y-%m-%dT%H:%M:%S-03:00')}</dhRecbto>"
        f"</infProt></protNFe></nfeProc>"
    )
    return {
        "status": status,
        "chave": _CHAVE,
        "protocolo": _NPROT,
        "xml_autorizado": xml,
        "tp_amb": 2,
        "eventos": [],
        "estabelecimento": "centro",
    }


class NfeSaidaEventoTests(SimpleTestCase):
    def test_xml_cancel_valida_xsd(self):
        _id, ev = _montar_evento(
            tp_evento=TP_CANCEL,
            cfg=_CFG,
            chave=_CHAVE,
            n_seq=1,
            nprot=_NPROT,
            x_just="Cancelamento de teste da nota fiscal",
        )
        xml = _xml_evento_str(ev)
        self.assertIn("<tpEvento>110111</tpEvento>", xml)
        self.assertTrue(xml.startswith("<evento") or "<evento" in xml)
        ok, det = validar_evento_xsd(xml, TP_CANCEL)
        self.assertTrue(ok, det)

    def test_xml_cce_valida_xsd(self):
        _id, ev = _montar_evento(
            tp_evento=TP_CCE,
            cfg=_CFG,
            chave=_CHAVE,
            n_seq=1,
            x_correcao="Corrige o bairro do destinatario para Centro",
        )
        xml = _xml_evento_str(ev)
        self.assertIn("<tpEvento>110110</tpEvento>", xml)
        self.assertIn("xCondUso", xml)
        ok, det = validar_evento_xsd(xml, TP_CCE)
        self.assertTrue(ok, det)

    def test_faixa_24_e_480(self):
        self.assertEqual(faixa_cancelamento_sp(_doc_aut(horas=2), agora=datetime(2026, 10, 27, 12, tzinfo=timezone.utc)), FAIXA_24H)
        d = _doc_aut(horas=30)
        agora = datetime(2026, 10, 28, 16, tzinfo=timezone.utc)
        self.assertEqual(faixa_cancelamento_sp(d, agora=agora), FAIXA_24_480)
        d2 = _doc_aut(horas=500)
        agora2 = datetime(2026, 11, 18, 10, tzinfo=timezone.utc)
        self.assertEqual(faixa_cancelamento_sp(d2, agora=agora2), FAIXA_480)

    def test_cancel_24h_ok_preparar(self):
        p = preparar_cancelamento(_doc_aut(horas=1), motivo="Mercadoria nao saiu do estabelecimento")
        self.assertTrue(p.get("ok"), p)

    def test_cancel_24_480_exige_confirmacao(self):
        d = _doc_aut(horas=48)
        p = preparar_cancelamento(d, motivo="Mercadoria nao saiu do estabelecimento", confirmar_fora_prazo=False)
        self.assertFalse(p.get("ok"))
        self.assertTrue(p.get("exige_confirmacao_fora_prazo"))
        p2 = preparar_cancelamento(d, motivo="Mercadoria nao saiu do estabelecimento", confirmar_fora_prazo=True)
        self.assertTrue(p2.get("ok"), p2)

    def test_cancel_apos_480_nao_transmite(self):
        d = _doc_aut(horas=500)
        p = preparar_cancelamento(d, motivo="Mercadoria nao saiu do estabelecimento", confirmar_fora_prazo=True)
        self.assertFalse(p.get("ok"))
        self.assertEqual(p.get("faixa"), FAIXA_480)
        self.assertIn("SIPET", p.get("erro") or "")

    def test_sefaz_aceita_e_rejeita(self):
        d = _doc_aut(horas=1)

        def sign(xml, *a, **k):
            from produtos.nfe_saida_xml_validate_util import _xml_para_validacao_xsd
            from produtos.sefaz_xml_fiscal_util import tostring_sem_prefixos
            from lxml import etree

            s = xml if isinstance(xml, str) else tostring_sem_prefixos(etree.tostring(xml, encoding="unicode"))
            return _xml_para_validacao_xsd(s).decode("utf-8"), None

        def env_ok(xml, cfg):
            self.assertIn("nfe.fazenda.sp.gov.br", URL_RECEPCAO_EVENTO_NFE_SP[2])
            self.assertNotIn("nfce.", URL_RECEPCAO_EVENTO_NFE_SP[2])
            return {"c_stat": "135", "x_motivo": "Evento registrado", "protocolo": "1" * 15, "registrado": True}, None

        def env_rej(xml, cfg):
            return {"c_stat": "501", "x_motivo": "Prazo de cancelamento superior", "protocolo": "", "registrado": False}, None

        with patch("produtos.nfe_saida_evento_util._cfg_evento", return_value=(_CFG, None)):
            with patch("produtos.nfe_saida_evento_util._assinar_evento_xml", side_effect=sign):
                ok = transmitir_evento(d, cnpj_emit="48900774000103", tp_evento=TP_CANCEL, n_seq=1, x_just="Mercadoria nao saiu do estabelecimento", enviar=env_ok)
                self.assertTrue(ok.get("ok"), ok)
                novo = aplicar_evento_no_doc(d, ok["evento"])
                self.assertEqual(novo["status"], "cancelada")
                rej = transmitir_evento(d, cnpj_emit="48900774000103", tp_evento=TP_CANCEL, n_seq=1, x_just="Mercadoria nao saiu do estabelecimento", enviar=env_rej)
                self.assertFalse(rej.get("ok"))
                self.assertNotEqual(d.get("status"), "cancelada")

    def test_cce_sequencial_e_invalida(self):
        d = _doc_aut(horas=1)
        p = preparar_cce(d, texto="abc")
        self.assertFalse(p.get("ok"))
        p2 = preparar_cce(d, texto="Corrige o endereco do destinatario")
        self.assertTrue(p2.get("ok"))
        self.assertEqual(p2["n_seq"], 1)
        d["eventos"] = [{"tp_evento": TP_CCE, "n_seq": 1, "ok": True}]
        p3 = preparar_cce(d, texto="Corrige o endereco e o complemento do destinatario")
        self.assertEqual(p3["n_seq"], 2)
        self.assertTrue(p3.get("exige_texto_consolidado"))

    def test_nao_cancela_de_novo(self):
        d = _doc_aut(horas=1, status="cancelada")
        p = preparar_cancelamento(d, motivo="Mercadoria nao saiu do estabelecimento")
        self.assertFalse(p.get("ok"))
        self.assertTrue(nota_ja_cancelada(d))

    def test_cce_em_cancelada(self):
        d = _doc_aut(horas=1, status="cancelada")
        p = preparar_cce(d, texto="Corrige o endereco do destinatario")
        self.assertFalse(p.get("ok"))

    def test_centro_vila_chave(self):
        from produtos.nfe_saida_evento_util import _cfg_evento

        self.assertEqual(cnpj_da_chave(_CHAVE), "48900774000103")
        d = _doc_aut(horas=1)
        cfg, err = _cfg_evento(d, "11111111111111")
        self.assertIsNone(cfg)
        self.assertIn("não é o da chave", err or "")
