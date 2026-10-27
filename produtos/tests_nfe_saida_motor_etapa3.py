"""Etapa 3 — motor NF-e 55 lê só regras Validadas. Sem transmissão."""
from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal

from django.conf import settings
from django.contrib.auth.models import User
from django.test import TestCase


class _DisableMigrations(dict):
    def __contains__(self, item):
        return True

    def __getitem__(self, item):
        return None


settings.MIGRATION_MODULES = _DisableMigrations()

from produtos.models import (
    NfeAliquotaCreditoSn,
    NfeEstabelecimentoFiscal,
    NfeFiscalStatus,
    NfeRegraOperacao,
)
from produtos.nfe_saida_regras_validas_util import resolver_cnpj_serie_emitente
from produtos.nfe_saida_tributacao_util import apurar_nfe_saida
from produtos.nfe_saida_util import doc_novo_vazio


def _doc(
    *,
    uf="SP",
    cidade="Jacupiranga",
    cmun="3524600",
    finalidade="consumo",
    documento="39053344705",
    ie="",
    cfop="5102",
    csosn="102",
    ncm="12345678",
    estab="centro",
    tipo_dest="",
):
    doc = doc_novo_vazio()
    doc["estabelecimento"] = estab
    dest = {
        "nome": "Cliente",
        "documento": documento,
        "uf": uf,
        "cidade": cidade,
        "cmun": cmun,
        "logradouro": "Rua A",
        "numero": "1",
        "finalidade_mercadoria": finalidade,
    }
    if ie:
        dest["ie"] = ie
    if tipo_dest:
        dest["tipo_destinatario"] = tipo_dest
    doc["destinatario"].update(dest)
    doc["itens"] = [
        {
            "descricao": "Item",
            "qtd": 1,
            "v_unit": 100,
            "v_total": 100,
            "ncm": ncm,
            "cfop": cfop,
            "csosn": csosn,
            "cst_pis": "49",
            "cst_cofins": "49",
        }
    ]
    return doc


def _regra(**kw):
    dados = dict(
        estabelecimento="centro",
        ambito="interna",
        tipo_destinatario="consumidor",
        escopo=NfeRegraOperacao.Escopo.GERAL,
        cfop="5102",
        csosn="102",
        cst_pis="49",
        cst_cofins="49",
        status=NfeFiscalStatus.VALIDADO,
        contador_nome="Contador Teste",
        contador_crc="CRC-SP 1",
        contador_aprovado_em=date(2026, 10, 1),
    )
    dados.update(kw)
    return NfeRegraOperacao.objects.create(**dados)


class NfeSaidaMotorEtapa3Tests(TestCase):
    def setUp(self):
        User.objects.create_user("adm", "a@t.local", "x", is_staff=True)

    def test_interna_consumidor_regra_validada(self):
        _regra()
        ap = apurar_nfe_saida(_doc(), uf_emit="SP")
        self.assertFalse(ap.bloqueios, ap.bloqueios)
        self.assertEqual(ap.itens[0].csosn, "102")
        self.assertEqual(ap.itens[0].icms_grupo, "ICMSSN102")
        self.assertNotIn("vICMS", ap.itens[0].icms_campos)

    def test_interna_revenda(self):
        _regra(tipo_destinatario="revenda", csosn="102")
        ap = apurar_nfe_saida(_doc(finalidade="revenda"), uf_emit="SP")
        self.assertFalse(ap.bloqueios, ap.bloqueios)
        self.assertEqual(ap.itens[0].csosn, "102")

    def test_produtor_rural_cpf_ie(self):
        _regra(tipo_destinatario="produtor_rural")
        ap = apurar_nfe_saida(
            _doc(documento="39053344705", ie="1234567890", tipo_dest="produtor_rural", finalidade="produtor_rural"),
            uf_emit="SP",
        )
        self.assertFalse(ap.bloqueios, ap.bloqueios)

    def test_consumidor_cnpj_nao_vira_revenda_sozinho(self):
        _regra(tipo_destinatario="consumidor")
        ap = apurar_nfe_saida(
            _doc(documento="11222333000181", ie="ISENTO", finalidade="uso_proprio"),
            uf_emit="SP",
        )
        self.assertFalse(ap.bloqueios, ap.bloqueios)
        self.assertEqual(ap.destinatario["ind_final"], 1)

    def test_interestadual_exige_cfop_6(self):
        _regra(ambito="interestadual", cfop="6102")
        ap = apurar_nfe_saida(
            _doc(uf="RJ", cidade="Rio de Janeiro", cmun="3304557", cfop="6102"),
            uf_emit="SP",
        )
        self.assertFalse(ap.bloqueios, ap.bloqueios)
        self.assertEqual(ap.itens[0].cfop, "6102")

    def test_csosn_101_credito_competencia(self):
        _regra(tipo_destinatario="revenda", csosn="101")
        NfeAliquotaCreditoSn.objects.create(
            estabelecimento="centro",
            competencia="2026-10",
            percentual=Decimal("1.2500"),
            vigencia_inicio=date(2026, 10, 1),
            vigencia_fim=date(2026, 10, 31),
            status=NfeFiscalStatus.VALIDADO,
        )
        doc = _doc(finalidade="revenda", csosn="101")
        ap = apurar_nfe_saida(doc, uf_emit="SP", data_emissao=date(2026, 10, 27))
        self.assertFalse(ap.bloqueios, ap.bloqueios)
        self.assertEqual(ap.itens[0].icms_campos.get("pCredSN"), "1.2500")
        self.assertEqual(ap.itens[0].icms_campos.get("vCredICMSSN"), "1.25")

    def test_csosn_101_nao_usa_outro_mes(self):
        _regra(tipo_destinatario="revenda", csosn="101")
        NfeAliquotaCreditoSn.objects.create(
            estabelecimento="centro",
            competencia="2026-09",
            percentual=Decimal("3.0000"),
            status=NfeFiscalStatus.VALIDADO,
        )
        ap = apurar_nfe_saida(_doc(finalidade="revenda", csosn="101"), uf_emit="SP", data_emissao=date(2026, 10, 27))
        self.assertTrue(any("2026-10" in b for b in ap.bloqueios))

    def test_sem_regra_validada_bloqueia(self):
        ap = apurar_nfe_saida(_doc(), uf_emit="SP")
        self.assertTrue(any("sem regra Validada" in b for b in ap.bloqueios))

    def test_regra_vencida(self):
        _regra(vigencia_inicio=date(2026, 1, 1), vigencia_fim=date(2026, 1, 31))
        ap = apurar_nfe_saida(_doc(), uf_emit="SP", data_emissao=date(2026, 10, 27))
        self.assertTrue(any("vigência" in b.lower() or "sem regra" in b for b in ap.bloqueios))

    def test_regra_outro_estabelecimento(self):
        _regra(estabelecimento="vila")
        ap = apurar_nfe_saida(_doc(estab="centro"), uf_emit="SP")
        self.assertTrue(any("sem regra Validada" in b for b in ap.bloqueios))

    def test_conflito_mesmo_nivel(self):
        _regra(cfop="5102", csosn="102")
        _regra(cfop="5101", csosn="102")
        ap = apurar_nfe_saida(_doc(cfop="5102"), uf_emit="SP")
        self.assertTrue(any("conflitantes" in b for b in ap.bloqueios))

    def test_municipio_diferente_sem_ibge(self):
        _regra()
        ap = apurar_nfe_saida(_doc(cidade="Registro", cmun="", uf="SP"), uf_emit="SP")
        self.assertTrue(any("IBGE" in b or "cMun" in b for b in ap.bloqueios))

    def test_ncm_invalido(self):
        _regra()
        ap = apurar_nfe_saida(_doc(ncm="123"), uf_emit="SP")
        self.assertTrue(any("NCM" in b for b in ap.bloqueios))

    def test_nao_substitui_csosn_cadastro_silenciosamente(self):
        _regra(csosn="102")
        ap = apurar_nfe_saida(_doc(csosn="101"), uf_emit="SP")
        self.assertTrue(any("incompatível" in b for b in ap.bloqueios))

    def test_vila_nao_usa_serie_centro(self):
        from unittest.mock import patch

        NfeEstabelecimentoFiscal.objects.create(estabelecimento="centro", serie_nfe_55=1, crt="1")
        with patch(
            "produtos.nfce_config_util.nfce_cfg",
            return_value={"cnpj": "22333444000155"},
        ):
            cnpj, serie, err = resolver_cnpj_serie_emitente({"estabelecimento": "vila"}, cnpj_pedido="")
        self.assertIsNone(serie)
        self.assertTrue(err)
        self.assertIn("Vila", err)

    def test_serie_navegador_divergente(self):
        from unittest.mock import patch

        NfeEstabelecimentoFiscal.objects.create(estabelecimento="centro", serie_nfe_55=7, crt="1")
        with patch(
            "produtos.nfce_config_util.nfce_cfg",
            return_value={"cnpj": "11222333000181"},
        ):
            _cnpj, _serie, err = resolver_cnpj_serie_emitente(
                {"estabelecimento": "centro", "fiscal": {"serie": 1}},
                cnpj_pedido="11222333000181",
            )
        self.assertTrue(err)
        self.assertIn("Série", err)
