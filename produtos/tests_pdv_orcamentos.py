"""Orçamentos do PDV — chave de cliente e JSON da lista."""
from __future__ import annotations

from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

from django.test import SimpleTestCase

ROOT = Path(__file__).resolve().parents[1]

from produtos.pdv_orcamento_util import (
    ORCAMENTO_CLIENTE_KEY_CONSUMIDOR,
    carimbar_entry_orcamento_pdv,
    nome_eh_consumidor_final,
    normalizar_orcamento_cliente_key,
)


class NormalizarClienteKeyTests(SimpleTestCase):
    def test_consumidor_por_nome(self):
        self.assertTrue(nome_eh_consumidor_final("CONSUMIDOR NÃO IDENTIFICADO..."))
        self.assertEqual(
            normalizar_orcamento_cliente_key("tmp:consumidor não identificado...:", "CONSUMIDOR NÃO IDENTIFICADO..."),
            ORCAMENTO_CLIENTE_KEY_CONSUMIDOR,
        )

    def test_vazio_vira_consumidor(self):
        self.assertEqual(normalizar_orcamento_cliente_key("", ""), ORCAMENTO_CLIENTE_KEY_CONSUMIDOR)
        self.assertEqual(normalizar_orcamento_cliente_key("null", ""), ORCAMENTO_CLIENTE_KEY_CONSUMIDOR)

    def test_pk_permanece(self):
        self.assertEqual(normalizar_orcamento_cliente_key("pk:42", "Maria"), "pk:42")

    def test_wizard_js_grava_em_memoria_antes_de_renderizar(self):
        js = (ROOT / "produtos/static/produtos/js/pdv_wizard.js").read_text(encoding="utf-8")
        self.assertIn("historicoOrcamentosMem", js)
        self.assertIn("canonicalizeOrcamentoEntry", js)
        salvar = js.split("function salvarOrcamentoWizard", 1)[1]
        self.assertLess(
            salvar.find("writeHistoricoOrcamentos(historico)"),
            salvar.find("renderRecentBudgetsSnippet()"),
        )
        self.assertIn("_orcamentoSyncSeq += 1", salvar)

    def test_payload_vazio_nao_ganha_do_modelo(self):
        entry = carimbar_entry_orcamento_pdv(
            {"cliente_key": "", "cliente": "CONSUMIDOR NÃO IDENTIFICADO...", "itens": [{"id": "1"}]},
            orc_local_id=1755960000123,
            cliente_key="consumidor_final",
            cliente_nome="CONSUMIDOR NÃO IDENTIFICADO...",
            total_texto="R$ 1,30",
            criado_em=datetime(2026, 8, 23, 13, 13, 0),
        )
        self.assertEqual(entry["id"], 1755960000123)
        self.assertEqual(entry["cliente_key"], ORCAMENTO_CLIENTE_KEY_CONSUMIDOR)
        self.assertEqual(entry["orc_barcode"], "GMORC1755960000123")
        self.assertEqual(entry["total"], "R$ 1,30")
        self.assertIn("13:13", entry["data"])


class EntryFromModelViewsTests(SimpleTestCase):
    def test_carimbo_sobrescreve_chave_vazia_do_payload(self):
        from produtos.views import _orcamento_pdv_entry_from_model

        obj = SimpleNamespace(
            orc_local_id=99,
            payload_json={"cliente_key": "", "cliente": "CONSUMIDOR NÃO IDENTIFICADO...", "itens": [{"nome": "teste"}]},
            cliente_key="consumidor_final",
            cliente_nome="CONSUMIDOR NÃO IDENTIFICADO...",
            cliente_mode="cliente",
            total_texto="R$ 1,30",
            entrega=False,
            forma_pagamento="",
            usuario_registro="RENAN",
            criado_em=datetime(2026, 8, 23, 13, 13, 0),
        )
        entry = _orcamento_pdv_entry_from_model(obj)
        self.assertEqual(entry["cliente_key"], "consumidor_final")
        self.assertEqual(entry["id"], 99)
        self.assertEqual(entry["usuario"], "RENAN")
