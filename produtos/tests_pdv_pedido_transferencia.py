"""Pedido de transferência PDV — testes sem banco (arquivo + util)."""
from __future__ import annotations

from decimal import Decimal
from pathlib import Path

from django.test import SimpleTestCase

from estoque.models import SolicitacaoTransferencia
from produtos.pdv_pedido_transferencia_util import outra_loja, serializar

ROOT = Path(__file__).resolve().parents[1]


class PedidoTransferenciaPdvPathTests(SimpleTestCase):
    def test_outra_loja(self):
        self.assertEqual(outra_loja("centro"), "vila")
        self.assertEqual(outra_loja("vila"), "centro")

    def test_serializar_papel(self):
        row = SolicitacaoTransferencia(
            produto_externo_id="X1",
            nome_produto="X",
            quantidade=Decimal("1"),
            loja_origem="vila",
            loja_destino="centro",
        )
        d = serializar(row, "vila")
        self.assertTrue(d["recebido"])
        self.assertFalse(d["enviado"])

    def test_templates_e_js(self):
        wiz = (ROOT / "produtos/templates/produtos/pdv_wizard.html").read_text(
            encoding="utf-8"
        )
        js = (
            ROOT / "produtos/static/produtos/js/pdv_pedido_transferencia.js"
        ).read_text(encoding="utf-8")
        ov = (
            ROOT
            / "produtos/templates/produtos/partials/pdv/pedido_transferencia_overlay.html"
        ).read_text(encoding="utf-8")
        pdv = (ROOT / "pdv/views.py").read_text(encoding="utf-8")
        self.assertIn("pdv-topbar-pedir-loja-btn", wiz)
        self.assertIn("pedido_transferencia_overlay.html", wiz)
        self.assertIn("pdv_pedido_transferencia.js", wiz)
        self.assertIn("apiPdvPedidoTransferenciaCriar", pdv)
        self.assertIn("Enviar pedido", ov)
        self.assertIn("Transferir todos", ov)
        self.assertIn("apiPdvPedidoTransferenciaTransferir", js)
        self.assertIn("pdv-wiz-topbar-btn--pedir-pendente", js)
