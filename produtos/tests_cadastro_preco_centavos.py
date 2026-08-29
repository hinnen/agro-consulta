"""CAD-PRECO-CENTAVOS — 82,90 no cadastro nao pode virar 829,00."""
from __future__ import annotations

from pathlib import Path

from django.test import SimpleTestCase

from produtos.precos_forma_pagamento_util import (
    normalizar_precos_grupos_payload,
    normalizar_precos_por_forma_payload,
)

ROOT = Path(__file__).resolve().parents[1]
MODAL = ROOT / "produtos" / "templates" / "produtos" / "_modal_editar_produto_cadastro_erp.inc.html"


class CadastroPrecoCentavosSourceTests(SimpleTestCase):
    def test_parser_nao_apaga_ponto_de_numero(self):
        html = MODAL.read_text(encoding="utf-8")
        self.assertIn("function _parseMoedaTexto", html)
        self.assertIn("typeof s === 'number'", html)
        self.assertIn("t.indexOf(',') >= 0", html)
        self.assertNotIn(
            ".replace(/\\s/g, '').replace(/\\./g, '').replace(',', '.')",
            html,
        )

    def test_backend_mantem_centavos_no_payload(self):
        out = normalizar_precos_por_forma_payload({"PIX": 82.9})
        self.assertAlmostEqual(out["PIX"], 82.9, places=2)
        grupos = normalizar_precos_grupos_payload(
            {"preco_a": 82.9, "preco_b": 92, "formas_a": ["PIX"], "formas_b": ["Fiado"]}
        )
        self.assertIsNotNone(grupos)
        self.assertAlmostEqual(grupos["preco_a"], 82.9, places=2)
