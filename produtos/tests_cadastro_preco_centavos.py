"""CAD-PRECO-CENTAVOS — path 82,90 nao vira 829,00."""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

from django.test import SimpleTestCase

from produtos.precos_forma_pagamento_util import (
    normalizar_precos_grupos_payload,
    normalizar_precos_por_forma_payload,
    preco_venda_para_forma,
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
        self.assertIn("commitPrecoGrupoCampo", html)

    def test_backend_mantem_centavos_no_payload(self):
        out = normalizar_precos_por_forma_payload({"PIX": 82.9, "Cartão de débito": 87})
        self.assertAlmostEqual(out["PIX"], 82.9, places=2)
        self.assertAlmostEqual(out["Cartão de débito"], 87.0, places=2)
        grupos = normalizar_precos_grupos_payload(
            {"preco_a": 82.9, "preco_b": 92, "formas_a": ["PIX"], "formas_b": ["Fiado"]}
        )
        self.assertIsNotNone(grupos)
        self.assertAlmostEqual(grupos["preco_a"], 82.9, places=2)

    def test_pdv_aplica_82_90_por_forma_e_grupos(self):
        ppf = normalizar_precos_por_forma_payload({"PIX": 82.9})
        self.assertAlmostEqual(preco_venda_para_forma(99, ppf, "PIX"), 82.9, places=2)
        self.assertAlmostEqual(preco_venda_para_forma(99, ppf, "Dinheiro"), 99.0, places=2)
        g = normalizar_precos_grupos_payload(
            {"preco_a": 82.9, "preco_b": 92, "formas_a": ["PIX"], "formas_b": ["Fiado"]}
        )
        self.assertAlmostEqual(
            preco_venda_para_forma(99, None, "PIX", precos_modo="grupos", precos_grupos=g),
            82.9,
            places=2,
        )

    def test_path_python(self):
        r = subprocess.run(
            [sys.executable, str(ROOT / "scripts" / "verify_cadastro_preco_centavos.py")],
            cwd=str(ROOT),
            capture_output=True,
            text=True,
            timeout=60,
            check=False,
        )
        self.assertEqual(r.returncode, 0, (r.stdout or "") + (r.stderr or ""))

    def test_path_node_js_real(self):
        r = subprocess.run(
            ["node", str(ROOT / "scripts" / "verify_cadastro_preco_centavos.js")],
            cwd=str(ROOT),
            capture_output=True,
            text=True,
            timeout=20,
            check=False,
        )
        self.assertEqual(r.returncode, 0, (r.stdout or "") + (r.stderr or ""))
