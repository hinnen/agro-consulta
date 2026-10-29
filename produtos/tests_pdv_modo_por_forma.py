"""PDV-MODO-POR-FORMA — path por_forma vs tabela A/B."""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

from django.test import SimpleTestCase

from produtos.precos_forma_pagamento_util import preco_venda_para_forma

ROOT = Path(__file__).resolve().parents[1]
JS = ROOT / "produtos" / "static" / "produtos" / "js" / "precos_forma_pagamento.js"
STATE = ROOT / "produtos" / "static" / "produtos" / "js" / "pdv_state.js"
CAT = ROOT / "produtos" / "catalogo_agro.py"


class PdvModoPorFormaTests(SimpleTestCase):
    def test_js_respeita_por_forma_explicito(self):
        js = JS.read_text(encoding="utf-8")
        self.assertIn("if (m === 'por_forma' || m === 'forma' || m === 'porforma')", js)
        self.assertIn("item.precos_modo === 'grupos' && pg", js)
        i_por = js.find("if (m === 'por_forma'")
        i_gt = js.find("if (gruposTemDados(precosGruposDoItem(item))) return 'grupos';")
        self.assertGreater(i_por, 0)
        self.assertGreater(i_gt, i_por)

    def test_pdv_state_nao_forca_grupos(self):
        st = STATE.read_text(encoding="utf-8")
        self.assertNotIn("else if (existing.precos_grupos)", st)
        self.assertNotIn("else if (novo.precos_grupos)", st)
        self.assertNotIn("if (!row.precos_modo) row.precos_modo = 'grupos'", st)
        self.assertIn("copiarPrecosPorFormaDoProduto", st)

    def test_catalogo_nao_forca_grupos(self):
        cat = CAT.read_text(encoding="utf-8")
        self.assertNotIn('if pg and modo != "grupos":', cat)
        self.assertIn('if modo == "grupos" and pg:', cat)

    def test_backend_por_forma_ignora_lixo_ab(self):
        g = {
            "preco_a": 50.0,
            "preco_b": 60.0,
            "formas_a": ["PIX"],
            "formas_b": ["Dinheiro"],
        }
        ppf = {"PIX": 87.0}
        self.assertAlmostEqual(
            preco_venda_para_forma(
                99, ppf, "PIX", precos_modo="por_forma", precos_grupos=g
            ),
            87.0,
            places=2,
        )

    def test_verify_python(self):
        r = subprocess.run(
            [sys.executable, str(ROOT / "scripts" / "verify_pdv_modo_por_forma.py")],
            cwd=str(ROOT),
            capture_output=True,
            text=True,
            timeout=90,
            check=False,
        )
        self.assertEqual(r.returncode, 0, (r.stdout or "") + (r.stderr or ""))

    def test_verify_node(self):
        r = subprocess.run(
            ["node", str(ROOT / "scripts" / "verify_pdv_modo_por_forma.js")],
            cwd=str(ROOT),
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
        self.assertEqual(r.returncode, 0, (r.stdout or "") + (r.stderr or ""))
