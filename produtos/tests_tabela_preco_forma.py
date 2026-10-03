"""TABELA-PRECO-FORMA — testes Django."""
from __future__ import annotations

import subprocess
import sys
from decimal import Decimal
from pathlib import Path

from django.test import SimpleTestCase

from produtos.tabela_preco_forma_util import (
    arredondar_dezena_centavos,
    preco_com_percentual,
    regra_promo_vs_tabela,
)

ROOT = Path(__file__).resolve().parents[1]


class TabelaPrecoFormaTests(SimpleTestCase):
    def test_arredondamento(self):
        self.assertEqual(arredondar_dezena_centavos("10.43"), Decimal("10.40"))
        self.assertEqual(arredondar_dezena_centavos("10.45"), Decimal("10.50"))

    def test_percentual(self):
        self.assertEqual(
            preco_com_percentual(10, Decimal("-5.5"), arredondar=True),
            Decimal("9.50"),
        )

    def test_regra_maior(self):
        self.assertEqual(regra_promo_vs_tabela(9, 10, "maior"), 10)

    def test_verify_script(self):
        r = subprocess.run(
            [sys.executable, str(ROOT / "scripts" / "verify_tabela_preco_forma.py")],
            cwd=str(ROOT),
            capture_output=True,
            text=True,
            timeout=90,
            check=False,
        )
        self.assertEqual(r.returncode, 0, (r.stdout or "") + (r.stderr or ""))
