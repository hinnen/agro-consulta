"""PROMO-REGRA-TABELA-SAVE — testes Django espelho (SimpleTestCase + verify script)."""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

from django.test import SimpleTestCase

ROOT = Path(__file__).resolve().parents[1]


class PromoRegraTabelaTests(SimpleTestCase):
    def test_verify_script(self):
        r = subprocess.run(
            [sys.executable, str(ROOT / "scripts/verify_promo_regra_tabela_path.py")],
            cwd=str(ROOT),
            capture_output=True,
            text=True,
            timeout=120,
            check=False,
        )
        self.assertEqual(r.returncode, 0, (r.stdout or "") + (r.stderr or ""))
