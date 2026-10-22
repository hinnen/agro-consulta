"""PDV produtos rápidos — slots 1–9 (sem DB)."""
from __future__ import annotations

from pathlib import Path

import unittest

from produtos.pdv_rapido_util import (
    APELIDO_MAX,
    SLOTS,
    normalizar_apelido,
    normalizar_produto_id,
    slot_int,
)

ROOT = Path(__file__).resolve().parents[1]


class SlotIntTests(unittest.TestCase):
    def test_validos(self):
        self.assertEqual(slot_int(1), 1)
        self.assertEqual(slot_int("9"), 9)
        self.assertEqual(len(SLOTS), 9)

    def test_invalidos(self):
        self.assertIsNone(slot_int(0))
        self.assertIsNone(slot_int(10))
        self.assertIsNone(slot_int("x"))


class ApelidoTests(unittest.TestCase):
    def test_curto(self):
        self.assertEqual(normalizar_apelido("  Milho   Grande "), "Milho Grande")

    def test_corta(self):
        self.assertEqual(len(normalizar_apelido("A" * 80)), APELIDO_MAX)

    def test_pid(self):
        self.assertEqual(normalizar_produto_id("  abc  "), "abc")


class PathTests(unittest.TestCase):
    def test_arquivos(self):
        html = (ROOT / "produtos/templates/produtos/pdv_wizard.html").read_text(encoding="utf-8")
        step = (ROOT / "produtos/templates/produtos/partials/pdv/step_produtos.html").read_text(
            encoding="utf-8"
        )
        js = (ROOT / "produtos/static/produtos/js/pdv_rapido.js").read_text(encoding="utf-8")
        wiz = (ROOT / "produtos/static/produtos/js/pdv_wizard.js").read_text(encoding="utf-8")
        urls = (ROOT / "produtos/urls.py").read_text(encoding="utf-8")
        self.assertIn('id="pdv-rapido-overlay"', html)
        self.assertIn("pdv_rapido.js", html)
        self.assertIn("90vw", html)
        self.assertIn('id="pdv-topbar-rapido-btn"', html)
        self.assertIn('id="pdv-btn-rapido"', step)
        self.assertIn("F11", js)
        self.assertIn("AgroPdvAddProductById", js)
        self.assertIn("window.AgroPdvAddProductById", wiz)
        self.assertIn("api_pdv_rapido_slots", urls)
        self.assertIn("api_pdv_rapido_salvar", urls)
        self.assertTrue((ROOT / "produtos/migrations/0150_pdv_rapido_slot.py").exists())
