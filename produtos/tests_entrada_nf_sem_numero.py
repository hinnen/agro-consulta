"""NF-SEM-NUMERO — Nº NF vazio gera SEM-DDMM-XXXX (único e pesquisável no CP)."""
from __future__ import annotations

import re
from unittest.mock import patch

from django.test import SimpleTestCase

from produtos.nfe_entrada_util import (
    _extrair_nf_numero_lancamento,
    _nf_numero_norm,
    gerar_numero_nf_sem_nota,
    garantir_numero_nf_cabecalho,
)


_SEM_RE = re.compile(r"^SEM-\d{4}-[A-Z0-9]{4}$")


class EntradaNfSemNumeroTests(SimpleTestCase):
    def test_gerar_formato(self):
        n = gerar_numero_nf_sem_nota()
        self.assertRegex(n, _SEM_RE)

    def test_garantir_preenche_vazio(self):
        cab = garantir_numero_nf_cabecalho({"emit_nome": "X", "numero": "  "})
        self.assertRegex(str(cab["numero"]), _SEM_RE)

    def test_garantir_mantem_informado(self):
        cab = garantir_numero_nf_cabecalho({"numero": "76468"})
        self.assertEqual(cab["numero"], "76468")

    def test_garantir_reusa_existente(self):
        with patch("produtos.nfe_entrada_util.gerar_numero_nf_sem_nota", return_value="SEM-0101-ZZZZ"):
            cab = garantir_numero_nf_cabecalho({"numero": ""}, existente="SEM-0710-A3F2")
        self.assertEqual(cab["numero"], "SEM-0710-A3F2")

    def test_extrair_do_titulo_cp(self):
        t = {
            "Descricao": "NF SEM-0710-A3F2 — Sn - Ms Comercio (parcela 1/1)",
            "Observacao": "Entrada NF-e Agro",
        }
        self.assertEqual(_extrair_nf_numero_lancamento(t), "SEM-0710-A3F2")
        self.assertEqual(
            _nf_numero_norm(_extrair_nf_numero_lancamento(t)),
            _nf_numero_norm("SEM-0710-A3F2"),
        )
