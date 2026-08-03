"""CAD-CB-OPC — barras opcionais no overlay + busca PDV (sem DB de teste)."""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from django.test import SimpleTestCase

from produtos.cadastro_busca_codigo_util import (
    index_codigos_de_campos,
    overlay_pids_por_codigo,
    termo_bate_codigos_produto,
)
from produtos.mongo_index_codigos import (
    codigos_barras_opcionais_de_cadastro_extras,
    normalizar_codigos_barras_opcionais,
)


class NormalizarCbOpcionaisTests(SimpleTestCase):
    def test_filtra_curto_e_duplicata_e_principal(self):
        out = normalizar_codigos_barras_opcionais(
            ["7891234567890", "789", "7891234567890", " 7899999999999 "],
            excluir="7891234567890",
        )
        self.assertEqual(out, ["7899999999999"])

    def test_string_separadores(self):
        out = normalizar_codigos_barras_opcionais("7891111111111;7892222222222|7891111111111")
        self.assertEqual(out, ["7891111111111", "7892222222222"])

    def test_cap_20(self):
        raw = [f"789{str(i).zfill(10)}" for i in range(25)]
        self.assertEqual(len(normalizar_codigos_barras_opcionais(raw)), 20)

    def test_extras_de_cadastro(self):
        ce = {"codigos_barras_opcionais": ["7893333333333", "abc"]}
        self.assertEqual(codigos_barras_opcionais_de_cadastro_extras(ce), ["7893333333333"])

    def test_termo_bate_extras(self):
        self.assertTrue(
            termo_bate_codigos_produto(
                "7894444444444",
                codigo_barras="7890000000000",
                extras=("7894444444444",),
            )
        )
        self.assertFalse(
            termo_bate_codigos_produto(
                "7894444444444",
                codigo_barras="7890000000000",
                extras=(),
            )
        )

    def test_index_codigos_inclui_extras(self):
        ix = index_codigos_de_campos(
            codigo="1234",
            codigo_nfe="GM1234",
            codigo_barras="7890000000000",
            extras=["7895555555555"],
        )
        joined = " ".join(str(x).lower() for x in ix)
        self.assertIn("7895555555555", joined)

    def test_overlay_pids_acha_por_opcional_mocked(self):
        pid = "AGRO-TEST-CB-OPC-1"
        principal = "7896000000001"
        opcional = "7896000000099"
        ov = SimpleNamespace(
            produto_externo_id=pid,
            codigo_nfe="",
            codigo_barras=principal,
            cadastro_extras={"codigos_barras_opcionais": [opcional]},
        )

        class _Sliceable:
            def __getitem__(self, _sl):
                return [ov]

        with patch("produtos.models.ProdutoGestaoOverlayAgro") as M:
            M.objects.filter.return_value.only.return_value = _Sliceable()
            found = overlay_pids_por_codigo(opcional)
            self.assertIn(pid, found)
            found_p = overlay_pids_por_codigo(principal)
            self.assertIn(pid, found_p)
