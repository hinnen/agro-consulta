"""CAD-CB-OPC — barras opcionais no overlay + busca PDV (sem DB de teste)."""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from django.test import SimpleTestCase

from produtos.cadastro_busca_codigo_util import (
    index_codigos_de_campos,
    overlay_pids_por_codigo,
    q_overlay_json_barras_opcionais,
    termo_bate_codigos_produto,
)
from produtos.mongo_index_codigos import (
    aplicar_bip_entrada_nf_troca_inteligente,
    codigos_barras_opcionais_de_cadastro_extras,
    mesclar_codigos_barras_opcionais_adicionar,
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

    def test_extras_mescla_alias_quando_lista_vazia(self):
        ce = {
            "codigos_barras_opcionais": [],
            "codigos_barras_alternativos": ["7893333333333"],
        }
        self.assertEqual(
            codigos_barras_opcionais_de_cadastro_extras(ce),
            ["7893333333333"],
        )

    def test_mesclar_adicionar_nao_apaga_existentes(self):
        ce = {"codigos_barras_opcionais": ["7891111111111"]}
        out = mesclar_codigos_barras_opcionais_adicionar(
            ce,
            ["7898752405197", "7891111111111"],
            principal="2300000001490",
        )
        self.assertEqual(out, ["7891111111111", "7898752405197"])

    def test_troca_inteligente_promove_230(self):
        res = aplicar_bip_entrada_nf_troca_inteligente(
            codigo_barras_atual="2300000001490",
            cadastro_extras={},
            bip="7898752405197",
        )
        self.assertEqual(res["acao"], "promove")
        self.assertEqual(res["codigo_barras"], "7898752405197")
        self.assertIn("2300000001490", res["codigos_barras_opcionais"])

    def test_troca_inteligente_ean_fabrica_so_opcional(self):
        res = aplicar_bip_entrada_nf_troca_inteligente(
            codigo_barras_atual="7891111111111",
            cadastro_extras={},
            bip="7898752405197",
        )
        self.assertEqual(res["acao"], "opcional")
        self.assertIsNone(res["codigo_barras"])
        self.assertEqual(res["codigos_barras_opcionais"], ["7898752405197"])

    def test_troca_inteligente_mesmo_codigo_noop(self):
        res = aplicar_bip_entrada_nf_troca_inteligente(
            codigo_barras_atual="7898752405197",
            cadastro_extras={},
            bip="7898752405197",
        )
        self.assertEqual(res["acao"], "noop")

    def test_troca_inteligente_sem_promover_se_loja_false(self):
        res = aplicar_bip_entrada_nf_troca_inteligente(
            codigo_barras_atual="2300000001490",
            cadastro_extras={},
            bip="7898752405197",
            promover_se_loja=False,
        )
        self.assertEqual(res["acao"], "opcional")
        self.assertIsNone(res["codigo_barras"])
        self.assertIn("7898752405197", res["codigos_barras_opcionais"])
        self.assertNotIn("2300000001490", res["codigos_barras_opcionais"])

    def test_parece_ean_fabrica_br(self):
        from produtos.management.commands.contar_bip_entrada_nf_cadastro import (
            parece_ean_fabrica_br,
        )

        self.assertTrue(parece_ean_fabrica_br("7898242031950"))
        self.assertTrue(parece_ean_fabrica_br("17898242031950"))
        self.assertFalse(parece_ean_fabrica_br("1111111111111"))
        self.assertFalse(parece_ean_fabrica_br("1234567891010"))
        self.assertFalse(parece_ean_fabrica_br("3000000052600"))

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

    def test_extras_q_overlay_duas_chaves_e_minimo_8(self):
        self.assertIsNone(q_overlay_json_barras_opcionais("1234567"))
        q = q_overlay_json_barras_opcionais("7896000000099")
        s = str(q)
        self.assertIn("codigos_barras_opcionais", s)
        self.assertIn("codigos_barras_alternativos", s)

    def test_overlay_pids_q_inclui_duas_chaves(self):
        pid = "AGRO-TEST-CB-OPC-Q"
        opcional = "7896000000099"
        ov = SimpleNamespace(
            produto_externo_id=pid,
            codigo_nfe="",
            codigo_barras="7896000000001",
            cadastro_extras={"codigos_barras_alternativos": [opcional]},
        )

        class _Sliceable:
            def __getitem__(self, _sl):
                return [ov]

        with patch("produtos.models.ProdutoGestaoOverlayAgro") as M:
            M.objects.filter.return_value.only.return_value = _Sliceable()
            found = overlay_pids_por_codigo(opcional)
            self.assertIn(pid, found)
            q = M.objects.filter.call_args_list[0][0][0]
            s = str(q)
            self.assertIn("codigos_barras_opcionais", s)
            self.assertIn("codigos_barras_alternativos", s)
