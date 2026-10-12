"""Overlay agro:vn (validade sem código) — não duplicar cadastro real."""
from django.test import TestCase

from produtos.cadastro_overlay_orfao_util import (
    AGRO_VN_PREFIX,
    eh_id_overlay_sintetico_validade,
    overlay_produto_externo_id_canonico,
    produto_externo_id_catalogo_por_codigo_ou_nome,
    remapear_id_planilha_agro_vn,
    remover_cadastro_duplicatas_agro_vn,
)
from produtos.models import Produto, ProdutoGestaoOverlayAgro


class CadastroOverlayOrfaoUtilTests(TestCase):
    def test_eh_id_sintetico(self):
        self.assertTrue(eh_id_overlay_sintetico_validade("agro:vn:abc123"))
        self.assertFalse(eh_id_overlay_sintetico_validade("69937a58a42b90be60615c02"))

    def test_resolve_por_gm_e_nome(self):
        p = Produto.objects.create(
            produto_externo_id="mongo-id-gm0443",
            nome="colosso pulverizacao 100ml",
            codigo_nfe="GM0443",
            ativo=True,
        )
        self.assertEqual(
            produto_externo_id_catalogo_por_codigo_ou_nome(codigo="GM0443"),
            "mongo-id-gm0443",
        )
        self.assertEqual(
            produto_externo_id_catalogo_por_codigo_ou_nome(nome=p.nome),
            "mongo-id-gm0443",
        )

    def test_overlay_canonico_prefere_produto_real(self):
        Produto.objects.create(
            produto_externo_id="real-pid-25",
            nome="colosso pulverizacao 25 ml",
            codigo_nfe="GM0445",
            ativo=True,
        )
        ProdutoGestaoOverlayAgro.objects.create(
            produto_externo_id=f"{AGRO_VN_PREFIX}deadbeef",
            nome="colosso pulverizacao 25 ml",
        )
        self.assertEqual(
            overlay_produto_externo_id_canonico(nome="colosso pulverizacao 25 ml"),
            "real-pid-25",
        )

    def test_remapear_planilha(self):
        Produto.objects.create(
            produto_externo_id="pid-100",
            nome="colosso pulverizacao 100ml",
            codigo_nfe="GM0443",
            ativo=True,
        )
        novo, aviso = remapear_id_planilha_agro_vn(
            f"{AGRO_VN_PREFIX}9446c6f51e7a72514cbf",
            codigo_gm="GM0443",
            nome="colosso pulverizacao 100ml",
        )
        self.assertEqual(novo, "pid-100")
        self.assertIsNotNone(aviso)

    def test_remove_duplicata_na_lista(self):
        rows = [
            {"id": "pid-real", "nome": "colosso pulverizacao 100ml", "codigo_nfe": "GM0443"},
            {
                "id": f"{AGRO_VN_PREFIX}9446c6f51e7a72514cbf",
                "nome": "colosso pulverizacao 100ml",
            },
        ]
        out = remover_cadastro_duplicatas_agro_vn(rows)
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0]["id"], "pid-real")
