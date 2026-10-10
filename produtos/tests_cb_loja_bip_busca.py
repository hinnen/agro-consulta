"""Busca 230… não cruza cadastro legado com EAN válido de outro produto."""
from django.test import SimpleTestCase

from produtos.busca_filtro_pdv_util import (
    filtrar_documentos_estilo_pdv,
    score_relevancia_doc,
    termo_eh_ean_loja_bip_valido,
)
from produtos.cadastro_busca_codigo_util import (
    cb_loja_bip_equivalente,
    index_codigos_de_campos,
    termo_bate_codigos_produto,
)


class CbLojaBipBuscaTests(SimpleTestCase):
    def test_1479_e_1471_nao_sao_equivalentes(self):
        self.assertFalse(cb_loja_bip_equivalente("2300000001471", "2300000001479"))
        self.assertFalse(cb_loja_bip_equivalente("2300000001479", "2300000001471"))

    def test_bip_valido_nao_bate_cadastro_legado(self):
        self.assertFalse(
            termo_bate_codigos_produto(
                "2300000001471",
                codigo_barras="2300000001479",
            )
        )

    def test_index_nao_inclui_canonicalizacao_ambigua(self):
        ix = index_codigos_de_campos(codigo_barras="2300000001479")
        self.assertIn("2300000001479", ix)
        self.assertNotIn("2300000001471", ix)

    def test_ean_loja_bip_valido_1556(self):
        self.assertTrue(termo_eh_ean_loja_bip_valido("2300000001556"))

    def test_pdv_score_prefere_cadastro_raiz_sobre_index_stale(self):
        bip = "2300000001556"
        errado = {
            "CodigoNFe": "GM4241",
            "CodigoBarras": "2300015721739",
            "index_codigos": [bip, "2300015721739"],
        }
        certo = {
            "CodigoNFe": "GM0024-P",
            "CodigoBarras": bip,
            "index_codigos": [bip],
        }
        self.assertGreater(score_relevancia_doc(certo, bip), score_relevancia_doc(errado, bip))
        self.assertEqual(score_relevancia_doc(errado, bip), 0)

    def test_pdv_filtro_remove_hit_só_por_index(self):
        bip = "2300000001556"
        docs = [
            {
                "CodigoNFe": "GM4241",
                "CodigoBarras": "2300015721739",
                "index_codigos": [bip],
            },
            {"CodigoNFe": "GM0024-P", "CodigoBarras": bip, "index_codigos": [bip]},
        ]
        out = filtrar_documentos_estilo_pdv(docs, bip)
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0]["CodigoNFe"], "GM0024-P")
