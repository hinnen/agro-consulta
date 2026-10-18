"""Busca por palavras-chave quando overlay e Produto usam IDs diferentes."""
from django.test import TestCase

from produtos.medicamento_vet_taxonomia import indice_busca_palavras_chave
from produtos.models import Produto, ProdutoGestaoOverlayAgro


class CadastroBuscaPalavrasOverlayIdTests(TestCase):
    def test_buscar_2_tempo_overlay_no_erp_produto_id(self):
        from produtos.catalogo_agro import buscar

        Produto.objects.create(
            codigo_interno="OLEO2T",
            nome="OLEO CASTROL 2T",
            produto_externo_id="GM-OLEO-2T-TEST",
            erp_produto_id="mongo-overlay-key-2t",
            ativo=True,
            cadastro_inativo=False,
        )
        pk = "oleo, 2 tempo, 2tempo"
        ProdutoGestaoOverlayAgro.objects.create(
            produto_externo_id="mongo-overlay-key-2t",
            cadastro_extras={
                "palavras_chave": pk,
                "busca_palavras_agro": indice_busca_palavras_chave(pk),
            },
        )

        for termo in ("2 tempo", "2tempo", "oleo"):
            rows = buscar(termo, limit=20)
            nomes = [r.get("nome") or "" for r in rows]
            self.assertIn(
                "OLEO CASTROL 2T",
                nomes,
                msg=f"termo={termo!r} rows={nomes}",
            )

    def test_overlay_pids_2tempo_sem_frase_com_espaco(self):
        from produtos.cadastro_busca_codigo_util import overlay_pids_por_palavras_busca

        ProdutoGestaoOverlayAgro.objects.create(
            produto_externo_id="ov-2t-only-compact",
            cadastro_extras={
                "palavras_chave": "oleo, 2tempo",
            },
        )
        pids = overlay_pids_por_palavras_busca("2 tempo", limit=10)
        self.assertIn("ov-2t-only-compact", pids)
        pids2 = overlay_pids_por_palavras_busca("2tempo", limit=10)
        self.assertIn("ov-2t-only-compact", pids2)

    def test_qs_produtos_catalogo_por_ids_externos_erp(self):
        from produtos.cadastro_busca_codigo_util import qs_produtos_catalogo_por_ids_externos
        from produtos.catalogo_agro import queryset_catalogo_ativos

        p = Produto.objects.create(
            codigo_interno="X1",
            nome="Produto alias ERP",
            produto_externo_id="GM-ALIAS-ERP",
            erp_produto_id="erp-only-id",
            ativo=True,
            cadastro_inativo=False,
        )
        qs = queryset_catalogo_ativos()
        found = list(qs_produtos_catalogo_por_ids_externos(qs, ["erp-only-id"]))
        self.assertEqual(len(found), 1)
        self.assertEqual(found[0].pk, p.pk)
