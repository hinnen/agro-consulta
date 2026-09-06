"""NF-VINCULO-NAO-SOBRESCREVE — vínculo NF não apaga nome/marca/preço do cadastro."""
from __future__ import annotations

from decimal import Decimal

from django.test import TestCase

from produtos.cadastro_alteracao_historico_util import (
    enriquecer_snapshot_antes_com_catalogo,
    registrar_diffs_cadastro,
    snapshot_overlay,
)
from produtos.catalogo_agro import (
    payload_overlay_deve_sincronizar_produto,
    sincronizar_modelo_produto_de_overlay,
)
from produtos.models import (
    Produto,
    ProdutoCadastroAlteracaoAgro,
    ProdutoGestaoOverlayAgro,
    registrar_lote_validade_apos_entrada_nf,
)


class NfVinculoNaoSobrescreveTests(TestCase):
    def setUp(self):
        self.pid = "pid-ivomec-1097"
        self.nome = "ivomec ivermectina injetavel 50ml boehringer ingelheim"
        self.p = Produto.objects.create(
            produto_externo_id=self.pid,
            codigo_interno="1097",
            codigo_nfe="GM1097",
            codigo_barras="7898053772789",
            nome=self.nome,
            marca="BOEHRINGER INGELHEIM",
            categoria="Medicamentos",
            unidade="UN",
            custo=Decimal("27.90"),
            preco_venda=Decimal("39.90"),
        )
        self.ov = ProdutoGestaoOverlayAgro.objects.create(produto_externo_id=self.pid)

    def _reload(self) -> Produto:
        self.p.refresh_from_db()
        return self.p

    def test_cprod_nao_mexe_catalogo(self):
        self.assertFalse(
            payload_overlay_deve_sincronizar_produto(
                {"produto_id": self.pid, "c_prod_nf": "199", "origem_entrada_nf": True}
            )
        )
        out = sincronizar_modelo_produto_de_overlay(
            self.pid,
            self.ov,
            payload={"produto_id": self.pid, "c_prod_nf": "199", "origem_entrada_nf": True},
        )
        p = self._reload()
        self.assertEqual(out.pk, p.pk)
        self.assertEqual(p.nome, self.nome)
        self.assertEqual(p.marca, "BOEHRINGER INGELHEIM")
        self.assertEqual(p.categoria, "Medicamentos")
        self.assertEqual(p.codigo_nfe, "GM1097")
        self.assertEqual(p.unidade, "UN")
        self.assertEqual(p.preco_venda, Decimal("39.90"))
        self.assertEqual(p.custo, Decimal("27.90"))

    def test_custo_nf_so_custo(self):
        sincronizar_modelo_produto_de_overlay(
            self.pid, self.ov, custo_payload=Decimal("14.45")
        )
        p = self._reload()
        self.assertEqual(p.nome, self.nome)
        self.assertEqual(p.marca, "BOEHRINGER INGELHEIM")
        self.assertEqual(p.codigo_nfe, "GM1097")
        self.assertEqual(p.custo, Decimal("14.45"))
        self.assertEqual(p.preco_venda, Decimal("39.90"))

    def test_editar_nome_ainda_grava(self):
        self.ov.nome = "Ivomec 50ml loja"
        self.ov.save(update_fields=["nome", "atualizado_em"])
        sincronizar_modelo_produto_de_overlay(
            self.pid, self.ov, payload={"nome": "Ivomec 50ml loja"}
        )
        self.assertEqual(self._reload().nome, "Ivomec 50ml loja")

    def test_lote_validade_nao_copia_xprod(self):
        registrar_lote_validade_apos_entrada_nf(
            self.pid,
            {"lote_numero": "L1", "lote_validade": "2027-08-11"},
            Decimal("6"),
            nome_produto="IVOMEC - 50 ML [7898053773339]",
            deposito="centro",
        )
        self.ov.refresh_from_db()
        self.assertEqual((self.ov.nome or "").strip(), "")
        self.assertEqual(self._reload().nome, self.nome)

    def test_historico_vinculo_nao_finge_apagar_nome(self):
        hist_antes = enriquecer_snapshot_antes_com_catalogo(self.pid, snapshot_overlay(self.ov))
        self.ov.cadastro_extras = {"entrada_nfe_c_prods": ["199"]}
        hist_depois = enriquecer_snapshot_antes_com_catalogo(self.pid, snapshot_overlay(self.ov))
        n = registrar_diffs_cadastro(
            produto_id=self.pid,
            antes=hist_antes,
            depois=hist_depois,
            origem="nf",
        )
        campos = set(
            ProdutoCadastroAlteracaoAgro.objects.filter(produto_externo_id=self.pid).values_list(
                "campo", flat=True
            )
        )
        self.assertGreaterEqual(n, 1)
        self.assertIn("c_prod_nf", campos)
        self.assertNotIn("nome", campos)
        self.assertNotIn("marca", campos)
        self.assertNotIn("preco_venda", campos)
        self.assertNotIn("codigo_nfe", campos)
