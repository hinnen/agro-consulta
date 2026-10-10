"""Migração legado 230 por grupo EAN (--liberar-intruso)."""
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from django.test import SimpleTestCase

from produtos.cb_loja_legado_migrate_util import (
    _escolher_vencedor_grupo_migracao,
    _migrar_cb_loja_legado_lote_por_grupo,
)


class EscolherVencedorGrupoTests(SimpleTestCase):
    @patch("produtos.models.Produto.objects")
    def test_prefere_codigo_gm_com_sufixo_p(self, prod_mgr):
        def _row(gm: str):
            return SimpleNamespace(codigo_nfe=gm, nome="", produto_externo_id="x")

        prod_mgr.filter.return_value.only.return_value.first.side_effect = [
            _row("OUTRO"),
            _row("GM0024-P"),
        ]
        gm = SimpleNamespace(produto_externo_id="gm0024")
        outro = SimpleNamespace(produto_externo_id="outro")
        members = [
            (outro, None, "2300000001554", "2300000001556"),
            (gm, None, "2300000001558", "2300000001556"),
        ]
        p, _ov, atual, bip = _escolher_vencedor_grupo_migracao(members)
        self.assertEqual(p.produto_externo_id, "gm0024")
        self.assertEqual(atual, "2300000001558")
        self.assertEqual(bip, "2300000001556")


class MigrarLotePorGrupoTests(SimpleTestCase):
    @patch("produtos.cb_loja_legado_migrate_util._liberar_slot_bip_antes_migracao")
    @patch("produtos.cb_loja_legado_migrate_util.transaction.atomic")
    @patch("produtos.cb_loja_legado_migrate_util.migrar_cb_loja_legado_em_produto")
    @patch("produtos.cb_loja_legado_migrate_util._escolher_vencedor_grupo_migracao")
    @patch("produtos.cb_loja_legado_migrate_util.iter_produtos_cb_loja_legado")
    def test_um_grupo_migra_vencedor_apos_reatribuir(
        self, iter_mock, escolher, migrar, atomic_mock, liberar_slot
    ):
        liberar_slot.return_value = [{"pid": "b"}]
        atomic_mock.return_value.__enter__ = MagicMock(return_value=None)
        atomic_mock.return_value.__exit__ = MagicMock(return_value=False)
        p1 = SimpleNamespace(produto_externo_id="a", codigo_barras="2300000001558")
        p2 = SimpleNamespace(produto_externo_id="b", codigo_barras="2300000001554")
        iter_mock.return_value = [(p1, None), (p2, None)]
        escolher.return_value = (p1, None, "2300000001558", "2300000001556")
        migrar.return_value = {"produto_externo_id": "a", "principal_novo": "2300000001556"}

        r = _migrar_cb_loja_legado_lote_por_grupo(dry_run=False)

        self.assertEqual(r["corrigidos"], 1)
        self.assertEqual(r["grupos"], 1)
        self.assertEqual(r["colisoes"], 0)
        liberar_slot.assert_called_once()
        migrar.assert_called_once()
        self.assertFalse(migrar.call_args.kwargs.get("liberar_intruso"))

    @patch("produtos.cb_loja_legado_migrate_util.migrar_cb_loja_legado_em_produto")
    @patch("produtos.cb_loja_legado_migrate_util._escolher_vencedor_grupo_migracao")
    @patch("produtos.cb_loja_legado_migrate_util.iter_produtos_cb_loja_legado")
    def test_dois_grupos_ean_contam_separado(self, iter_mock, escolher, migrar):
        pa = SimpleNamespace(produto_externo_id="a", codigo_barras="2300000001558")
        pb = SimpleNamespace(produto_externo_id="b", codigo_barras="2300000001472")
        iter_mock.return_value = [(pa, None), (pb, None)]
        escolher.side_effect = [
            (pa, None, "2300000001558", "2300000001556"),
            (pb, None, "2300000001472", "2300000001471"),
        ]
        migrar.return_value = {"ok": True}

        with patch(
            "produtos.cb_loja_legado_migrate_util._liberar_slot_bip_antes_migracao",
            return_value=[],
        ):
            r = _migrar_cb_loja_legado_lote_por_grupo(dry_run=True)

        self.assertEqual(r["grupos"], 2)
        self.assertEqual(r["corrigidos"], 2)
        self.assertEqual(migrar.call_count, 2)
