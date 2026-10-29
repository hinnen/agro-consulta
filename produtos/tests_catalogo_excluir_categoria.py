from django.test import SimpleTestCase
from unittest.mock import MagicMock, patch

from produtos.catalogo_delivery_util import (
    _DELIVERY_CAT_ID_KEYS,
    ids_subarvore_categoria,
    limpar_refs_delivery_categorias,
)


class CatalogoExcluirCategoriaHelpersTests(SimpleTestCase):
    def test_keys_delivery_cat(self):
        self.assertIn("categoria_id", _DELIVERY_CAT_ID_KEYS)
        self.assertIn("subcategoria4_id", _DELIVERY_CAT_ID_KEYS)

    @patch("produtos.catalogo_delivery_util.CatalogoDeliveryCategoria")
    def test_ids_subarvore(self, Cat):
        # raiz 1 → 2 → 3 ; 1 → 4
        rows = [
            MagicMock(pk=1, parent_id=None),
            MagicMock(pk=2, parent_id=1),
            MagicMock(pk=3, parent_id=2),
            MagicMock(pk=4, parent_id=1),
            MagicMock(pk=9, parent_id=None),
        ]
        Cat.objects.all.return_value.only.return_value = rows
        self.assertEqual(ids_subarvore_categoria(1), {1, 2, 3, 4})
        self.assertEqual(ids_subarvore_categoria(2), {2, 3})
        self.assertEqual(ids_subarvore_categoria(9), {9})
        self.assertEqual(ids_subarvore_categoria(0), set())

    @patch("produtos.catalogo_delivery_util.ProdutoGestaoOverlayAgro")
    def test_limpar_refs(self, Ov):
        ov = MagicMock()
        ov.cadastro_extras = {
            "delivery": {
                "ativo": True,
                "categoria_id": 10,
                "subcategoria_id": 20,
                "subcategoria2_id": 0,
                "subcategoria3_id": 0,
                "subcategoria4_id": 0,
            }
        }
        Ov.objects.iterator.return_value = [ov]
        n = limpar_refs_delivery_categorias({10, 20})
        self.assertEqual(n, 1)
        ov.save.assert_called_once()
        d = ov.cadastro_extras["delivery"]
        self.assertEqual(int(d.get("categoria_id") or 0), 0)
        self.assertEqual(int(d.get("subcategoria_id") or 0), 0)
