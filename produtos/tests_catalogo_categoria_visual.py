from django.test import SimpleTestCase
from unittest.mock import MagicMock, patch

from produtos.catalogo_delivery_util import (
    COR_CARD_PADRAO,
    cor_card_categoria,
    listar_categorias_arvore,
    normalizar_cor_categoria,
    url_imagem_categoria,
)


class CorCategoriaTests(SimpleTestCase):
    def test_hex_ok(self):
        self.assertEqual(normalizar_cor_categoria("#059669"), "#059669")
        self.assertEqual(normalizar_cor_categoria("FF8800"), "#ff8800")
        self.assertEqual(normalizar_cor_categoria("#abc"), "#aabbcc")

    def test_vazio_e_invalido(self):
        self.assertEqual(normalizar_cor_categoria(""), "")
        self.assertEqual(normalizar_cor_categoria("verde"), "")
        self.assertEqual(normalizar_cor_categoria("#gg0000"), "")

    def test_padrao_card(self):
        self.assertEqual(cor_card_categoria(""), COR_CARD_PADRAO)
        self.assertEqual(cor_card_categoria("#123456"), "#123456")


class UrlImagemCategoriaTests(SimpleTestCase):
    def test_sem_foto(self):
        cat = MagicMock(pk=9, imagem_base64="")
        self.assertEqual(url_imagem_categoria(cat), "")

    def test_com_foto(self):
        cat = MagicMock(pk=12, imagem_base64="abcd")
        self.assertEqual(url_imagem_categoria(cat), "/catalogo/cat-img/12/?v=4")


class ArvoreIncluiVisualNosFilhosTests(SimpleTestCase):
    @patch("produtos.catalogo_delivery_util.CatalogoDeliveryCategoria")
    def test_filho_tem_cor_e_imagem(self, Cat):
        raiz = MagicMock(
            pk=1,
            parent_id=None,
            nome="Cão",
            slug="cao",
            ordem=0,
            ativo=True,
            cor="#112233",
            imagem_base64="aaa",
            imagem_mime="image/jpeg",
        )
        filho = MagicMock(
            pk=2,
            parent_id=1,
            nome="Adulto",
            slug="adulto",
            ordem=0,
            ativo=True,
            cor="#ff8800",
            imagem_base64="bbbb",
            imagem_mime="image/jpeg",
        )
        Cat.objects.all.return_value.order_by.return_value.filter.return_value = [
            raiz,
            filho,
        ]
        arv = listar_categorias_arvore(so_ativas=True)
        self.assertEqual(len(arv), 1)
        self.assertEqual(arv[0]["cor"], "#112233")
        self.assertTrue(arv[0]["imagem"].startswith("/catalogo/cat-img/1/"))
        self.assertEqual(len(arv[0]["filhos"]), 1)
        self.assertEqual(arv[0]["filhos"][0]["cor"], "#ff8800")
        self.assertTrue(arv[0]["filhos"][0]["imagem"].startswith("/catalogo/cat-img/2/"))


class FotoApiQualquerNivelTests(SimpleTestCase):
    def test_view_nao_restringe_raiz(self):
        from pathlib import Path

        src = (
            Path(__file__).resolve().parent / "views_catalogo_delivery.py"
        ).read_text(encoding="utf-8")
        self.assertNotIn("parent__isnull=True", src)
        self.assertNotIn("só principal", src)
        self.assertIn("qualquer nível", src.lower())
