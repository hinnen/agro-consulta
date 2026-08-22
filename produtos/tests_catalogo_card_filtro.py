from pathlib import Path

from django.template.loader import get_template
from django.test import SimpleTestCase

_CATALOGO_DIR = Path(__file__).resolve().parent / "templates" / "produtos" / "catalogo"
_JS = Path(__file__).resolve().parent / "static" / "produtos" / "js" / "catalogo_delivery.js"


class CatalogoCardFiltroTemplateTests(SimpleTestCase):
    def test_partial_card_filtro(self):
        html = get_template("produtos/catalogo/_card_filtro.html").render(
            {
                "btn_id": "btn-voltar-cats",
                "path_id": "titulo-cat-atual",
                "kicker": "Filtro",
            }
        )
        self.assertIn('id="btn-voltar-cats"', html)
        self.assertIn('id="titulo-cat-atual"', html)
        self.assertIn("card-filtro-voltar", html)
        self.assertIn("Voltar", html)
        self.assertIn("Filtro", html)

    def test_vitrine_usa_card_nas_tres_views(self):
        src = (_CATALOGO_DIR / "catalogo_delivery.html").read_text(encoding="utf-8")
        self.assertIn("card-filtro-sticky", src)
        self.assertIn('btn_id="btn-voltar-home"', src)
        self.assertIn('btn_id="btn-voltar-pesos"', src)
        self.assertIn('btn_id="btn-voltar-cats"', src)
        self.assertNotIn("bg-white/95 backdrop-blur", src)

    def test_js_renderiza_chips(self):
        src = _JS.read_text(encoding="utf-8")
        self.assertIn("function renderPathChips", src)
        self.assertIn("card-filtro-chip", src)
        self.assertIn("card-filtro-atual", src)
