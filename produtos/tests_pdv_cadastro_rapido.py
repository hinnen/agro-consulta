"""PDV-CAD-RAPIDO — cadastro rápido no balcão (sem DB de teste Django)."""
from __future__ import annotations

import ast
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from django.test import SimpleTestCase

from produtos.pdv_cadastro_rapido_util import (
    alocar_gm_preview,
    ean_parece_valido,
    limpar_pendente_conferencia,
    marcar_extras_origem_pdv,
    normalizar_ean,
)

ROOT = Path(__file__).resolve().parents[1]


class UtilEanTests(SimpleTestCase):
    def test_normalizar(self):
        self.assertEqual(normalizar_ean("789.1234-567890"), "7891234567890")
        self.assertEqual(normalizar_ean(""), "")

    def test_parece_valido(self):
        self.assertTrue(ean_parece_valido("7891000100103"))
        self.assertFalse(ean_parece_valido("123"))
        self.assertFalse(ean_parece_valido("GM1542"))

    def test_marcar_limpar_pendente(self):
        ex = marcar_extras_origem_pdv({"fiscal": {"ncm": "x"}})
        self.assertTrue(ex["origem_pdv"])
        self.assertTrue(ex["pendente_conferencia"])
        self.assertEqual(ex["fiscal"]["ncm"], "x")
        limpo = limpar_pendente_conferencia(ex)
        self.assertFalse(limpo["pendente_conferencia"])
        self.assertTrue(limpo.get("conferido_em"))
        self.assertTrue(limpo["origem_pdv"])


class AlocarGmPreviewTests(SimpleTestCase):
    def test_preview_ok(self):
        with patch(
            "produtos.cadastro_codigo_sequencial_util.alocar_codigo_sequencial_novo_cadastro",
            return_value=(None, "4512", "GM4512"),
        ):
            err, sys, gm = alocar_gm_preview()
        self.assertIsNone(err)
        self.assertEqual(sys, "4512")
        self.assertEqual(gm, "GM4512")

    def test_preview_erro(self):
        with patch(
            "produtos.cadastro_codigo_sequencial_util.alocar_codigo_sequencial_novo_cadastro",
            return_value=({"erro": "esgotado", "status": 400}, None, None),
        ):
            err, sys, gm = alocar_gm_preview()
        self.assertIn("esgotado", err or "")
        self.assertEqual(sys, "")
        self.assertEqual(gm, "")


class BuscarProdutoPorCodigoTests(SimpleTestCase):
    def test_vazio(self):
        from produtos.pdv_cadastro_rapido_util import buscar_produto_por_codigo

        self.assertIsNone(buscar_produto_por_codigo(""))

    def test_acha_por_barras(self):
        from produtos.pdv_cadastro_rapido_util import buscar_produto_por_codigo

        fake_p = SimpleNamespace(produto_externo_id="AGROX1")
        qs = MagicMock()
        qs.filter.return_value.order_by.return_value.first.return_value = fake_p
        with patch("produtos.models.Produto.objects", qs), patch(
            "produtos.catalogo_agro.produto_agro_para_row",
            return_value={
                "id": "AGROX1",
                "nome": "Racao",
                "codigo": "4501",
                "codigo_nfe": "GM4501",
                "codigo_barras": "7899999999999",
                "preco_venda": 10,
            },
        ):
            row = buscar_produto_por_codigo("7899999999999")
        self.assertIsNotNone(row)
        self.assertEqual(row["nome"], "Racao")
        self.assertEqual(row["id"], "AGROX1")


class ConsultarInternetTests(SimpleTestCase):
    def test_ean_curto_nao_chama_rede(self):
        from produtos.pdv_cadastro_rapido_util import consultar_ean_internet

        with patch("urllib.request.urlopen") as u:
            out = consultar_ean_internet("123")
            u.assert_not_called()
        self.assertFalse(out["achou"])

    def test_achou_openfoodfacts(self):
        from produtos.pdv_cadastro_rapido_util import consultar_ean_internet
        import json

        payload = json.dumps(
            {
                "status": 1,
                "product": {
                    "product_name_pt": "Leite Integral",
                    "brands": "Marca X, Outra",
                },
            }
        ).encode()

        class Resp:
            def read(self):
                return payload

            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

        with patch(
            "produtos.pdv_cadastro_rapido_util._consultar_ean_cosmos", return_value=None
        ), patch("urllib.request.urlopen", return_value=Resp()):
            out = consultar_ean_internet("7891000100103")
        self.assertTrue(out["achou"])
        self.assertEqual(out["nome"], "Leite Integral")
        self.assertEqual(out["marca"], "Marca X")
        self.assertEqual(out["fonte"], "openfoodfacts")

    def test_sem_token_cosmos_motivo(self):
        from produtos.pdv_cadastro_rapido_util import consultar_ean_internet
        import urllib.error

        with patch(
            "produtos.pdv_cadastro_rapido_util._consultar_ean_cosmos", return_value=None
        ), patch(
            "urllib.request.urlopen",
            side_effect=urllib.error.HTTPError(
                "https://x", 404, "Not Found", hdrs=None, fp=None
            ),
        ), self.settings(AGRO_COSMOS_TOKEN=""):
            out = consultar_ean_internet("7898288820020")
        self.assertFalse(out["achou"])
        self.assertEqual(out.get("motivo"), "sem_cosmos")


class DerivarGmNoCriarLogicTests(SimpleTestCase):
    """Espelho da regra no api_pdv_cadastro_rapido_criar (GM sem código sistema)."""

    def _derive(self, cod_sys: str, cod_gm: str) -> tuple[str, str]:
        import re as _re

        if cod_gm and not cod_sys:
            m_gm = _re.match(r"^GM\s*(\d{4})$", cod_gm, flags=_re.IGNORECASE)
            if m_gm:
                cod_sys = m_gm.group(1)
                cod_gm = f"GM{cod_sys}"
        return cod_sys, cod_gm

    def test_gm_puro_deriva(self):
        self.assertEqual(self._derive("", "GM4522"), ("4522", "GM4522"))

    def test_ambos_ok(self):
        self.assertEqual(self._derive("4522", "GM4522"), ("4522", "GM4522"))

    def test_vazio_fica_vazio(self):
        self.assertEqual(self._derive("", ""), ("", ""))


class ArquivosPathTests(SimpleTestCase):
    def test_urls_registradas(self):
        from django.urls import reverse

        self.assertEqual(
            reverse("api_pdv_cadastro_rapido_checar"),
            "/api/produtos/pdv-cadastro-rapido/checar/",
        )
        self.assertEqual(
            reverse("api_pdv_cadastro_rapido_criar"),
            "/api/produtos/pdv-cadastro-rapido/criar/",
        )
        self.assertEqual(
            reverse("api_cadastro_pendentes_pdv"),
            "/api/produtos/cadastro/pendentes-pdv/",
        )

    def test_views_ast_ok(self):
        for rel in (
            "produtos/pdv_cadastro_rapido_util.py",
            "produtos/views.py",
            "produtos/cadastro_filtros_util.py",
            "pdv/views.py",
        ):
            src = (ROOT / rel).read_text(encoding="utf-8")
            ast.parse(src)

    def test_pdv_js_tem_fluxo(self):
        js = (ROOT / "produtos/static/produtos/js/pdv_wizard.js").read_text(encoding="utf-8")
        for token in (
            "wireCadastroRapidoUi",
            "apiPdvCadastroRapidoChecar",
            "apiPdvCadastroRapidoCriar",
            "cadastroRapidoChecarEan",
            "cadastroRapidoSalvar",
        ):
            self.assertIn(token, js)

    def test_templates_tem_modal_e_botao(self):
        step = (ROOT / "produtos/templates/produtos/partials/pdv/step_produtos.html").read_text(
            encoding="utf-8"
        )
        wiz = (ROOT / "produtos/templates/produtos/pdv_wizard.html").read_text(encoding="utf-8")
        cad = (ROOT / "produtos/templates/produtos/produtos_cadastro_erp.html").read_text(
            encoding="utf-8"
        )
        self.assertIn("pdv-btn-cadastro-rapido", step)
        self.assertIn("pdv-cadastro-rapido-overlay", wiz)
        self.assertIn("pdv-cadastro-rapido-step-ean", wiz)
        self.assertIn("cadastro-card-pendentes-pdv", cad)
        self.assertIn("URL_PENDENTES_PDV", cad)

    def test_cadastro_panel_filtro(self):
        js = (ROOT / "produtos/static/produtos/js/cadastro_erp_panel.js").read_text(encoding="utf-8")
        self.assertIn("pendente_pdv", js)
        self.assertIn("filtroPendentePdvAtivo", js)
        self.assertIn("refreshPendentesPdv", js)

    def test_bootstrap_urls_pdv(self):
        src = (ROOT / "pdv/views.py").read_text(encoding="utf-8")
        self.assertIn("apiPdvCadastroRapidoChecar", src)
        self.assertIn("apiPdvCadastroRapidoCriar", src)

    def test_filtro_parse_pendente(self):
        from produtos.cadastro_filtros_util import (
            filtros_cadastro_ativos,
            parse_filtros_cadastro,
        )

        class _Get(dict):
            def getlist(self, key):
                v = self.get(key)
                if v is None:
                    return []
                if isinstance(v, (list, tuple)):
                    return list(v)
                return [v]

        req = SimpleNamespace(GET=_Get(pendente_pdv="1"))
        f = parse_filtros_cadastro(req)
        self.assertTrue(f["pendente_pdv"])
        self.assertTrue(filtros_cadastro_ativos(f))
