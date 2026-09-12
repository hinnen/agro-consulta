import json
from contextlib import nullcontext
from types import SimpleNamespace
from unittest.mock import patch

from django.db.models import JSONField
from django.test import RequestFactory, SimpleTestCase

from produtos.models import EtiquetaPresetAgro
from produtos.views import api_etiquetas_presets


class EtiquetaPresetAgroApiTests(SimpleTestCase):
    def test_modelo_persiste_payload_em_jsonfield_postgres(self):
        self.assertIsInstance(EtiquetaPresetAgro._meta.get_field("payload"), JSONField)
        self.assertTrue(EtiquetaPresetAgro._meta.get_field("client_key").unique)

    @patch("produtos.views.transaction.atomic", return_value=nullcontext())
    @patch("produtos.views.EtiquetaPresetAgro.objects")
    def test_api_persiste_grade_layout_e_cores_do_preset_60mm(self, objects, _atomic):
        payload = {
            "id": "remedios-6cm",
            "nome": "remedios 6cm",
            "estilo": "gondola",
            "largura_mm": 60,
            "altura_mm": 30,
            "borda_mm": 0.5,
            "cols_folha": 3,
            "rows_folha": 9,
            "show_logo": True,
            "show_gm": True,
            "cores": {"faixa_bg": "#123456", "preco_fg": "#111111"},
            "layout": {"preco": {"x": 27, "y": 35, "w": 57, "h": 43}},
        }
        saved = SimpleNamespace(
            pk=7,
            client_key=payload["id"],
            nome=payload["nome"],
            payload=payload,
            atualizado_em=None,
        )
        objects.filter.return_value.first.return_value = None
        objects.create.return_value = saved
        request = RequestFactory().post(
            "/api/produtos/etiquetas/presets/",
            data=json.dumps({"client_key": payload["id"], "payload": payload}),
            content_type="application/json",
        )
        request.user = SimpleNamespace(is_authenticated=True)

        response = api_etiquetas_presets(request)

        self.assertEqual(response.status_code, 200)
        clean = objects.create.call_args.kwargs["payload"]
        self.assertEqual(clean["cols_folha"], 3)
        self.assertEqual(clean["rows_folha"], 9)
        self.assertEqual(clean["layout"], payload["layout"])
        self.assertEqual(clean["cores"], payload["cores"])

    @patch("produtos.views.transaction.atomic", return_value=nullcontext())
    @patch("produtos.views.EtiquetaPresetAgro.objects")
    def test_api_persiste_folha_a6_bonus_100x45(self, objects, _atomic):
        payload = {
            "id": "bonus-a6",
            "nome": "Bônus A6",
            "estilo": "gondola",
            "folha": "a6",
            "largura_mm": 100,
            "altura_mm": 45,
            "borda_mm": 0.5,
            "cols_folha": 1,
            "rows_folha": 3,
            "show_logo": True,
            "show_nome": True,
            "show_preco": True,
            "cores": {"faixa_bg": "#1a4d2e", "preco_fg": "#1a4d2e"},
            "layout": {"nome": {"x": 0, "y": 0, "w": 100, "h": 32}},
        }
        saved = SimpleNamespace(
            pk=9,
            client_key=payload["id"],
            nome=payload["nome"],
            payload=payload,
            atualizado_em=None,
        )
        objects.filter.return_value.first.return_value = None
        objects.create.return_value = saved
        request = RequestFactory().post(
            "/api/produtos/etiquetas/presets/",
            data=json.dumps({"client_key": payload["id"], "nome": payload["nome"], "payload": payload}),
            content_type="application/json",
        )
        request.user = SimpleNamespace(is_authenticated=True)

        response = api_etiquetas_presets(request)

        self.assertEqual(response.status_code, 200)
        clean = objects.create.call_args.kwargs["payload"]
        self.assertEqual(clean["folha"], "a6")
        self.assertEqual(clean["largura_mm"], 100)
        self.assertEqual(clean["altura_mm"], 45)
        self.assertEqual(clean["cols_folha"], 1)
        self.assertEqual(clean["rows_folha"], 3)
        self.assertEqual(clean["estilo"], "gondola")
        self.assertEqual(clean["layout"], payload["layout"])
