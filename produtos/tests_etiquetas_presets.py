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
