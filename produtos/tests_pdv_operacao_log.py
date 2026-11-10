"""Log de operações do PDV — registro seletivo de APIs."""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from django.test import RequestFactory, SimpleTestCase

from produtos.pdv_operacao_log_util import (
    _acao_de_path,
    deve_registrar_request_pdv,
    registrar_request_pdv,
)


class PdvOperacaoLogUtilTests(SimpleTestCase):
    def test_acao_mp_point_paths(self):
        self.assertEqual(_acao_de_path("/api/pdv/mp-point/criar/", "POST"), "mp_point_criar")
        self.assertEqual(
            _acao_de_path("/api/pdv/mp-point/forcar-liberar/", "POST"),
            "mp_point_forcar_liberar",
        )
        self.assertEqual(_acao_de_path("/api/fiado/baixa-pdv/", "POST"), "fiado_baixa_pdv")

    def test_deve_registrar_post_mp_nao_status(self):
        rf = RequestFactory()
        req = rf.post("/api/pdv/mp-point/criar/")
        self.assertTrue(deve_registrar_request_pdv(req))
        req_st = rf.get("/api/pdv/mp-point/status/?order_id=x")
        self.assertFalse(deve_registrar_request_pdv(req_st))
        req_skip = rf.get("/api/pdv/operador/")
        self.assertFalse(deve_registrar_request_pdv(req_skip))

    @patch("produtos.pdv_operacao_log_util.PdvOperacaoLogAgro.objects")
    @patch("produtos.pdv_operacao_log_util.operador_label_request", return_value="Vitor")
    def test_registrar_request_grava(self, _op, objs):
        rf = RequestFactory()
        req = rf.post(
            "/api/pdv/mp-point/forcar-liberar/",
            data=b'{"pin":"1"}',
            content_type="application/json",
        )
        req.session = MagicMock(session_key="sess-1")
        req.user = SimpleNamespace(is_authenticated=True, get_username=lambda: "admin")
        resp = SimpleNamespace(
            status_code=200,
            content=b'{"ok":true,"action":"finalize_required","order_id":"ORD1"}',
            get=lambda k, d="": "application/json" if "Type" in k else d,
        )
        registrar_request_pdv(req, resp)
        objs.create.assert_called_once()
        kwargs = objs.create.call_args.kwargs
        self.assertEqual(kwargs["acao"], "mp_point_forcar_liberar")
        self.assertEqual(kwargs["operador_nome"], "Vitor")
        self.assertEqual(kwargs["status_http"], 200)
        self.assertEqual(kwargs["detalhe_json"].get("resp_action"), "finalize_required")
