#!/usr/bin/env python3
"""Registro automatico da URL do tunel no PDV (agro_wa_public_origin_store + API)."""
from __future__ import annotations

import os
import sys
import unittest
from unittest.mock import patch

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")


class WaPublicOriginRegisterTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import django

        django.setup()

    def test_validacao_origin(self):
        from produtos.agro_wa_public_origin_store import _validar_origin

        self.assertEqual(_validar_origin("https://lab.trycloudflare.com/"), "https://lab.trycloudflare.com")
        self.assertIsNone(_validar_origin("http://inseguro.com"))

    def test_prioridade_registro_sobre_env(self):
        from django.test import override_settings

        from produtos.agro_wa_origin_util import agro_wa_public_origin

        with override_settings(AGRO_WA_PUBLIC_ORIGIN="https://render-antigo.onrender.com"):
            with patch(
                "produtos.agro_wa_public_origin_store.agro_wa_public_origin_registrada",
                return_value="https://pc.trycloudflare.com",
            ):
                self.assertEqual(agro_wa_public_origin(), "https://pc.trycloudflare.com")

    def test_api_registrar(self):
        from django.test import RequestFactory, override_settings

        from produtos.views_atendimento_whatsapp import api_atendimento_whatsapp_registrar_origin_publica

        token = "segredo-registro-teste"
        tunel = "https://lab-auto.trycloudflare.com"
        rf = RequestFactory()

        with override_settings(AGRO_WA_BRIDGE_TOKEN=token):
            with patch(
                "produtos.agro_wa_public_origin_store.registrar_origin_publica",
                return_value=(True, tunel),
            ) as mock_reg:
                req = rf.post(
                    "/api/atendimento-whatsapp/registrar-origin-publica/",
                    data='{"origin":"' + tunel + '"}',
                    content_type="application/json",
                    HTTP_X_AGRO_WA_TOKEN=token,
                )
                resp = api_atendimento_whatsapp_registrar_origin_publica(req)
                self.assertEqual(resp.status_code, 200)
                mock_reg.assert_called_once()

            req2 = rf.post(
                "/api/atendimento-whatsapp/registrar-origin-publica/",
                data='{"origin":"https://x.com"}',
                content_type="application/json",
            )
            resp2 = api_atendimento_whatsapp_registrar_origin_publica(req2)
            self.assertEqual(resp2.status_code, 403)

    def test_sem_registro_fica_no_render(self):
        from django.test import override_settings

        from produtos.agro_wa_origin_util import agro_wa_public_origin

        with override_settings(AGRO_WA_PUBLIC_ORIGIN="https://agro-consulta-wa.onrender.com"):
            with patch(
                "produtos.agro_wa_public_origin_store.agro_wa_public_origin_registrada",
                return_value="",
            ):
                self.assertEqual(agro_wa_public_origin(), "https://agro-consulta-wa.onrender.com")

    def test_registro_fica_no_pdv_e_venda_nao_sai(self):
        from django.http import HttpResponse
        from django.test import RequestFactory
        from django.urls import reverse

        from base.wa_split_middleware import AgroWhatsAppSplitMiddleware
        from produtos.agro_wa_origin_util import agro_wa_path_is_split_target

        self.assertFalse(agro_wa_path_is_split_target("/api/atendimento-whatsapp/registrar-origin-publica/"))
        self.assertFalse(agro_wa_path_is_split_target("/pdv/checkout/"))
        self.assertFalse(agro_wa_path_is_split_target("/caixa/"))
        self.assertFalse(agro_wa_path_is_split_target("/api/pdv/zap-loja/orcamento/"))
        self.assertTrue(agro_wa_path_is_split_target("/atendimento-whatsapp/"))
        self.assertTrue(agro_wa_path_is_split_target("/api/atendimento-whatsapp/bridge/orcamento/"))

        from django.conf import settings

        settings.AGRO_WA_PUBLIC_ORIGIN = "https://agro-consulta-wa.onrender.com"

        def _app(_request):
            return HttpResponse("pass", status=200)

        mw = AgroWhatsAppSplitMiddleware(_app)
        rf = RequestFactory()
        req = rf.post("/api/atendimento-whatsapp/registrar-origin-publica/")
        req.META["HTTP_HOST"] = "www.sistvale.com.br"
        self.assertEqual(mw(req).content, b"pass")
        chat = rf.get("/atendimento-whatsapp/")
        chat.META["HTTP_HOST"] = "www.sistvale.com.br"
        redir = mw(chat)
        self.assertIn(redir.status_code, (301, 302))
        self.assertEqual(reverse("pdv_checkout"), "/pdv/checkout/")
        self.assertTrue(reverse("api_nfe_saida_cancelar", kwargs={"doc_id": "x"}).endswith("/cancelar/"))
        mig = open(
            os.path.join(ROOT, "produtos/migrations/0154_whatsapp_public_origin.py"),
            encoding="utf-8",
        ).read()
        self.assertIn('("produtos", "0153_nfe_fiscal_contador")', mig)
        self.assertNotIn("0154_whatsapp_msg_reacao", mig)


def main() -> int:
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(WaPublicOriginRegisterTests)
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    if result.wasSuccessful():
        print("VERIFY_OK 5")
        return 0
    print(f"VERIFY_FAILS={len(result.failures) + len(result.errors)}")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
