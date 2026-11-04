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
        self.assertEqual(
            _validar_origin("https://Loja.trycloudflare.com/zap?x=1"),
            "https://loja.trycloudflare.com",
        )
        self.assertIsNone(_validar_origin("http://inseguro.com"))
        self.assertIsNone(_validar_origin("https://user:senha@loja.com"))
        self.assertIsNone(_validar_origin(""))

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
        self.assertFalse(agro_wa_path_is_split_target("/api/atendimento-whatsapp/registrar-origin-publica"))
        self.assertTrue(agro_wa_path_is_split_target("/api/atendimento-whatsapp/registrar-origin-publica-extra/"))
        self.assertFalse(agro_wa_path_is_split_target("/pdv/checkout/"))
        self.assertFalse(agro_wa_path_is_split_target("/caixa/"))
        self.assertFalse(agro_wa_path_is_split_target("/api/pdv/zap-loja/orcamento/"))
        self.assertFalse(agro_wa_path_is_split_target("/consulta/"))
        self.assertFalse(agro_wa_path_is_split_target("/vendas/"))
        self.assertFalse(agro_wa_path_is_split_target("/api/nfe-saida/x/cancelar/"))
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
        self.assertIn("criar_tabela_se_faltar", mig)
        self.assertIn("if _TABELA in nomes", mig)
        for caminho in (
            "/pdv/checkout/",
            "/caixa/",
            "/caixa/fechar/",
            "/consulta/",
            "/vendas/",
            "/api/pdv/zap-loja/orcamento/",
            "/api/nfe-saida/abc/cancelar/",
            "/healthz",
        ):
            req = rf.get(caminho)
            req.META["HTTP_HOST"] = "www.sistvale.com.br"
            self.assertEqual(mw(req).content, b"pass", caminho)

    def test_migrate_nao_recria_tabela_existente(self):
        import importlib

        from django.apps import apps
        from django.db import connection

        from produtos.models import WhatsAppPublicOriginAgro

        mod = importlib.import_module("produtos.migrations.0154_whatsapp_public_origin")
        tabela = WhatsAppPublicOriginAgro._meta.db_table
        ja_tinha = tabela in connection.introspection.table_names()
        if not ja_tinha:
            with connection.schema_editor() as editor:
                editor.create_model(WhatsAppPublicOriginAgro)
        try:
            with connection.schema_editor() as editor:
                mod.criar_tabela_se_faltar(apps, editor)
            self.assertIn(tabela, connection.introspection.table_names())
        finally:
            if not ja_tinha:
                with connection.schema_editor() as editor:
                    editor.delete_model(WhatsAppPublicOriginAgro)

    def test_grava_so_o_host_e_cache_nao_derruba(self):
        from django.core.cache import cache
        from django.db import connection

        from produtos.agro_wa_public_origin_store import (
            agro_wa_public_origin_efetiva,
            agro_wa_public_origin_registrada,
            registrar_origin_publica,
        )
        from produtos.models import WhatsAppPublicOriginAgro

        tabela = WhatsAppPublicOriginAgro._meta.db_table
        ja_tinha = tabela in connection.introspection.table_names()
        if not ja_tinha:
            with connection.schema_editor() as editor:
                editor.create_model(WhatsAppPublicOriginAgro)
        try:
            cache.clear()
            ok, msg = registrar_origin_publica("https://Loja.trycloudflare.com/zap?x=1", fonte="pc")
            self.assertTrue(ok)
            self.assertEqual(msg, "https://loja.trycloudflare.com")
            cache.clear()
            self.assertEqual(agro_wa_public_origin_registrada(), "https://loja.trycloudflare.com")
        finally:
            WhatsAppPublicOriginAgro.objects.all().delete()
            cache.clear()
            if not ja_tinha:
                with connection.schema_editor() as editor:
                    editor.delete_model(WhatsAppPublicOriginAgro)

        from django.test import override_settings

        with override_settings(AGRO_WA_PUBLIC_ORIGIN="https://agro-consulta-wa.onrender.com"):
            with patch("produtos.agro_wa_public_origin_store.cache.get", side_effect=RuntimeError("redis")):
                with patch(
                    "produtos.agro_wa_public_origin_store.cache.set",
                    side_effect=RuntimeError("redis"),
                ):
                    self.assertEqual(
                        agro_wa_public_origin_efetiva(),
                        "https://agro-consulta-wa.onrender.com",
                    )


def main() -> int:
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(WaPublicOriginRegisterTests)
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    if result.wasSuccessful():
        print("VERIFY_OK 7")
        return 0
    print(f"VERIFY_FAILS={len(result.failures) + len(result.errors)}")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
