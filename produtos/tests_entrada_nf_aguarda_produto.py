"""Testes NF-AGUARDA-PRODUTO — fornecedor deve produto."""
from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import patch

from django.test import RequestFactory, SimpleTestCase

from produtos.nfe_entrada_util import (
    _entrada_nfe_item_casa_filtro_lista,
    entrada_nfe_enriquecer_doc_serializado,
    entrada_nfe_fila_bucket_lista,
    entrada_nfe_status_efetivo,
    pipeline_acao_rascunho_entrada,
)
from produtos.tests_entrada_nf_reabertura_estoque import FakeCollection, RID, _doc
from produtos.views import api_entrada_nota_rascunho_acao


class EntradaNfAguardaProdutoTests(SimpleTestCase):
    def test_bucket_pin_com_flag_fica_aguardando(self):
        d = _doc({"aprovacao_wizard_em": "2026-09-10T12:00:00+00:00"}, status="estoque_aplicado")
        d["entrada_status_efetivo"] = entrada_nfe_status_efetivo(d)
        d["entrada_financeiro_lancado"] = True
        self.assertEqual(entrada_nfe_fila_bucket_lista(d), "concluida")
        d["extra"]["aguardando_produto"] = True
        self.assertEqual(entrada_nfe_fila_bucket_lista(d), "aguardando_produto")

    def test_filtros_lista(self):
        d = _doc(
            {
                "aprovacao_wizard_em": "2026-09-10T12:00:00+00:00",
                "aguardando_produto": True,
                "aguardando_produto_txt": "falta 1",
            },
            status="estoque_aplicado",
        )
        d["entrada_status_efetivo"] = entrada_nfe_status_efetivo(d)
        d["entrada_financeiro_lancado"] = True
        item = entrada_nfe_enriquecer_doc_serializado(dict(d))
        self.assertTrue(_entrada_nfe_item_casa_filtro_lista(item, "em_andamento"))
        self.assertTrue(_entrada_nfe_item_casa_filtro_lista(item, "aguardando_produto"))
        self.assertFalse(_entrada_nfe_item_casa_filtro_lista(item, "concluida"))

    def test_pipeline_on_off_com_pin(self):
        col = FakeCollection(_doc({"aprovacao_wizard_em": "2026-09-10T12:00:00+00:00"}, status="estoque_aplicado"))
        with (
            patch("produtos.nfe_entrada_util._entrada_nota_rascunho_store", return_value=col),
            patch("produtos.nfe_entrada_util._object_id_rascunho", return_value=RID),
        ):
            on = pipeline_acao_rascunho_entrada(
                None, RID, "aguardando_produto_on", usuario="teste", texto="GM1"
            )
            self.assertTrue(on["ok"])
            self.assertTrue(col.doc["extra"]["aguardando_produto"])
            self.assertEqual(col.doc["extra"]["aguardando_produto_txt"], "GM1")
            off = pipeline_acao_rascunho_entrada(
                None, RID, "aguardando_produto_off", usuario="teste"
            )
            self.assertTrue(off["ok"])
            self.assertNotIn("aguardando_produto", col.doc["extra"])

    def test_api_on_off(self):
        col = FakeCollection(_doc({"aprovacao_wizard_em": "2026-09-10T12:00:00+00:00"}, status="estoque_aplicado"))
        factory = RequestFactory()
        user = SimpleNamespace(
            is_authenticated=True, email="t@x", pk=1, get_username=lambda: "t"
        )
        req = factory.post(
            "/api/entrada-nota/rascunho/acao/",
            data=json.dumps({"id": RID, "acao": "aguardando_produto_on", "texto": "x"}),
            content_type="application/json",
        )
        req.user = user
        with (
            patch("produtos.views._entrada_nfe_conexao", return_value=(None, object())),
            patch("produtos.views._entrada_nota_rascunho_store", return_value=col),
            patch("produtos.nfe_entrada_util._entrada_nota_rascunho_store", return_value=col),
            patch("produtos.nfe_entrada_util._object_id_rascunho", return_value=RID),
        ):
            r = api_entrada_nota_rascunho_acao(req)
        self.assertEqual(r.status_code, 200)
        body = json.loads(r.content)
        self.assertTrue(body["ok"])
        self.assertTrue(body["aguardando_produto"])
