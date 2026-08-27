import copy
import json
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import patch

from django.test import RequestFactory, SimpleTestCase

from produtos.nfe_entrada_util import (
    claim_rascunho_para_estoque_agro,
    entrada_nfe_extra_financeiro_ok,
    liberar_rascunho_entrada_para_estoque_pendente,
    marcar_rascunho_estoque_aplicado,
    reverter_integracao_entrada_nota_para_reabertura,
)
from produtos.views import api_entrada_nota_financeiro, api_entrada_nota_reabrir_nota


RID = "a" * 24


def _get_nested(doc, path):
    cur = doc
    for part in path.split("."):
        if not isinstance(cur, dict) or part not in cur:
            return None, False
        cur = cur[part]
    return cur, True


def _set_nested(doc, path, value):
    parts = path.split(".")
    cur = doc
    for part in parts[:-1]:
        cur = cur.setdefault(part, {})
    cur[parts[-1]] = value


def _unset_nested(doc, path):
    parts = path.split(".")
    cur = doc
    for part in parts[:-1]:
        cur = cur.get(part, {})
    if isinstance(cur, dict):
        cur.pop(parts[-1], None)


class FakeCollection:
    def __init__(self, doc):
        self.doc = copy.deepcopy(doc)
        self.update_calls = []

    def find_one(self, filt, projection=None):
        if str(filt.get("_id")) != str(self.doc.get("_id")):
            return None
        return copy.deepcopy(self.doc)

    def _matches(self, filt):
        if str(filt.get("_id")) != str(self.doc.get("_id")):
            return False
        status = filt.get("status")
        if isinstance(status, dict) and "$nin" in status:
            if self.doc.get("status") in status["$nin"]:
                return False
        clauses = filt.get("$or")
        if clauses:
            lock, exists = _get_nested(self.doc, "extra.estoque_agro_lock")
            matched = False
            for clause in clauses:
                cond = clause.get("extra.estoque_agro_lock")
                if isinstance(cond, dict) and cond.get("$exists") is False and not exists:
                    matched = True
                elif cond is None and lock is None:
                    matched = True
                elif isinstance(cond, dict) and "$lte" in cond and lock:
                    try:
                        matched = datetime.fromisoformat(str(lock)) <= cond["$lte"]
                    except ValueError:
                        matched = False
            if not matched:
                return False
        return True

    def update_one(self, filt, update):
        self.update_calls.append(copy.deepcopy((filt, update)))
        if not self._matches(filt):
            return SimpleNamespace(matched_count=0, modified_count=0)
        for path, value in update.get("$set", {}).items():
            _set_nested(self.doc, path, value)
        for path in update.get("$unset", {}):
            _unset_nested(self.doc, path)
        return SimpleNamespace(matched_count=1, modified_count=1)


def _doc(extra=None, *, status="pronta"):
    return {
        "_id": RID,
        "status": status,
        "cabecalho": {
            "emit_nome": "FORNECEDOR TESTE",
            "numero": "123",
            "data_entrada": "2026-08-27",
            "plano_conta": "COMPRA MERCADORIA SN",
            "empresa_faturada_id": "1",
            "deposito_entrada": "centro",
        },
        "linhas": [{"produto_id": "P1", "x_prod": "ITEM", "q_estoque": 2}],
        "extra": {
            "aprovacao_wizard_em": "2026-08-27T12:00:00+00:00",
            "aprovacao_wizard_usuario": "operador",
            "wizard_etapa2_confirmada_em": "ok",
            "wizard_etapa3_confirmada_em": "ok",
            **(extra or {}),
        },
    }


class EntradaNfReaberturaEstoqueTests(SimpleTestCase):
    def _liberar(self, col):
        with (
            patch("produtos.nfe_entrada_util._entrada_nota_rascunho_store", return_value=col),
            patch("produtos.nfe_entrada_util._object_id_rascunho", return_value=RID),
        ):
            return liberar_rascunho_entrada_para_estoque_pendente(None, RID, usuario="teste")

    def test_preserva_financeiro_quitado_ou_com_movimento_sem_excluir(self):
        financeiro = {
            "financeiro_lancado": True,
            "financeiro_ids": ["titulo-quitado"],
            "financeiro_lancado_em": "2026-08-27T10:00:00+00:00",
            "financeiro_ui": {"forma": "BOLETO", "parcelas": [100]},
        }
        col = FakeCollection(_doc(financeiro))
        antes = copy.deepcopy(col.doc["extra"])
        with patch(
            "produtos.lancamentos_financeiro_pg_write_util.excluir_lancamento_dispatch"
        ) as excluir:
            out = self._liberar(col)
        self.assertTrue(out["ok"])
        excluir.assert_not_called()
        depois = col.doc["extra"]
        for campo in ("financeiro_lancado", "financeiro_ids", "financeiro_lancado_em", "financeiro_ui"):
            self.assertEqual(depois[campo], antes[campo])
        self.assertNotIn("aprovacao_wizard_em", depois)
        self.assertTrue(depois["estoque_pendente_liberado_em"])
        self.assertTrue(entrada_nfe_extra_financeiro_ok(depois))

    def test_sem_financeiro_tambem_libera(self):
        col = FakeCollection(_doc())
        out = self._liberar(col)
        self.assertTrue(out["ok"])
        self.assertTrue(out["financeiro_preservado"])

    def test_rejeita_estoque_existente_descartada_e_rascunho_invalido(self):
        casos = [
            _doc({"estoque_agro_ajuste_ids": [9]}),
            _doc({"estoque_agro_ajuste_ids": "9"}),
            _doc({"estoque_aplicado_em": "2026-08-27T12:00:00+00:00"}),
            _doc(status="descartada"),
            _doc(status="encerrada"),
            {**_doc(), "cabecalho": {}},
        ]
        for doc in casos:
            col = FakeCollection(doc)
            antes = copy.deepcopy(col.doc)
            out = self._liberar(col)
            self.assertFalse(out["ok"])
            self.assertEqual(col.doc, antes)

    def test_liberacao_seguida_de_estoque_tem_claim_unico_e_preserva_financeiro(self):
        col = FakeCollection(_doc({"financeiro_lancado": True, "financeiro_ids": ["fin-1"]}))
        self.assertTrue(self._liberar(col)["ok"])
        with (
            patch("produtos.nfe_entrada_util._entrada_nota_rascunho_store", return_value=col),
            patch("produtos.nfe_entrada_util._object_id_rascunho", return_value=RID),
        ):
            self.assertTrue(claim_rascunho_para_estoque_agro(None, RID)["ok"])
            self.assertTrue(marcar_rascunho_estoque_aplicado(None, RID, usuario="teste")["ok"])
            repetido = claim_rascunho_para_estoque_agro(None, RID)
        self.assertFalse(repetido["ok"])
        self.assertEqual(col.doc["extra"]["financeiro_ids"], ["fin-1"])
        self.assertNotIn("estoque_pendente_liberado_em", col.doc["extra"])
        self.assertTrue(entrada_nfe_extra_financeiro_ok(col.doc["extra"]))

    def test_reabertura_completa_continua_tentando_excluir_e_bloqueia_falha(self):
        col = FakeCollection(_doc({"financeiro_lancado": True, "financeiro_ids": ["quitado"]}))
        antes = copy.deepcopy(col.doc)
        with (
            patch("produtos.nfe_entrada_util._entrada_nota_rascunho_store", return_value=col),
            patch("produtos.nfe_entrada_util._object_id_rascunho", return_value=RID),
            patch(
                "produtos.lancamentos_financeiro_pg_write_util.excluir_lancamento_dispatch",
                return_value={"ok": False, "erro": "quitado ou com movimento"},
            ) as excluir,
        ):
            out = reverter_integracao_entrada_nota_para_reabertura(None, RID, usuario="teste")
        self.assertFalse(out["ok"])
        excluir.assert_called_once()
        self.assertEqual(col.doc, antes)

    @patch("produtos.views.liberar_rascunho_entrada_para_estoque_pendente")
    @patch("produtos.views._entrada_nfe_rascunho_db_ok", return_value=True)
    @patch("produtos.views._entrada_nfe_conexao", return_value=(None, object()))
    @patch("produtos.views._emprestimos_interno_validar_pin", return_value=(False, "PIN inválido"))
    def test_pin_incorreto_rejeita_sem_chamar_recuperacao(self, _pin, _con, _db_ok, liberar):
        request = RequestFactory().post(
            "/api/entrada-nota/reabrir/",
            data=json.dumps({"rascunho_id": RID, "pin": "0000", "escopo": "estoque_pendente"}),
            content_type="application/json",
        )
        request.user = SimpleNamespace(is_authenticated=True)
        response = api_entrada_nota_reabrir_nota(request)
        self.assertEqual(response.status_code, 403)
        liberar.assert_not_called()

    def test_botao_especial_envia_escopo_explicito(self):
        from pathlib import Path

        html = Path("produtos/templates/produtos/entrada_nota.html").read_text(encoding="utf-8")
        self.assertIn("entradaNfeReabrirNota('estoque_pendente')", html)
        self.assertIn("entradaNfeReabrirNota('completo')", html)
        self.assertIn("body: JSON.stringify({ rascunho_id: rid, pin, escopo })", html)

    @patch("produtos.views._invalidar_caches_apos_ajuste_pin")
    @patch("produtos.views.reverter_integracao_entrada_nota_para_reabertura")
    @patch("produtos.views.liberar_rascunho_entrada_para_estoque_pendente")
    @patch("produtos.views._entrada_nfe_rascunho_db_ok", return_value=True)
    @patch("produtos.views._entrada_nfe_conexao", return_value=(None, object()))
    @patch("produtos.views._emprestimos_interno_validar_pin", return_value=(True, ""))
    def test_api_separa_recuperacao_limitada_da_completa(
        self, _pin, _con, _db_ok, limitada, completa, _cache
    ):
        limitada.return_value = {"ok": True, "escopo": "estoque_pendente"}
        completa.return_value = {"ok": True, "escopo": "completo"}
        user = SimpleNamespace(
            is_authenticated=True, email="teste@local", pk=1, get_username=lambda: "teste"
        )
        factory = RequestFactory()

        req_lim = factory.post(
            "/api/entrada-nota/reabrir/",
            data=json.dumps({"rascunho_id": RID, "pin": "1234", "escopo": "estoque_pendente"}),
            content_type="application/json",
        )
        req_lim.user = user
        self.assertEqual(api_entrada_nota_reabrir_nota(req_lim).status_code, 200)
        limitada.assert_called_once()
        completa.assert_not_called()

        req_full = factory.post(
            "/api/entrada-nota/reabrir/",
            data=json.dumps({"rascunho_id": RID, "pin": "1234", "escopo": "completo"}),
            content_type="application/json",
        )
        req_full.user = user
        self.assertEqual(api_entrada_nota_reabrir_nota(req_full).status_code, 200)
        completa.assert_called_once()

    @patch("produtos.views.sincronizar_financeiro_rascunho_entrada_nfe")
    @patch("produtos.views.normalizar_cabecalho_emit_fornecedor_entrada_nfe", side_effect=lambda db, col, cab: cab)
    @patch("produtos.views._object_id_rascunho", return_value=RID)
    @patch("produtos.views._entrada_nota_rascunho_store")
    @patch("produtos.views._entrada_nfe_rascunho_db_ok", return_value=True)
    @patch(
        "produtos.views._entrada_nfe_conexao",
        return_value=(SimpleNamespace(col_c="DtoPessoa"), object()),
    )
    def test_etapa_financeiro_reconhece_ids_preservados_sem_criar_duplicata(
        self, _con, _db_ok, store, _oid, _norm, sincronizar
    ):
        store.return_value.find_one.return_value = _doc(
            {"financeiro_lancado": True, "financeiro_ids": ["fin-existente"]}
        )
        request = RequestFactory().post(
            "/api/entrada-nota/financeiro/",
            data=json.dumps(
                {
                    "rascunho_id": RID,
                    "cabecalho": {"emit_nome": "FORNECEDOR"},
                    "linhas": [{"produto_id": "P1", "q_estoque": 2}],
                    "financeiro": {},
                }
            ),
            content_type="application/json",
        )
        request.user = SimpleNamespace(
            is_authenticated=True, email="teste@local", pk=1, get_username=lambda: "teste"
        )
        response = api_entrada_nota_financeiro(request)
        self.assertEqual(response.status_code, 200)
        body = json.loads(response.content)
        self.assertTrue(body["financeiro"]["ja_existia"])
        self.assertEqual(body["financeiro"]["ids"], ["fin-existente"])
        sincronizar.assert_not_called()
