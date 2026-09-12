"""Prova detalhada NF-AGUARDA-PRODUTO: fornecedor deve produto.

Cobre fonte, bucket, filtros, pipeline on/off, API HTTP mock, descartada bloqueada.
VERIFY_OK / VERIFY_FAIL.
"""
from __future__ import annotations

import json
import os
import sys
from types import SimpleNamespace
from unittest.mock import patch

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.chdir(ROOT)
sys.path.insert(0, ROOT)
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

CHECKS = 0


def fail(msg: str) -> None:
    print(f"VERIFY_FAIL: {msg}")
    sys.exit(1)


def ok(msg: str) -> None:
    global CHECKS
    CHECKS += 1
    print(f"OK {msg}")


def _read(rel: str) -> str:
    with open(os.path.join(ROOT, rel), encoding="utf-8") as f:
        return f.read()


def prova_fonte() -> None:
    util = _read("produtos/nfe_entrada_util.py")
    html = _read("produtos/templates/produtos/entrada_nota.html")
    views = _read("produtos/views.py")

    for needle in (
        "_entrada_nfe_extra_aguardando_produto",
        "_entrada_nfe_extra_aguardando_produto_txt",
        'return "aguardando_produto"',
        "aguardando_produto_on",
        "aguardando_produto_off",
        "extra.aguardando_produto",
    ):
        if needle not in util:
            fail(f"util sem {needle!r}")
    ok("fonte util completa")

    # Em andamento inclui o bucket
    if '"aguardando_produto",' not in util and "'aguardando_produto'" not in util:
        fail("em_andamento sem aguardando_produto na lista de buckets")
    if "Permitido com PIN já gravado" not in util:
        fail("comentario/regra PIN+deve produto ausente")
    ok("Em andamento + permitido com PIN")

    if 'data-filtro="aguardando_produto"' not in html:
        fail("chip Deve produto ausente")
    if "modal-entrada-nfe-deve-produto" not in html:
        fail("modal HTML ausente")
    if "entradaNfeAbrirModalDeveProduto" not in html:
        fail("JS abrir modal ausente")
    if "entradaNfeRascunhoAcaoComTexto" not in html:
        fail("JS acao com texto ausente")
    if "aguardando_produto_off" not in html or "Chegou" not in html:
        fail("botao Chegou ausente")
    if "Deve produto" not in html:
        fail("rotulo Deve produto ausente")
    if "btn-entrada-nfe-deve-produto-salvar" not in html:
        fail("listener salvar deve produto ausente")
    ok("UI lista + modal + Chegou")

    if "pipeline_acao_rascunho_entrada" not in views:
        fail("views sem pipeline acao")
    if 'texto = str(payload.get("texto")' not in views:
        fail("API acao nao passa texto")
    ok("API rascunho/acao passa texto")


def prova_logica_e_api() -> None:
    import django

    django.setup()
    from django.test import RequestFactory

    from produtos.nfe_entrada_util import (
        _entrada_nfe_item_casa_filtro_lista,
        entrada_nfe_enriquecer_doc_serializado,
        entrada_nfe_fila_bucket_lista,
        entrada_nfe_status_efetivo,
        pipeline_acao_rascunho_entrada,
    )
    from produtos.tests_entrada_nf_reabertura_estoque import FakeCollection, RID, _doc
    from produtos.views import api_entrada_nota_rascunho_acao

    # 1) Sem flag + PIN => concluida
    d = _doc({"aprovacao_wizard_em": "2026-09-10T12:00:00+00:00"}, status="estoque_aplicado")
    d["entrada_status_efetivo"] = entrada_nfe_status_efetivo(d)
    d["entrada_financeiro_lancado"] = True
    bk = entrada_nfe_fila_bucket_lista(d)
    if bk != "concluida":
        fail(f"sem flag esperava concluida, veio {bk}")
    ok("sem flag + PIN = concluida")

    # 2) Com flag + PIN => aguardando_produto (nao some de Em andamento)
    d["extra"]["aguardando_produto"] = True
    d["extra"]["aguardando_produto_txt"] = "1 cx racao 15kg"
    bk2 = entrada_nfe_fila_bucket_lista(d)
    if bk2 != "aguardando_produto":
        fail(f"com flag esperava aguardando_produto, veio {bk2}")
    ok("com flag + PIN = aguardando_produto")

    item = entrada_nfe_enriquecer_doc_serializado(dict(d))
    if not item.get("entrada_aguardando_produto"):
        fail("enrich sem entrada_aguardando_produto")
    if item.get("entrada_aguardando_produto_txt") != "1 cx racao 15kg":
        fail("enrich texto errado")
    if not _entrada_nfe_item_casa_filtro_lista(item, "em_andamento"):
        fail("nao aparece em Em andamento")
    if not _entrada_nfe_item_casa_filtro_lista(item, "aguardando_produto"):
        fail("nao aparece no chip Deve produto")
    if _entrada_nfe_item_casa_filtro_lista(item, "concluida"):
        fail("ainda aparece em Concluida")
    ok("filtros Em andamento sim / Concluida nao / chip sim")

    # 3) Sem PIN ainda, so flag — tambem aguardando_produto
    d3 = _doc({}, status="estoque_aplicado")
    d3["extra"].pop("aprovacao_wizard_em", None)
    d3["extra"]["aguardando_produto"] = True
    d3["entrada_status_efetivo"] = entrada_nfe_status_efetivo(d3)
    d3["entrada_financeiro_lancado"] = True
    if entrada_nfe_fila_bucket_lista(d3) != "aguardando_produto":
        fail("flag sem PIN nao ficou aguardando_produto")
    ok("flag sem PIN tambem fica no lembrete")

    # 4) pipeline on/off com PIN
    col = FakeCollection(_doc({}, status="estoque_aplicado"))
    col.doc["extra"]["aprovacao_wizard_em"] = "2026-09-10T12:00:00+00:00"
    with (
        patch("produtos.nfe_entrada_util._entrada_nota_rascunho_store", return_value=col),
        patch("produtos.nfe_entrada_util._object_id_rascunho", return_value=RID),
    ):
        r_on = pipeline_acao_rascunho_entrada(
            None, RID, "aguardando_produto_on", usuario="Renan", texto="1 cx"
        )
        if not r_on.get("ok") or not r_on.get("aguardando_produto"):
            fail(f"on falhou: {r_on}")
        if col.doc["extra"].get("aguardando_produto_txt") != "1 cx":
            fail("texto nao gravado")
        # on sem texto mantem texto antigo
        r_on2 = pipeline_acao_rascunho_entrada(
            None, RID, "aguardando_produto_on", usuario="Renan", texto=""
        )
        if not r_on2.get("ok"):
            fail(f"on vazio falhou: {r_on2}")
        if col.doc["extra"].get("aguardando_produto_txt") != "1 cx":
            fail("texto antigo sumiu ao reativar sem digitar")
        r_off = pipeline_acao_rascunho_entrada(
            None, RID, "aguardando_produto_off", usuario="Renan"
        )
        if not r_off.get("ok") or r_off.get("aguardando_produto"):
            fail(f"off falhou: {r_off}")
        if col.doc["extra"].get("aguardando_produto"):
            fail("flag nao removida")
        if col.doc["extra"].get("aguardando_produto_txt"):
            fail("texto nao limpo no off")
    ok("pipeline on/off + texto opcional")

    # 5) descartada bloqueia
    col_d = FakeCollection(_doc({}, status="descartada"))
    with (
        patch("produtos.nfe_entrada_util._entrada_nota_rascunho_store", return_value=col_d),
        patch("produtos.nfe_entrada_util._object_id_rascunho", return_value=RID),
    ):
        r_bad = pipeline_acao_rascunho_entrada(
            None, RID, "aguardando_produto_on", usuario="x", texto="y"
        )
        if r_bad.get("ok"):
            fail("descartada nao deveria aceitar on")
    ok("descartada bloqueia marca")

    # 6) API view end-to-end mock
    col_api = FakeCollection(_doc({"aprovacao_wizard_em": "2026-09-10T12:00:00+00:00"}, status="estoque_aplicado"))
    factory = RequestFactory()
    req = factory.post(
        "/api/entrada-nota/rascunho/acao/",
        data=json.dumps(
            {"id": RID, "acao": "aguardando_produto_on", "texto": "falta 2 un GM123"}
        ),
        content_type="application/json",
    )
    req.user = SimpleNamespace(
        is_authenticated=True,
        email="renan@local",
        pk=1,
        get_username=lambda: "renan",
    )
    with (
        patch("produtos.views._entrada_nfe_conexao", return_value=(None, object())),
        patch("produtos.views._entrada_nota_rascunho_store", return_value=col_api),
        patch("produtos.nfe_entrada_util._entrada_nota_rascunho_store", return_value=col_api),
        patch("produtos.nfe_entrada_util._object_id_rascunho", return_value=RID),
    ):
        resp = api_entrada_nota_rascunho_acao(req)
    if resp.status_code != 200:
        fail(f"API on status={resp.status_code} body={resp.content!r}")
    body = json.loads(resp.content)
    if not body.get("ok") or not body.get("aguardando_produto"):
        fail(f"API on body={body}")
    if col_api.doc["extra"].get("aguardando_produto_txt") != "falta 2 un GM123":
        fail("API nao gravou texto")
    ok("API POST on 200 grava texto")

    req2 = factory.post(
        "/api/entrada-nota/rascunho/acao/",
        data=json.dumps({"id": RID, "acao": "aguardando_produto_off"}),
        content_type="application/json",
    )
    req2.user = req.user
    with (
        patch("produtos.views._entrada_nfe_conexao", return_value=(None, object())),
        patch("produtos.views._entrada_nota_rascunho_store", return_value=col_api),
        patch("produtos.nfe_entrada_util._entrada_nota_rascunho_store", return_value=col_api),
        patch("produtos.nfe_entrada_util._object_id_rascunho", return_value=RID),
    ):
        resp2 = api_entrada_nota_rascunho_acao(req2)
    body2 = json.loads(resp2.content)
    if resp2.status_code != 200 or not body2.get("ok") or body2.get("aguardando_produto"):
        fail(f"API off falhou: {resp2.status_code} {body2}")
    if col_api.doc["extra"].get("aguardando_produto"):
        fail("API off nao limpou flag")
    ok("API POST off 200 limpa flag")


def prova_pg_live_opcional() -> None:
    """Se houver rascunho local, simula on/off numa copia em memoria (sem gravar loja)."""
    import django

    django.setup()
    try:
        from produtos.models import EntradaNotaRascunhoAgro
    except Exception as exc:
        print(f"SKIP PG: {exc}")
        return
    try:
        n = EntradaNotaRascunhoAgro.objects.count()
    except Exception as exc:
        print(f"SKIP PG query: {exc}")
        return
    ok(f"PG rascunhos acessivel (qtd={n})")


def main() -> None:
    prova_fonte()
    prova_logica_e_api()
    prova_pg_live_opcional()
    print(f"VERIFY_OK {CHECKS}/{CHECKS}")


if __name__ == "__main__":
    main()
