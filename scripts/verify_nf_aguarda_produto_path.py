"""Prova NF-AGUARDA-PRODUTO: fornecedor deve produto → Em andamento até «Chegou».

VERIFY_OK / VERIFY_FAIL.
"""
from __future__ import annotations

import os
import re
import subprocess
import sys
from unittest.mock import MagicMock

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
    if "_entrada_nfe_extra_aguardando_produto" not in util:
        fail("sem helper aguardando_produto")
    if 'return "aguardando_produto"' not in util:
        fail("bucket nao retorna aguardando_produto")
    if '"aguardando_produto"' not in util or "em_andamento" not in util:
        fail("filtro Em andamento sem aguardando_produto")
    if "aguardando_produto_on" not in util or "aguardando_produto_off" not in util:
        fail("pipeline sem acoes on/off")
    ok("backend: flag + bucket + acoes")
    if 'data-filtro="aguardando_produto"' not in html:
        fail("chip Deve produto ausente")
    if "entradaNfeAbrirModalDeveProduto" not in html:
        fail("modal Deve produto ausente")
    if "aguardando_produto_off" not in html or "Chegou" not in html:
        fail("botao Chegou ausente")
    if "Deve produto" not in html:
        fail("rotulo Deve produto ausente na lista")
    ok("UI: chip + modal + Chegou/Deve produto")


def prova_logica() -> None:
    import django

    django.setup()
    from produtos.nfe_entrada_util import (
        _entrada_nfe_item_casa_filtro_lista,
        entrada_nfe_enriquecer_doc_serializado,
        entrada_nfe_fila_bucket_lista,
        pipeline_acao_rascunho_entrada,
    )
    from produtos.tests_entrada_nf_reabertura_estoque import FakeCollection, RID, _doc

    d = _doc({"aprovacao_wizard_em": "2026-09-10T12:00:00+00:00"}, status="estoque_aplicado")
    d["entrada_financeiro_lancado"] = True
    d["entrada_status_efetivo"] = "estoque_aplicado"
    # Sem flag: concluida
    enriched = entrada_nfe_enriquecer_doc_serializado(dict(d))
    if enriched.get("entrada_lista_bucket") != "concluida":
        # pode depender de status efetivo recalculado
        d2 = dict(d)
        d2["extra"] = dict(d["extra"])
        # force via enrich after setting status fields like serialize does
        from produtos.nfe_entrada_util import entrada_nfe_status_efetivo

        d2["entrada_status_efetivo"] = entrada_nfe_status_efetivo(d2)
        d2["entrada_financeiro_lancado"] = True
        bk = entrada_nfe_fila_bucket_lista(d2)
        if bk != "concluida":
            # PIN in extra should make final_ok
            if not d2["extra"].get("aprovacao_wizard_em"):
                fail("doc teste sem PIN")
            fail(f"sem flag esperava concluida, veio {bk}")
    ok("sem flag + PIN = concluida")

    d["extra"]["aguardando_produto"] = True
    d["extra"]["aguardando_produto_txt"] = "1 cx racao"
    d["entrada_status_efetivo"] = "estoque_aplicado"
    d["entrada_financeiro_lancado"] = True
    bk2 = entrada_nfe_fila_bucket_lista(d)
    if bk2 != "aguardando_produto":
        fail(f"com flag esperava aguardando_produto, veio {bk2}")
    ok("com flag + PIN = aguardando_produto")

    item = entrada_nfe_enriquecer_doc_serializado(dict(d))
    if not _entrada_nfe_item_casa_filtro_lista(item, "em_andamento"):
        fail("nao aparece em Em andamento")
    if not _entrada_nfe_item_casa_filtro_lista(item, "aguardando_produto"):
        fail("nao aparece no chip Deve produto")
    if _entrada_nfe_item_casa_filtro_lista(item, "concluida"):
        fail("ainda aparece em Concluida")
    ok("filtros: Em andamento sim / Concluida nao")

    # pipeline on/off
    col = FakeCollection(_doc({}, status="estoque_aplicado"))
    col.doc["extra"]["aprovacao_wizard_em"] = "2026-09-10T12:00:00+00:00"
    with (
        __import__("unittest.mock", fromlist=["patch"]).patch(
            "produtos.nfe_entrada_util._entrada_nota_rascunho_store", return_value=col
        ),
        __import__("unittest.mock", fromlist=["patch"]).patch(
            "produtos.nfe_entrada_util._object_id_rascunho", return_value=RID
        ),
    ):
        r_on = pipeline_acao_rascunho_entrada(
            None, RID, "aguardando_produto_on", usuario="Renan", texto="1 cx"
        )
        if not r_on.get("ok") or not r_on.get("aguardando_produto"):
            fail(f"on falhou: {r_on}")
        if not col.doc["extra"].get("aguardando_produto"):
            fail("flag nao gravada")
        if col.doc["extra"].get("aguardando_produto_txt") != "1 cx":
            fail("texto nao gravado")
        r_off = pipeline_acao_rascunho_entrada(
            None, RID, "aguardando_produto_off", usuario="Renan"
        )
        if not r_off.get("ok") or r_off.get("aguardando_produto"):
            fail(f"off falhou: {r_off}")
        if col.doc["extra"].get("aguardando_produto"):
            fail("flag nao removida")
    ok("pipeline on/off grava e limpa (mesmo com PIN)")


def main() -> None:
    prova_fonte()
    prova_logica()
    print(f"VERIFY_OK {CHECKS}/{CHECKS}")


if __name__ == "__main__":
    main()
