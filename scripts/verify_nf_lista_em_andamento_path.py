"""Prova NF-LISTA-ANDAMENTO: «Em andamento» não some sem busca.

Bug: lista pegava só lim (~25) mais novas por criado_em e filtrava depois —
nota em financeiro/estoque antiga sumia até digitar na busca (scan maior).

VERIFY_OK / VERIFY_FAIL.
"""
from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta, timezone
from typing import Any
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
    if "def _entrada_nfe_item_casa_filtro_lista" not in util:
        fail("falta _entrada_nfe_item_casa_filtro_lista")
    ok("helper filtro lista")
    if "precisa_scan_largo" not in util:
        fail("falta precisa_scan_largo")
    if "max(lim * 20, 250)" not in util:
        fail("scan largo fraco demais")
    ok("scan largo com filtro de estágio")
    # filtro aplicado no loop (não só depois do lim)
    idx_loop = util.find("if not _entrada_nfe_item_casa_filtro_lista(item, f):")
    idx_fn = util.find("def listar_rascunhos_entrada(")
    if idx_fn < 0 or idx_loop < idx_fn:
        fail("filtro não está dentro do loop de listar_rascunhos_entrada")
    ok("filtro dentro do loop (preenche lim com quem casa)")


def prova_filtro_helper() -> None:
    import django

    django.setup()
    from produtos.nfe_entrada_util import (
        ENTRADA_NFE_STATUS_ESTOQUE_APLICADO,
        _entrada_nfe_item_casa_filtro_lista,
        entrada_nfe_enriquecer_doc_serializado,
    )

    fin = entrada_nfe_enriquecer_doc_serializado(
        {
            "status": ENTRADA_NFE_STATUS_ESTOQUE_APLICADO,
            "extra": {},
            "cabecalho": {"emit_nome": "Sn - Ms Comercio"},
            "linhas": [],
        }
    )
    if fin.get("entrada_lista_bucket") != "financeiro":
        fail(f"bucket esperado financeiro, veio {fin.get('entrada_lista_bucket')}")
    if not _entrada_nfe_item_casa_filtro_lista(fin, "em_andamento"):
        fail("financeiro deveria entrar em em_andamento")
    if not _entrada_nfe_item_casa_filtro_lista(fin, "financeiro"):
        fail("financeiro deveria entrar no chip financeiro")
    if _entrada_nfe_item_casa_filtro_lista(fin, "concluida"):
        fail("financeiro não deveria entrar em concluida")
    ok("helper: financeiro entra em em_andamento")


class _FakeCol:
    def __init__(self, docs: list[dict[str, Any]]) -> None:
        self.docs = list(docs)

    def aggregate(self, pipeline: list[dict[str, Any]]) -> list[dict[str, Any]]:
        lim = 100
        for st in pipeline:
            if "$limit" in st:
                lim = int(st["$limit"])
        # já ordenados por criado_em desc no fixture
        return [dict(d) for d in self.docs[:lim]]


def prova_listar_nao_some() -> None:
    import django

    django.setup()
    from produtos.nfe_entrada_util import (
        ENTRADA_NFE_STATUS_ESTOQUE_APLICADO,
        listar_rascunhos_entrada,
    )

    agora = datetime.now(timezone.utc)
    docs: list[dict[str, Any]] = []
    # 40 concluídas mais novas (só PIN) — engoliam o lim=25 antigo
    for i in range(40):
        docs.append(
            {
                "_id": f"c{i:03d}",
                "status": ENTRADA_NFE_STATUS_ESTOQUE_APLICADO,
                "cabecalho": {"emit_nome": f"Fornecedor Concluido {i}", "numero": str(1000 + i)},
                "modo": "manual",
                "extra": {
                    "financeiro_lancado": True,
                    "aprovacao_wizard_em": (agora - timedelta(hours=i)).isoformat(),
                },
                "xml_chave": "",
                "criado_em": agora - timedelta(hours=i),
                "atualizado_em": agora - timedelta(hours=i),
                "linhas": [{"produto_id": "1", "qtd": 1, "valor_unit": 10}],
            }
        )
    # nota antiga ainda em financeiro (como a MS do print)
    docs.append(
        {
            "_id": "ms-antiga",
            "status": ENTRADA_NFE_STATUS_ESTOQUE_APLICADO,
            "cabecalho": {"emit_nome": "Sn - Ms Comercio E Representa", "numero": ""},
            "modo": "manual",
            "extra": {"aviso_operacional": "faltou - veneno"},
            "xml_chave": "",
            "criado_em": agora - timedelta(days=32),
            "atualizado_em": agora - timedelta(minutes=1),
            "linhas": [{"produto_id": "9", "qtd": 1, "valor_unit": 1692.2}],
        }
    )
    # ordenar como o aggregate (criado_em desc)
    docs.sort(key=lambda d: d["criado_em"], reverse=True)
    fake = _FakeCol(docs)

    with (
        patch("produtos.nfe_entrada_util._entrada_nota_rascunho_store", return_value=fake),
        patch("produtos.agro_fonte_config.agro_entrada_nota_rascunho_postgres", return_value=False),
        patch("produtos.nfe_entrada_util.sanear_carimbo_estoque_falso_rascunho", side_effect=lambda _db, d: d),
    ):
        vazia_bug = listar_rascunhos_entrada(None, limit=25, filtro="em_andamento", busca=None)
        ids = [str(x.get("_id") or "") for x in vazia_bug]
        if "ms-antiga" not in ids:
            fail(f"em_andamento sem busca não achou ms-antiga; ids={ids}")
        ok("em_andamento sem busca acha nota antiga em financeiro")

        com_busca = listar_rascunhos_entrada(
            None, limit=25, filtro="em_andamento", busca={"q": "ms"}
        )
        ids_b = [str(x.get("_id") or "") for x in com_busca]
        if "ms-antiga" not in ids_b:
            fail(f"busca ms não achou; ids={ids_b}")
        ok("busca ms continua achando a mesma nota")


def main() -> None:
    prova_fonte()
    prova_filtro_helper()
    prova_listar_nao_some()
    print(f"VERIFY_OK {CHECKS}/{CHECKS}")


if __name__ == "__main__":
    main()
