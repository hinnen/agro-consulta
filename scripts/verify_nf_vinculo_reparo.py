#!/usr/bin/env python
"""Prova NF-VINCULO-REPARO — devolve nome colado da NF pelo histórico."""
from __future__ import annotations

import ast
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

FAIL: list[str] = []
OK = 0


def check(name: str, cond: bool, detail: str = "") -> None:
    global OK
    if cond:
        OK += 1
        print(f"  OK  {name}" + (f" — {detail}" if detail else ""))
    else:
        FAIL.append(name + (f" — {detail}" if detail else ""))
        print(f"  FAIL {name}" + (f" — {detail}" if detail else ""))


def _read(rel: str) -> str:
    return (ROOT / rel).read_text(encoding="utf-8").replace("\r\n", "\n")


def main() -> int:
    print("== AST ==")
    for rel in (
        "produtos/reparar_vinculo_nf_cadastro_util.py",
        "produtos/management/commands/reparar_cadastro_vinculo_nf.py",
    ):
        try:
            ast.parse(_read(rel))
            check(f"AST {rel}", True)
        except SyntaxError as e:
            check(f"AST {rel}", False, str(e))

    util = _read("produtos/reparar_vinculo_nf_cadastro_util.py")
    cmd = _read("produtos/management/commands/reparar_cadastro_vinculo_nf.py")
    check("comando exige --aplicar", '"--aplicar"' in cmd or "'--aplicar'" in cmd)
    check("não grava preço", "preco_venda" not in util and "preco_custo" not in util)
    check("detecta EAN no colchete", "parece_nome_nf" in util)
    check("não desfaz edição posterior", "mais_novo" in util)

    print("\n== Heurística + runtime ==")
    import django

    django.setup()
    from produtos.reparar_vinculo_nf_cadastro_util import (
        _hist_bom,
        aplicar_reparo_vinculo_nf,
        parece_nome_nf,
        planejar_reparo_vinculo_nf,
    )

    check("Ivomec NF", parece_nome_nf("IVOMEC - 50 ML [7898053773339]"))
    check("nome loja ok", not parece_nome_nf("ivomec ivermectina injetavel 50ml"))
    check("hist lixo", not _hist_bom("[NOME QUEBRADO]"))
    check("hist bom", _hist_bom("ivomec ivermectina injetavel 50ml boehringer"))

    print("\n== Runtime ==")
    from decimal import Decimal

    from produtos.models import Produto, ProdutoCadastroAlteracaoAgro, ProdutoGestaoOverlayAgro

    pid = "verify-reparo-vinculo-nf"
    ProdutoCadastroAlteracaoAgro.objects.filter(produto_externo_id=pid).delete()
    ProdutoGestaoOverlayAgro.objects.filter(produto_externo_id=pid).delete()
    Produto.objects.filter(produto_externo_id=pid).delete()
    Produto.objects.create(
        produto_externo_id=pid,
        codigo_interno="1097",
        codigo_nfe="GM1097",
        nome="IVOMEC - 50 ML [7898053773339]",
        marca="",
        categoria="",
        unidade="UN",
        custo=Decimal("27.90"),
        preco_venda=Decimal("39.90"),
    )
    ProdutoGestaoOverlayAgro.objects.create(
        produto_externo_id=pid,
        nome="IVOMEC - 50 ML [7898053773339]",
    )
    ProdutoCadastroAlteracaoAgro.objects.create(
        produto_externo_id=pid,
        campo="nome",
        campo_label="Nome",
        valor_antes="ivomec ivermectina injetavel 50ml boehringer ingelheim",
        valor_depois="—",
        origem="gestao",
    )
    ProdutoCadastroAlteracaoAgro.objects.create(
        produto_externo_id=pid,
        campo="marca",
        campo_label="Marca",
        valor_antes="BOEHRINGER INGELHEIM",
        valor_depois="—",
        origem="gestao",
    )
    planos = planejar_reparo_vinculo_nf(pid=pid)
    check("planeja 1 produto", len(planos) == 1)
    check("volta nome loja", (planos[0].get("nome_volta") or "").startswith("ivomec ivermectina"))
    n = aplicar_reparo_vinculo_nf(planos)
    p = Produto.objects.get(produto_externo_id=pid)
    ov = ProdutoGestaoOverlayAgro.objects.get(produto_externo_id=pid)
    check("aplicou", n == 1)
    check("nome PG restaurado", p.nome.startswith("ivomec ivermectina"))
    check("overlay nome limpo", (ov.nome or "").strip() == "")
    check("marca restaurada", p.marca == "BOEHRINGER INGELHEIM")
    check("preço venda intacto", p.preco_venda == Decimal("39.90"))

    try:
        ProdutoCadastroAlteracaoAgro.objects.filter(produto_externo_id=pid).delete()
        ProdutoGestaoOverlayAgro.objects.filter(produto_externo_id=pid).delete()
        Produto.objects.filter(produto_externo_id=pid).delete()
    except Exception:
        pass

    print(f"\n{'VERIFY_OK' if not FAIL else 'VERIFY_FAIL'}  {OK} ok · {len(FAIL)} fail")
    for f in FAIL:
        print(f"  - {f}")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
