#!/usr/bin/env python
"""Prova NF-VINCULO-NAO-SOBRESCREVE — vínculo NF não apaga nome do cadastro."""
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
        "produtos/catalogo_agro.py",
        "produtos/cadastro_alteracao_historico_util.py",
        "produtos/models.py",
        "produtos/views.py",
        "produtos/tests_nf_vinculo_nao_sobrescreve.py",
    ):
        try:
            ast.parse(_read(rel))
            check(f"AST {rel}", True)
        except SyntaxError as e:
            check(f"AST {rel}", False, str(e))

    html = _read("produtos/templates/produtos/entrada_nota.html")
    cat = _read("produtos/catalogo_agro.py")
    views = _read("produtos/views.py")
    models = _read("produtos/models.py")
    hist = _read("produtos/cadastro_alteracao_historico_util.py")

    print("\n== Entrada NF (JS) ==")
    check(
        "lembrar cProd origem_entrada_nf",
        html.split("async function entradaNfeLembrarCProdAposPick", 1)[-1]
        .split("async function entradaNfeLembrarEanEmbalagemAposPick", 1)[0]
        .count("origem_entrada_nf: true")
        >= 1,
    )
    ean_fn = html.split("async function entradaNfeLembrarEanEmbalagemAposPick", 1)[-1].split(
        "/** Reaplica V. unit", 1
    )[0]
    check("lembrar EAN origem_entrada_nf", "origem_entrada_nf: true" in ean_fn)

    print("\n== Sync cadastro ==")
    check("helper payload_overlay_deve_sincronizar_produto", "def payload_overlay_deve_sincronizar_produto" in cat)
    check("c_prod_nf fora da lista de sync", '"c_prod_nf"' not in cat.split("_PAYLOAD_KEYS_SYNC_PRODUTO", 1)[-1].split(")", 1)[0])
    check(
        "overlay salvar pula sync se só vínculo",
        "payload_overlay_deve_sincronizar_produto" in views,
    )
    check(
        "sync não apaga nome se chave ausente",
        'if "nome" in keys:' in cat and "Overlay vazio não apaga cadastro existente" in cat,
    )

    print("\n== Histórico + lote ==")
    check(
        "histórico depois também enriquece catálogo",
        "hist_depois = enriquecer_snapshot_antes_com_catalogo(pid, snapshot_overlay(ov))" in views,
    )
    check('rótulo Cód. produto na NF', '("c_prod_nf", "Cód. produto na NF")' in hist)
    lote_fn = models.split("def registrar_lote_validade_apos_entrada_nf", 1)[-1].split(
        "def reduzir_lote_validade_estorno_entrada_nf", 1
    )[0]
    check("lote não grava overlay.nome", "ov.nome =" not in lote_fn and 'defaults={"nome"' not in lote_fn)
    check(
        "estoque usa cadastro antes do xProd",
        'nome_p = str((doc or {}).get("Nome") or "")[:200]' in views
        and 'nome_p = str(ln.get("x_prod") or "")[:200]' in views,
    )

    print("\n== Runtime Postgres ==")
    import django

    django.setup()
    from decimal import Decimal

    from produtos.cadastro_alteracao_historico_util import (
        enriquecer_snapshot_antes_com_catalogo,
        registrar_diffs_cadastro,
        snapshot_overlay,
    )
    from produtos.catalogo_agro import (
        payload_overlay_deve_sincronizar_produto,
        sincronizar_modelo_produto_de_overlay,
    )
    from produtos.models import (
        Produto,
        ProdutoCadastroAlteracaoAgro,
        ProdutoGestaoOverlayAgro,
        registrar_lote_validade_apos_entrada_nf,
    )

    pid = "verify-vinculo-nao-sobrescreve"
    nome = "ivomec ivermectina injetavel 50ml boehringer ingelheim"
    ProdutoCadastroAlteracaoAgro.objects.filter(produto_externo_id=pid).delete()
    ProdutoGestaoOverlayAgro.objects.filter(produto_externo_id=pid).delete()
    Produto.objects.filter(produto_externo_id=pid).delete()
    p = Produto.objects.create(
        produto_externo_id=pid,
        codigo_interno="1097",
        codigo_nfe="GM1097",
        codigo_barras="7898053772789",
        nome=nome,
        marca="BOEHRINGER INGELHEIM",
        categoria="Medicamentos",
        unidade="UN",
        custo=Decimal("27.90"),
        preco_venda=Decimal("39.90"),
    )
    ov = ProdutoGestaoOverlayAgro.objects.create(produto_externo_id=pid)
    check(
        "cProd não é sync de cadastro",
        not payload_overlay_deve_sincronizar_produto(
            {"produto_id": pid, "c_prod_nf": "199", "origem_entrada_nf": True}
        ),
    )
    sincronizar_modelo_produto_de_overlay(
        pid, ov, payload={"produto_id": pid, "c_prod_nf": "199", "origem_entrada_nf": True}
    )
    p.refresh_from_db()
    check("vínculo preserva nome", p.nome == nome)
    check("vínculo preserva marca", p.marca == "BOEHRINGER INGELHEIM")
    check("vínculo preserva categoria", p.categoria == "Medicamentos")
    check("vínculo preserva GM", p.codigo_nfe == "GM1097")
    check("vínculo preserva venda", p.preco_venda == Decimal("39.90"))

    sincronizar_modelo_produto_de_overlay(pid, ov, custo_payload=Decimal("14.45"))
    p.refresh_from_db()
    check("custo NF só muda custo", p.custo == Decimal("14.45") and p.nome == nome)

    registrar_lote_validade_apos_entrada_nf(
        pid,
        {"lote_numero": "L1", "lote_validade": "2027-08-11"},
        Decimal("6"),
        nome_produto="IVOMEC - 50 ML [7898053773339]",
        deposito="centro",
    )
    ov.refresh_from_db()
    p.refresh_from_db()
    check("lote não copia xProd no overlay.nome", (ov.nome or "").strip() == "")
    check("lote não troca Produto.nome", p.nome == nome)

    hist_antes = enriquecer_snapshot_antes_com_catalogo(pid, snapshot_overlay(ov))
    ov.cadastro_extras = dict(ov.cadastro_extras or {})
    ov.cadastro_extras["entrada_nfe_c_prods"] = ["199"]
    hist_depois = enriquecer_snapshot_antes_com_catalogo(pid, snapshot_overlay(ov))
    registrar_diffs_cadastro(
        produto_id=pid, antes=hist_antes, depois=hist_depois, origem="nf"
    )
    campos = set(
        ProdutoCadastroAlteracaoAgro.objects.filter(produto_externo_id=pid).values_list(
            "campo", flat=True
        )
    )
    check("histórico grava cProd", "c_prod_nf" in campos)
    check("histórico não finge apagar nome", "nome" not in campos)
    check("histórico não finge apagar marca", "marca" not in campos)

    ov.nome = "Ivomec 50ml loja"
    ov.save(update_fields=["nome", "atualizado_em"])
    sincronizar_modelo_produto_de_overlay(pid, ov, payload={"nome": "Ivomec 50ml loja"})
    p.refresh_from_db()
    check("editar nome ainda grava", p.nome == "Ivomec 50ml loja")

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
