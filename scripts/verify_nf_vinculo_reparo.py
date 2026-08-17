#!/usr/bin/env python
"""Prova NF-VINCULO-REPARO — path detalhado: histórico, overlay, corte EAN, não desfaz edição."""
from __future__ import annotations

import ast
import os
import sys
from decimal import Decimal
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


def _limpar(*pids: str) -> None:
    from produtos.models import Produto, ProdutoCadastroAlteracaoAgro, ProdutoGestaoOverlayAgro

    for pid in pids:
        ProdutoCadastroAlteracaoAgro.objects.filter(produto_externo_id=pid).delete()
        ProdutoGestaoOverlayAgro.objects.filter(produto_externo_id=pid).delete()
        Produto.objects.filter(produto_externo_id=pid).delete()


def _criar(
    pid: str,
    *,
    nome: str,
    ov_nome: str = "",
    marca: str = "",
    hist_nome: str = "",
    hist_marca: str = "",
    depois_nome: str = "",
) -> None:
    from produtos.models import Produto, ProdutoCadastroAlteracaoAgro, ProdutoGestaoOverlayAgro

    Produto.objects.create(
        produto_externo_id=pid,
        codigo_interno=pid[-4:],
        codigo_nfe=f"GM{pid[-4:]}",
        nome=nome,
        marca=marca,
        categoria="Medicamentos" if marca else "",
        unidade="UN",
        custo=Decimal("10.00"),
        preco_venda=Decimal("20.00"),
    )
    ProdutoGestaoOverlayAgro.objects.create(produto_externo_id=pid, nome=ov_nome)
    if hist_nome:
        ProdutoCadastroAlteracaoAgro.objects.create(
            produto_externo_id=pid,
            campo="nome",
            campo_label="Nome",
            valor_antes=hist_nome,
            valor_depois="—",
            origem="gestao",
        )
    if hist_marca:
        ProdutoCadastroAlteracaoAgro.objects.create(
            produto_externo_id=pid,
            campo="marca",
            campo_label="Marca",
            valor_antes=hist_marca,
            valor_depois="—",
            origem="gestao",
        )
    if depois_nome:
        ProdutoCadastroAlteracaoAgro.objects.create(
            produto_externo_id=pid,
            campo="nome",
            campo_label="Nome",
            valor_antes="—",
            valor_depois=depois_nome,
            origem="modal",
        )


def main() -> int:
    print("== AST / travas ==")
    for rel in (
        "produtos/reparar_vinculo_nf_cadastro_util.py",
        "produtos/management/commands/reparar_cadastro_vinculo_nf.py",
        "produtos/catalogo_agro.py",
    ):
        try:
            ast.parse(_read(rel))
            check(f"AST {rel}", True)
        except SyntaxError as e:
            check(f"AST {rel}", False, str(e))

    util = _read("produtos/reparar_vinculo_nf_cadastro_util.py")
    cmd = _read("produtos/management/commands/reparar_cadastro_vinculo_nf.py")
    cat = _read("produtos/catalogo_agro.py")
    html = _read("produtos/templates/produtos/entrada_nota.html")
    models = _read("produtos/models.py")
    views = _read("produtos/views.py")
    check("comando exige --aplicar", "--aplicar" in cmd)
    check("não lista preço no patch", '"preco_venda"' not in util and '"preco_custo"' not in util)
    bloco = util.split("_CAMPOS_TXT", 1)[-1].split(")", 1)[0]
    check("não devolve GM/barras/unidade", "codigo_nfe" not in bloco and "unidade" not in bloco)
    check("corte EAN sem inventar", "def nome_sem_ean_colchete" in util)
    check("vínculo não sync cProd", "def payload_overlay_deve_sincronizar_produto" in cat)
    check("cProd origem NF", "origem_entrada_nf: true" in html)
    lote_fn = models.split("def registrar_lote_validade_apos_entrada_nf", 1)[-1].split(
        "def reduzir_lote", 1
    )[0]
    check("lote não copia xProd", "ov.nome =" not in lote_fn)
    check("estoque cadastro antes do xProd", 'nome_p = str((doc or {}).get("Nome") or "")[:200]' in views)

    print("\n== Heurística ==")
    import django

    django.setup()
    from produtos.models import Produto, ProdutoGestaoOverlayAgro
    from produtos.reparar_vinculo_nf_cadastro_util import (
        _hist_bom,
        aplicar_reparo_vinculo_nf,
        nome_sem_ean_colchete,
        parece_nome_nf,
        planejar_reparo_vinculo_nf,
    )

    check("Ivomec NF", parece_nome_nf("IVOMEC - 50 ML [7898053773339]"))
    check("nome loja ok", not parece_nome_nf("ivomec ivermectina injetavel 50ml"))
    check("hist lixo", not _hist_bom("[NOME QUEBRADO]"))
    check("hist NF não serve", not _hist_bom("IVOMEC - 50 ML [7898053773339]"))
    check("hist bom", _hist_bom("ivomec ivermectina injetavel 50ml boehringer"))
    check(
        "corte EAN",
        nome_sem_ean_colchete("ferron b12 - 100 ml [7898185261353]") == "ferron b12 - 100 ml",
    )

    pids = [
        "vrp-hist",
        "vrp-overlay",
        "vrp-corte",
        "vrp-depois",
        "vrp-limpo",
        "vrp-quebrado",
        "vrp-dry",
    ]
    _limpar(*pids)

    print("\n== Caso A — PG+overlay NF, histórico bom ==")
    _criar(
        "vrp-hist",
        nome="IVOMEC - 50 ML [7898053773339]",
        ov_nome="IVOMEC - 50 ML [7898053773339]",
        hist_nome="ivomec ivermectina injetavel 50ml boehringer ingelheim",
        hist_marca="BOEHRINGER INGELHEIM",
    )
    pl = planejar_reparo_vinculo_nf(pid="vrp-hist")
    check("A planeja", len(pl) == 1)
    n = aplicar_reparo_vinculo_nf(pl)
    p = Produto.objects.get(produto_externo_id="vrp-hist")
    ov = ProdutoGestaoOverlayAgro.objects.get(produto_externo_id="vrp-hist")
    check("A aplicou", n == 1)
    check("A nome histórico", p.nome.startswith("ivomec ivermectina"))
    check("A overlay limpo", (ov.nome or "").strip() == "")
    check("A marca", p.marca == "BOEHRINGER INGELHEIM")
    check("A preço intacto", p.preco_venda == Decimal("20.00"))
    check("A GM intacto", p.codigo_nfe.startswith("GM"))

    print("\n== Caso B — PG bom, só overlay NF ==")
    _criar(
        "vrp-overlay",
        nome="equipalazone injetavel 100ml",
        ov_nome="EQUIPALAZONE INJ. - 100 ML [7898213520261]",
        marca="CHELEVET",
    )
    nome_antes = Produto.objects.get(produto_externo_id="vrp-overlay").nome
    aplicar_reparo_vinculo_nf(planejar_reparo_vinculo_nf(pid="vrp-overlay"))
    p = Produto.objects.get(produto_externo_id="vrp-overlay")
    ov = ProdutoGestaoOverlayAgro.objects.get(produto_externo_id="vrp-overlay")
    check("B PG não muda", p.nome == nome_antes)
    check("B overlay limpo", (ov.nome or "").strip() == "")
    check("B marca fica", p.marca == "CHELEVET")

    print("\n== Caso C — PG NF sem histórico: só corta o [EAN] ==")
    _criar("vrp-corte", nome="ferron b12 - 100 ml [7898185261353]", ov_nome="")
    aplicar_reparo_vinculo_nf(planejar_reparo_vinculo_nf(pid="vrp-corte"))
    p = Produto.objects.get(produto_externo_id="vrp-corte")
    check("C corta EAN", p.nome == "ferron b12 - 100 ml")

    print("\n== Caso D — edição boa depois do wipe: não desfaz ==")
    _criar(
        "vrp-depois",
        nome="IVOMEC - 50 ML [7898053773339]",
        ov_nome="IVOMEC - 50 ML [7898053773339]",
        hist_nome="ivomec ivermectina injetavel 50ml boehringer ingelheim",
        depois_nome="ivomec 50ml loja novo",
    )
    aplicar_reparo_vinculo_nf(planejar_reparo_vinculo_nf(pid="vrp-depois"))
    p = Produto.objects.get(produto_externo_id="vrp-depois")
    ov = ProdutoGestaoOverlayAgro.objects.get(produto_externo_id="vrp-depois")
    check("D usa edição posterior", p.nome == "ivomec 50ml loja novo")
    check("D overlay some", (ov.nome or "").strip() == "")

    print("\n== Caso E — produto limpo não entra ==")
    _criar("vrp-limpo", nome="racao cao adulto 15kg", ov_nome="", marca="PREMIER")
    pl = planejar_reparo_vinculo_nf(pid="vrp-limpo")
    check("E fora da lista", pl == [])
    p = Produto.objects.get(produto_externo_id="vrp-limpo")
    check("E nome igual", p.nome == "racao cao adulto 15kg")

    print("\n== Caso F — hist [NOME QUEBRADO] não volta lixo ==")
    _criar(
        "vrp-quebrado",
        nome="CALMINEX - 30 GR [7896185970930]",
        ov_nome="CALMINEX - 30 GR [7896185970930]",
        hist_nome="[NOME QUEBRADO]",
    )
    aplicar_reparo_vinculo_nf(planejar_reparo_vinculo_nf(pid="vrp-quebrado"))
    p = Produto.objects.get(produto_externo_id="vrp-quebrado")
    check("F não usa NOME QUEBRADO", "[NOME QUEBRADO]" not in p.nome)
    check("F corta EAN", p.nome == "CALMINEX - 30 GR")

    print("\n== Dry-run não grava ==")
    _criar(
        "vrp-dry",
        nome="BUTOX CE 25 - 20 ML [7896185902405]",
        ov_nome="BUTOX CE 25 - 20 ML [7896185902405]",
        hist_nome="veneno butox p ce 25 msd 20ml",
    )
    planejar_reparo_vinculo_nf(pid="vrp-dry")
    p = Produto.objects.get(produto_externo_id="vrp-dry")
    check("dry não grava", "[" in p.nome)

    _limpar(*pids)

    print(f"\n{'VERIFY_OK' if not FAIL else 'VERIFY_FAIL'}  {OK} ok · {len(FAIL)} fail")
    for f in FAIL:
        print(f"  - {f}")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
