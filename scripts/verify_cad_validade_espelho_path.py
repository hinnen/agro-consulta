"""
Cadastro aba Validade e lote espelha resumo (extras) / NF.
Roda: python scripts/verify_cad_validade_espelho_path.py
"""
from __future__ import annotations

import os
import sys
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

import django

django.setup()

from produtos.models import (
    EstoqueLote,
    ProdutoGestaoOverlayAgro,
    garantir_estoque_lote_desde_extras,
    registrar_lote_validade_apos_entrada_nf,
)

PASS = 0
FAIL = 0
PREFIX = "VERIFYCADVAL"


def ok(msg: str) -> None:
    global PASS
    PASS += 1
    print(f"  OK  {msg}")


def bad(msg: str) -> None:
    global FAIL
    FAIL += 1
    print(f"  FAIL {msg}")


def check(cond: bool, msg: str) -> None:
    if cond:
        ok(msg)
    else:
        bad(msg)


def wipe() -> None:
    ProdutoGestaoOverlayAgro.objects.filter(
        produto_externo_id__startswith=PREFIX
    ).delete()


def test_heal_extras_para_lote() -> None:
    print("\n== extras -> EstoqueLote (heal) ==")
    wipe()
    pid = f"{PREFIX}HEAL01"
    dv = (date.today() + timedelta(days=40)).isoformat()
    ov = ProdutoGestaoOverlayAgro.objects.create(
        produto_externo_id=pid,
        nome="Heal",
        cadastro_extras={"validade": dv, "lote": "L-HEAL"},
    )
    check(EstoqueLote.objects.filter(overlay=ov).count() == 0, "sem lote antes")
    lotes = garantir_estoque_lote_desde_extras(ov, quantidade_atual=Decimal("3.00"))
    check(len(lotes) == 1, "criou 1 lote")
    el = lotes[0]
    check(el.lote_codigo == "L-HEAL", "codigo lote")
    check(el.data_validade.isoformat()[:10] == dv, "data validade")
    check(Decimal(el.quantidade_atual) == Decimal("3.00"), "qtd")
    lotes2 = garantir_estoque_lote_desde_extras(ov)
    check(len(lotes2) == 1 and lotes2[0].pk == el.pk, "idempotente")


def test_nf_atualiza_resumo() -> None:
    print("\n== NF lote atualiza extras (resumo) ==")
    wipe()
    pid = f"{PREFIX}NF01"
    dv = (date.today() + timedelta(days=20)).isoformat()
    info = registrar_lote_validade_apos_entrada_nf(
        pid,
        {"lote_validade": dv, "lote_numero": "NF-1"},
        Decimal("2"),
        nome_produto="NF",
        deposito="centro",
    )
    check(bool(info and info.get("lote_id")), "criou lote NF")
    ov = ProdutoGestaoOverlayAgro.objects.get(produto_externo_id=pid)
    ex = ov.cadastro_extras if isinstance(ov.cadastro_extras, dict) else {}
    check(ex.get("validade") == dv, "extras.validade espelhada")
    check(str(ex.get("lote") or "") == "NF-1", "extras.lote espelhado")


def test_views_import() -> None:
    print("\n== views usa garantir ==")
    src = (ROOT / "produtos/views.py").read_text(encoding="utf-8")
    check("garantir_estoque_lote_desde_extras" in src, "import/uso no views")
    check("teve_lote_ativo" in src, "relatorio nao esconde extras com lote qtd0")


def main() -> int:
    test_heal_extras_para_lote()
    test_nf_atualiza_resumo()
    test_views_import()
    wipe()
    print(f"\n== resultado {PASS}/{PASS + FAIL} ==")
    return 0 if FAIL == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
