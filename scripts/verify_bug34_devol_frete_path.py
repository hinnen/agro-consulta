# -*- coding: utf-8 -*-
"""Prova path BUG-34 — devolução de entrega inclui frete ao zerar itens.

Uso: python scripts/verify_bug34_devol_frete_path.py
"""
from __future__ import annotations

import os
import sys
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

import django

django.setup()

from produtos.devolucao_venda_util import (  # noqa: E402
    frete_restante,
    montar_selecao_devolucao,
)
from produtos.models import ItemVendaAgro, VendaAgro  # noqa: E402

FAILS: list[str] = []
OKS: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    if cond:
        OKS.append(name)
        print(f"  OK  {name}" + (f" — {detail}" if detail else ""))
    else:
        FAILS.append(name)
        print(f" FAIL {name}" + (f" — {detail}" if detail else ""))


class _FakeItem:
    def __init__(self, pk: int, qtd: Decimal, devolvida: Decimal, valor: Decimal, desc: str):
        self.pk = pk
        self.quantidade = qtd
        self.quantidade_devolvida = devolvida
        self.valor_total = valor
        self.valor_unitario = (valor / qtd) if qtd else Decimal("0")
        self.descricao = desc

    @property
    def quantidade_restante(self):
        return max(Decimal("0"), Decimal(str(self.quantidade)) - Decimal(str(self.quantidade_devolvida)))


class _FakeQS:
    def __init__(self, items):
        self._items = list(items)

    def all(self):
        return self._items


class _FakeVenda:
    def __init__(self, frete: Decimal, frete_dev: Decimal, items):
        self.frete = frete
        self.frete_devolvido = frete_dev
        self.itens = _FakeQS(items)


def main() -> int:
    print("=== PATH BUG-34 DEVOL FRETE ===\n")

    html = (ROOT / "produtos/templates/produtos/venda_agro_detalhe.html").read_text(
        encoding="utf-8"
    )
    util = (ROOT / "produtos/devolucao_venda_util.py").read_text(encoding="utf-8")

    print("[1] Marcadores UI / util")
    check("ui_frete_checked_html", 'id="devolucao-frete-chk"' in html and "checked" in html)
    check("ui_frete_destaque", "border-rose-400" in html and "Taxa de entrega" in html)
    check("ui_garantir_frete", "garantirFreteSeEsgotaItens" in html)
    check("ui_selecao_esgota", "selecaoEsgotaItens" in html)
    check("ui_default_frete_true", "freteChk.checked = freteRestante > 0.009" in html)
    check("util_esgota_fn", "_selecao_esgota_itens_restantes" in util)
    check("util_incluir_auto", "esgota_itens" in util and "incluir_frete" in util)

    print("\n[2] montar_selecao — milho + frete (caso Gabriel)")
    milho = _FakeItem(1, Decimal("1"), Decimal("0"), Decimal("80.00"), "MILHO")
    v = _FakeVenda(Decimal("15.00"), Decimal("0"), [milho])

    # Checkbox frete desmarcado, mas item inteiro → frete entra junto
    linhas, frete_v, err = montar_selecao_devolucao(
        v,
        itens_raw=[{"item_id": 1, "quantidade": 1}],
        devolver_frete=False,
        devolver_tudo=False,
    )
    check("sem_erro_milho", err is None, str(err))
    check("frete_auto_com_item_total", frete_v == Decimal("15.00"), str(frete_v))
    check("linha_milho", linhas is not None and len(linhas) == 1)

    print("\n[3] parcial de item — frete NÃO automático")
    milho2 = _FakeItem(2, Decimal("2"), Decimal("0"), Decimal("100.00"), "MILHO 2")
    v2 = _FakeVenda(Decimal("15.00"), Decimal("0"), [milho2])
    linhas2, frete2, err2 = montar_selecao_devolucao(
        v2,
        itens_raw=[{"item_id": 2, "quantidade": 1}],
        devolver_frete=False,
        devolver_tudo=False,
    )
    check("parcial_sem_erro", err2 is None)
    check("parcial_sem_frete", frete2 == Decimal("0.00"), str(frete2))
    check("parcial_com_frete_marcado", True)
    _, frete2b, _ = montar_selecao_devolucao(
        v2,
        itens_raw=[{"item_id": 2, "quantidade": 1}],
        devolver_frete=True,
        devolver_tudo=False,
    )
    check("parcial_frete_se_marcado", frete2b == Decimal("15.00"), str(frete2b))

    print("\n[4] só frete restante (itens já devolvidos)")
    milho3 = _FakeItem(3, Decimal("1"), Decimal("1"), Decimal("80.00"), "MILHO")
    v3 = _FakeVenda(Decimal("15.00"), Decimal("0"), [milho3])
    check("frete_restante_15", frete_restante(v3) == Decimal("15.00"))
    linhas3, frete3, err3 = montar_selecao_devolucao(
        v3,
        itens_raw=[],
        devolver_frete=False,  # auto: esgota (nada restante)
        devolver_tudo=False,
    )
    check("so_frete_auto", err3 is None and frete3 == Decimal("15.00"), f"{err3}/{frete3}")
    check("so_frete_sem_linhas", linhas3 is not None and len(linhas3) == 0)

    print("\n[5] devolver_tudo clássico")
    milho4 = _FakeItem(4, Decimal("1"), Decimal("0"), Decimal("50.00"), "X")
    v4 = _FakeVenda(Decimal("10.00"), Decimal("0"), [milho4])
    linhas4, frete4, err4 = montar_selecao_devolucao(
        v4, itens_raw=None, devolver_frete=False, devolver_tudo=True
    )
    check("tudo_com_frete", err4 is None and frete4 == Decimal("10.00") and len(linhas4) == 1)

    # Tipos reais no import (smoke)
    check("models_ok", issubclass(VendaAgro, object) and issubclass(ItemVendaAgro, object))

    print("\n---")
    print(f"OK={len(OKS)} FAIL={len(FAILS)}")
    for f in FAILS:
        print(" ", f)
    if FAILS:
        print("VERIFY_FAIL")
        return 1
    print("VERIFY_OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
