# -*- coding: utf-8 -*-
"""Prova path CAIXA-DEVOL-DINHEIRO-MP — venda Point + devolução em dinheiro no fechamento."""
from __future__ import annotations

import os
import sys
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

ROOT = Path(__file__).resolve().parent.parent
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
sys.path.insert(0, str(ROOT))

import django

django.setup()

from produtos.caixa_util import _agregar_resumo_turno_sessao  # noqa: E402

FAILS: list[str] = []
OKS = 0


def ok(msg: str) -> None:
    global OKS
    OKS += 1
    print("OK", msg)


def fail(msg: str) -> None:
    FAILS.append(msg)
    print("FAIL", msg)


def check(cond: bool, msg: str) -> None:
    if cond:
        ok(msg)
    else:
        fail(msg)


class _Rel:
    def __init__(self, items):
        self._items = items

    def all(self):
        return self._items


def _venda(*, pk: int, forma: str, valor: Decimal, maquina: str | None, devolvida: bool):
    row: dict = {"forma": forma, "valor": float(valor)}
    if maquina:
        row["maquinaId"] = maquina
        row["cobrarNoPointMp"] = True
        row["mpBalcaoModo"] = "point"
    return SimpleNamespace(
        pk=pk,
        pagamentos_json=[row],
        forma_pagamento=forma,
        total=valor,
        devolvida_em=object() if devolvida else None,
    )


def _mov(*, pk: int, tipo: str, forma: str, valor: Decimal, obs: str):
    return SimpleNamespace(
        pk=pk,
        tipo=tipo,
        forma_pagamento=forma,
        valor=valor,
        observacao=obs,
    )


def _sessao(vendas, movimentos, *, abertura: str = "0"):
    return SimpleNamespace(
        pk=50,
        valor_abertura=Decimal(abertura),
        vendas=_Rel(vendas),
        movimentos=_Rel(movimentos),
    )


def _agregar(sessao):
    with patch("produtos.models.PdvMercadoPagoPointOrder.objects") as objs:
        objs.filter.return_value.values_list.return_value = []
        return _agregar_resumo_turno_sessao(sessao)


def main() -> int:
    util = (ROOT / "produtos/caixa_util.py").read_text(encoding="utf-8")
    html = (ROOT / "produtos/templates/produtos/caixa_fechar.html").read_text(encoding="utf-8")
    check("_movimentos_retirada_devolucao_duplicados_turno" not in util, "helper FL-017 duplicado removido")
    check("vendas_list = list(vendas_rel.all())" in util, "agrega vendas devolvidas do turno")
    check("Devolução em <strong>dinheiro</strong>" in html, "ajuda fechar caixa menciona devolução em dinheiro")

    mp = "Cartão de débito — Mercado Pago"
    v_dev = _venda(pk=1, forma="Cartão de débito", valor=Decimal("49.00"), maquina="mp_balcao", devolvida=True)
    v_ok = _venda(pk=2, forma="Cartão de débito", valor=Decimal("5.90"), maquina="mp_balcao", devolvida=False)
    ret_din = _mov(pk=10, tipo="retirada", forma="Dinheiro", valor=Decimal("49.00"), obs="Devolução venda #1")
    sess = _sessao([v_dev, v_ok], [ret_din], abertura="100.00")
    esperado, vendas, _ref, retirada = _agregar(sess)
    check(vendas.get(mp) == Decimal("54.90"), f"vendas MP = 54.90 (inclui devolvida) got {vendas.get(mp)}")
    check(esperado.get(mp) == Decimal("54.90"), f"esperado MP = 54.90 (pinpad) got {esperado.get(mp)}")
    check(retirada.get("Dinheiro") == Decimal("49.00"), f"retirada dinheiro 49 got {retirada.get('Dinheiro')}")
    check(esperado.get("Dinheiro") == Decimal("51.00"), f"esperado dinheiro 100-49=51 got {esperado.get('Dinheiro')}")

    v_cash = _venda(pk=3, forma="Dinheiro", valor=Decimal("49.00"), maquina=None, devolvida=True)
    ret_cash = _mov(pk=11, tipo="retirada", forma="Dinheiro", valor=Decimal("49.00"), obs="Devolução venda #3")
    sess_fl = _sessao([v_cash], [ret_cash], abertura="20.00")
    esp_fl, vend_fl, _, ret_fl = _agregar(sess_fl)
    check(vend_fl.get("Dinheiro") == Decimal("49.00"), "FL-017 vendas dinheiro incluem devolvida")
    check(ret_fl.get("Dinheiro") == Decimal("49.00"), "FL-017 retirada dinheiro aplicada")
    check(esp_fl.get("Dinheiro") == Decimal("20.00"), f"FL-017 esperado = abertura got {esp_fl.get('Dinheiro')}")

    print(f"---\noks={OKS} fails={len(FAILS)}")
    if FAILS:
        for f in FAILS:
            print(" ", f)
        return 1
    print("VERIFY_CAIXA_DEVOL_DINHEIRO_MP_OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
