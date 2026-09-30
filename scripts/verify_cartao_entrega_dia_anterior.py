# -*- coding: utf-8 -*-
"""Prova — cartão de entrega do dia anterior não entra no esperado de hoje.

  python scripts/verify_cartao_entrega_dia_anterior.py
"""
from __future__ import annotations

import os
import sys
from datetime import timedelta
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
os.environ.setdefault("SECRET_KEY", "verify-cartao-entrega-dia-anterior")

import django

django.setup()

from django.utils import timezone

from produtos.caixa_util import (
    _agregar_resumo_turno_sessao,
    linha_eh_cartao_maquina,
    resumo_cartao_entrega_dia_anterior,
)
from produtos.entrega_pdv_pendente_util import cartao_maquina_dia_anterior_aceito
import produtos.entrega_pdv_pendente_util as eu
import produtos.models as pm

fails: list[str] = []
oks: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    if cond:
        oks.append(name)
        print("  OK  " + name + ((" — " + detail) if detail else ""))
    else:
        fails.append(name)
        print("  FAIL " + name + ((" — " + detail) if detail else ""))


class _QS:
    def values_list(self, *a, **k):
        return []


class _Mgr:
    class Status:
        FINALIZED = "finalized"

    class objects:
        @staticmethod
        def filter(**k):
            return _QS()


class _Rel:
    def __init__(self, items):
        self._items = items

    def all(self):
        return list(self._items)


class _Venda:
    def __init__(self, pk, pagamentos, flag=False):
        self.pk = pk
        self.pagamentos_json = pagamentos
        self.cartao_maquina_dia_anterior = flag
        self.devolvida_em = None
        self.forma_pagamento = ""
        self.total = 0


class _Sessao:
    def __init__(self, vendas):
        self.valor_abertura = Decimal("100.00")
        self.vendas = _Rel(vendas)
        self.movimentos = _Rel([])


def main() -> int:
    check("cartão débito", linha_eh_cartao_maquina("Cartão de débito"))
    check("cartão MP", linha_eh_cartao_maquina("Cartão de crédito — Mercado Pago"))
    check("pix fora", not linha_eh_cartao_maquina("PIX"))
    check("dinheiro fora", not linha_eh_cartao_maquina("Dinheiro"))

    real_mp = pm.PdvMercadoPagoPointOrder
    pm.PdvMercadoPagoPointOrder = _Mgr
    try:
        venda = _Venda(
            1,
            [
                {"forma": "Cartão de débito", "valor": 40},
                {"forma": "Dinheiro", "valor": 10},
            ],
            flag=True,
        )
        esp, vendas, _, _ = _agregar_resumo_turno_sessao(_Sessao([venda]))
        check("cartão ontem fora do esperado", esp.get("Cartão de débito", Decimal("0")) == 0)
        check(
            "dinheiro de hoje continua",
            esp.get("Dinheiro") == Decimal("110.00"),
            str(esp.get("Dinheiro")),
        )
        check("vendas cartão zeradas", vendas.get("Cartão de débito", Decimal("0")) == 0)

        normal = _Venda(2, [{"forma": "Cartão de crédito", "valor": 25}], flag=False)
        esp2, _, _, _ = _agregar_resumo_turno_sessao(_Sessao([normal]))
        check("cartão de hoje entra", esp2.get("Cartão de crédito") == Decimal("25.00"))

        av = resumo_cartao_entrega_dia_anterior([_Sessao([venda])])
        check("aviso tem", av.get("tem") is True, str(av.get("valor")))
        check("aviso 40", av.get("valor") == "40.00")
        check("aviso some sem marca", resumo_cartao_entrega_dia_anterior([_Sessao([normal])]).get("tem") is False)
    finally:
        pm.PdvMercadoPagoPointOrder = real_mp

    class _Ent:
        criado_em = timezone.now() - timedelta(days=1, hours=3)

    class _Filtro:
        def exclude(self, **k):
            return self

        def first(self):
            return _Ent()

    real_filter = eu.PedidoEntrega.objects.filter
    eu.PedidoEntrega.objects.filter = lambda *a, **k: _Filtro()
    try:
        pag = [{"forma": "Cartão de débito", "valor": 15}]
        check(
            "aceita ontem + cartão",
            cartao_maquina_dia_anterior_aceito(
                {"cartao_maquina_dia_anterior": True, "pedido_entrega_pendente_id": 9},
                pag,
            ),
        )
        check(
            "recusa sem marca",
            not cartao_maquina_dia_anterior_aceito(
                {"pedido_entrega_pendente_id": 9},
                pag,
            ),
        )
        check(
            "recusa pix",
            not cartao_maquina_dia_anterior_aceito(
                {"cartao_maquina_dia_anterior": True, "pedido_entrega_pendente_id": 9},
                [{"forma": "PIX", "valor": 15}],
            ),
        )
        _Ent.criado_em = timezone.now()
        check(
            "recusa mesmo dia",
            not cartao_maquina_dia_anterior_aceito(
                {"cartao_maquina_dia_anterior": True, "pedido_entrega_pendente_id": 9},
                pag,
            ),
        )
    finally:
        eu.PedidoEntrega.objects.filter = real_filter

    wizard = (ROOT / "produtos/static/produtos/js/pdv_wizard.js").read_text(encoding="utf-8")
    caixa = (ROOT / "produtos/templates/produtos/caixa_fechar.html").read_text(encoding="utf-8")
    check("pdv pergunta o dia", "O cartão passou em qual dia?" in (ROOT / "produtos/templates/produtos/partials/pdv/step_pagamento.html").read_text(encoding="utf-8"))
    check("payload ontem", "cartao_maquina_dia_anterior" in wizard)
    check("fechar caixa linha", "cf-aviso-cartao-entrega-ontem" in caixa)

    print(f"\n{len(oks)} ok, {len(fails)} falha(s)")
    return 1 if fails else 0


if __name__ == "__main__":
    raise SystemExit(main())
