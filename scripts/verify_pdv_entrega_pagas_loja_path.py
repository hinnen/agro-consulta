# -*- coding: utf-8 -*-
"""Prova path PDV-ENTREGA-PAGAS-24H.

  python scripts/verify_pdv_entrega_pagas_loja_path.py
"""
from __future__ import annotations

import os
import sys
from datetime import timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

import django

django.setup()

from django.utils import timezone

from produtos.entrega_pdv_pendente_util import (
    HORAS_PAGAS_LOJA_PDV,
    listar_entregas_bloqueando_fechamento_caixa,
    listar_entregas_pagas_loja_pdv,
    queryset_entregas_pagas_loja_pdv,
)
from produtos.models import PedidoEntrega

fails: list[str] = []
oks: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    if cond:
        oks.append(name)
        print("  OK  " + name + ((" — " + detail) if detail else ""))
    else:
        fails.append(name)
        print("  FAIL " + name + ((" — " + detail) if detail else ""))


def _read(rel: str) -> str:
    return (ROOT / rel).read_text(encoding="utf-8")


def test_arquivos() -> None:
    print("== Arquivos ==")
    js = _read("produtos/static/produtos/js/pdv_wizard.js")
    html = _read("produtos/templates/produtos/partials/pdv/step_produtos.html")
    util = _read("produtos/entrega_pdv_pendente_util.py")
    views = _read("produtos/views.py")
    mig = _read("produtos/migrations/0129_pedido_entrega_paga_na_loja.py")
    check("horas_24", "HORAS_PAGAS_LOJA_PDV = 24" in util)
    check("qs_pagas", "def queryset_entregas_pagas_loja_pdv" in util)
    check("api_itens_pagas", '"itens_pagas": itens_pagas' in views)
    check("registrar_flag", '"paga_na_loja": True' in views)
    check("tab_pagas", 'id="pdv-entregas-tab-pagas"' in html)
    check("js_aba", "entregasPendentesAba" in js)
    check("js_rota", "function abrirRotaEntregasPagas" in js)
    check("mig_0129", "paga_na_loja" in mig)
    check("model_field", "paga_na_loja = models.BooleanField" in _read("produtos/models.py"))


def test_db() -> None:
    print("== Dados ==")
    agora = timezone.now()
    nova = PedidoEntrega.objects.create(
        cliente_nome="Verify Paga Loja",
        status=PedidoEntrega.Status.PENDENTE,
        aguarda_pagamento_pdv=False,
        paga_na_loja=True,
        forma_pagamento="Pago na loja",
        loja_entrega="centro",
        itens_json=[{"nome": "X", "qtd": 1}],
        total_texto="R$ 1,00",
    )
    velha = PedidoEntrega.objects.create(
        cliente_nome="Verify Paga Velha",
        status=PedidoEntrega.Status.PENDENTE,
        aguarda_pagamento_pdv=False,
        paga_na_loja=True,
        forma_pagamento="Pago na loja",
        loja_entrega="centro",
        itens_json=[{"nome": "Y", "qtd": 1}],
        total_texto="R$ 2,00",
    )
    cobranca = PedidoEntrega.objects.create(
        cliente_nome="Verify A Pagar",
        status=PedidoEntrega.Status.PENDENTE,
        aguarda_pagamento_pdv=True,
        paga_na_loja=False,
        loja_entrega="centro",
        itens_json=[{"nome": "Z", "qtd": 1}],
        total_texto="R$ 3,00",
    )
    try:
        PedidoEntrega.objects.filter(pk=velha.pk).update(
            criado_em=agora - timedelta(hours=HORAS_PAGAS_LOJA_PDV + 2)
        )
        ids_pagas = {r["id"] for r in listar_entregas_pagas_loja_pdv(loja="centro")}
        ids_bloq = {r["id"] for r in listar_entregas_bloqueando_fechamento_caixa(loja="centro")}
        check("paga_aparece_24h", nova.pk in ids_pagas)
        check("velha_some", velha.pk not in ids_pagas)
        check("paga_nao_trava_caixa", nova.pk not in ids_bloq)
        check("cobranca_trava", cobranca.pk in ids_bloq)
        check("qs_paga_flag", queryset_entregas_pagas_loja_pdv().filter(pk=nova.pk).exists())
        row = next((r for r in listar_entregas_pagas_loja_pdv(loja="centro") if r["id"] == nova.pk), None)
        check("sem_retomar", bool(row) and row.get("pode_retomar") is False)
        check("sem_adiar", bool(row) and row.get("pode_adiar") is False)
        check("tem_maps", bool(row) and "maps_url" in row)
    finally:
        PedidoEntrega.objects.filter(pk__in=[nova.pk, velha.pk, cobranca.pk]).delete()


def main() -> None:
    print("=== PDV-ENTREGA-PAGAS-24H ===")
    test_arquivos()
    try:
        test_db()
    except Exception as ex:
        check("db_exc", False, str(ex)[:240])
    print("")
    print(f"VERIFY {'OK' if not fails else 'FAIL'} {len(oks)}/{len(oks) + len(fails)}")
    if fails:
        print("Falhas: " + ", ".join(fails))
        sys.exit(1)


if __name__ == "__main__":
    main()
