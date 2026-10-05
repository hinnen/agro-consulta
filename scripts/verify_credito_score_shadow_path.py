#!/usr/bin/env python
"""Prova de caminho — crédito shadow (rollback; não grava na loja).

Uso (com DATABASE_URL Postgres local / .env):

  python scripts/verify_credito_score_shadow_path.py

Sem --persist no motor: só analisa. Snapshots de prova são criados e removidos
na mesma transaction (rollback).
"""

from __future__ import annotations

import os
import sys
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

import django

django.setup()

from django.db import transaction
from django.utils import timezone

from produtos.credito_score_shadow import analisar_cliente, persistir_analise
from produtos.models import (
    ClienteAgro,
    ClienteAnaliseCreditoAgro,
    FiadoBaixaAgro,
    FiadoTituloAgro,
)

OK = 0
FAIL = 0


def check(name: str, cond: bool, detail: str = ""):
    global OK, FAIL
    if cond:
        OK += 1
        print(f"  OK  {name}")
    else:
        FAIL += 1
        print(f" FAIL {name} {detail}")


def main():
    from django.db import connection

    hoje = date(2026, 10, 5)
    print("=== verify credito_score_shadow (atomic rollback) ===")
    tables = set(connection.introspection.table_names())
    if "produtos_clienteanalisecreditoagro" not in tables:
        print(
            "SKIP: tabela produtos_clienteanalisecreditoagro ausente "
            f"(vendor={connection.vendor}). Rode migrate 0137 no Postgres local."
        )
        # Prova só de leitura / score sem persistir (não precisa da tabela nova)
        with transaction.atomic():
            sid = transaction.savepoint()
            try:
                cli = ClienteAgro.objects.create(
                    nome="ZZ Shadow Prova Temp",
                    limite_fiado_local=Decimal("0.01"),
                )
                r0 = analisar_cliente(cli, hoje=hoje)
                check("sem_hist_score_null", r0.score is None)
                check("limite_001_intact", cli.limite_fiado_local == Decimal("0.01"))
            finally:
                transaction.savepoint_rollback(sid)
        print(f"=== {OK} OK · {FAIL} FAIL (persist SKIP) ===")
        return 0 if FAIL == 0 else 1

    with transaction.atomic():
        sid = transaction.savepoint()
        try:
            cli = ClienteAgro.objects.create(
                nome="ZZ Shadow Prova Temp",
                limite_fiado_local=Decimal("0.01"),
            )
            r0 = analisar_cliente(cli, hoje=hoje)
            check("sem_hist_score_null", r0.score is None)
            check("limite_001_intact", cli.limite_fiado_local == Decimal("0.01"))

            cli2 = ClienteAgro.objects.create(
                nome="ZZ Shadow Bom Pagador",
                limite_fiado_local=Decimal("500"),
            )
            for i in range(6):
                venc = hoje - timedelta(days=30 * (i + 1))
                t = FiadoTituloAgro.objects.create(
                    chave_unica=f"shadow-prova-{cli2.pk}-{i}",
                    cliente_agro=cli2,
                    cliente_nome=cli2.nome,
                    numero_documento=f"SP{i}",
                    parcela_num=1,
                    parcela_total=1,
                    vencimento=venc,
                    valor_bruto=Decimal("100.00"),
                    valor_pago=Decimal("100.00"),
                    situacao=FiadoTituloAgro.Situacao.QUITADO,
                    origem=FiadoTituloAgro.Origem.PDV,
                )
                b = FiadoBaixaAgro.objects.create(
                    titulo=t,
                    valor=Decimal("100.00"),
                    forma_pagamento="Dinheiro",
                    usuario="prova",
                )
                FiadoBaixaAgro.objects.filter(pk=b.pk).update(
                    criado_em=timezone.make_aware(
                        datetime(venc.year, venc.month, venc.day, 12, 0, 0)
                    )
                )

            lim_antes = cli2.limite_fiado_local
            n_tit = FiadoTituloAgro.objects.filter(cliente_agro=cli2).count()
            n_bx = FiadoBaixaAgro.objects.filter(titulo__cliente_agro=cli2).count()
            r = analisar_cliente(cli2, hoje=hoje, media_3m=Decimal("200"))
            snap = persistir_analise(r)
            cli2.refresh_from_db()
            check("score_numerico", r.score is not None and r.score >= 70, str(r.score))
            check("limite_nao_mudou", cli2.limite_fiado_local == lim_antes)
            check("titulos_iguais", FiadoTituloAgro.objects.filter(cliente_agro=cli2).count() == n_tit)
            check("baixas_iguais", FiadoBaixaAgro.objects.filter(titulo__cliente_agro=cli2).count() == n_bx)
            check("snapshot_criado", snap.pk is not None)
            check("so_tabela_shadow", ClienteAnaliseCreditoAgro.objects.filter(pk=snap.pk).exists())

            # segundo snapshot
            persistir_analise(analisar_cliente(cli2, hoje=hoje, media_3m=Decimal("200")))
            check(
                "historico_duas_linhas",
                ClienteAnaliseCreditoAgro.objects.filter(cliente=cli2).count() == 2,
            )
        finally:
            transaction.savepoint_rollback(sid)

    print(f"=== {OK} OK · {FAIL} FAIL ===")
    return 0 if FAIL == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
