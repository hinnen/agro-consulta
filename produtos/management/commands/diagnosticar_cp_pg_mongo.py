"""Compara CP jul (ou período) — Mongo backup / Mongo lista dedup / Postgres dedup."""
from __future__ import annotations

from datetime import date
from decimal import Decimal

from django.core.management.base import BaseCommand

from produtos.lancamentos_backup_util import _linha_backup
from produtos.lancamentos_financeiro_pg_util import (
    _totais_de_titulos,
    contas_pagar_buscar_pagina_pg,
    dedup_titulos,
    titulos_financeiro_montar_qs,
)
from produtos.mongo_financeiro_util import (
    COL_DTO_LANCAMENTO,
    lancamento_para_api,
    lancamentos_montar_query_mongo,
    lancamentos_buscar_pagina,
)
from produtos.views import obter_conexao_mongo


def _dec(v) -> Decimal:
    try:
        return Decimal(str(v or 0)).quantize(Decimal("0.01"))
    except Exception:
        return Decimal("0.00")


def _fmt(d: Decimal) -> str:
    return f"{float(d):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


class Command(BaseCommand):
    help = (
        "Diagnóstico read-only: totais CP abertos no período — "
        "Excel backup (Mongo cru) vs Mongo lista dedup vs Postgres dedup."
    )

    def add_arguments(self, parser):
        parser.add_argument("--venc-de", default="2026-07-01")
        parser.add_argument("--venc-ate", default="2026-07-31")

    def handle(self, *args, **options):
        v_de = date.fromisoformat(str(options["venc_de"])[:10])
        v_ate = date.fromisoformat(str(options["venc_ate"])[:10])

        self.stdout.write(f"\n=== CP abertos · venc. {v_de} — {v_ate} ===\n")

        # Postgres (como a CP produção hoje)
        _, tot_pg, totais_pg = contas_pagar_buscar_pagina_pg(
            status="abertos",
            vencimento_de=v_de,
            vencimento_ate=v_ate,
            page=1,
            page_size=1,
            skip_totais=False,
            limite_max=25_000,
        )
        tp = totais_pg or {}
        self.stdout.write(
            f"Postgres (tela CP):  qtd={tot_pg}  "
            f"bruto={tp.get('bruto')}  pago={tp.get('movimentado')}  "
            f"a_pagar={tp.get('saldo_aberto')}"
        )

        qs_pg = titulos_financeiro_montar_qs(
            despesa=True,
            status="abertos",
            vencimento_de=v_de,
            vencimento_ate=v_ate,
        )
        dedup_pg = dedup_titulos(list(qs_pg))
        cong = [t for t in dedup_pg if t.mongo_congelado]
        pos = [t for t in dedup_pg if not t.mongo_congelado]
        tc = _totais_de_titulos(cong)
        tp2 = _totais_de_titulos(pos)
        self.stdout.write(
            f"  PG congelado 19/06: qtd={tc['quantidade']}  a_pagar={tc['saldo_aberto']:.2f}"
        )
        self.stdout.write(
            f"  PG pós-checkpoint:  qtd={tp2['quantidade']}  a_pagar={tp2['saldo_aberto']:.2f}"
        )

        _, db = obter_conexao_mongo()
        if db is None:
            self.stdout.write(self.style.WARNING("\nMongo indisponível — só Postgres acima.\n"))
            return

        # Mongo lista dedup (como CP lia antes do PG)
        q_cp = lancamentos_montar_query_mongo(
            despesa=True,
            status="abertos",
            vencimento_de=v_de,
            vencimento_ate=v_ate,
        )
        _, tot_mongo, totais_m = lancamentos_buscar_pagina(
            db,
            q_cp,
            True,
            page=1,
            page_size=1,
            skip_totais=False,
            limite_max=25_000,
        )
        tm = totais_m or {}
        self.stdout.write(
            f"\nMongo (lista dedup): qtd={tot_mongo}  "
            f"bruto={tm.get('bruto')}  pago={tm.get('movimentado')}  "
            f"a_pagar={tm.get('saldo_aberto')}"
        )

        # Mongo cru (como backup Excel — sem dedup)
        col = db[COL_DTO_LANCAMENTO]
        filtro = q_cp
        bruto = saldo = Decimal("0")
        n_raw = 0
        n_cong = 0
        saldo_cong = Decimal("0")
        saldo_pos = Decimal("0")
        for doc in col.find(filtro):
            row = _linha_backup(doc, True)
            api = lancamento_para_api(doc, True)
            rest = _dec(api.get("restante"))
            if rest <= Decimal("0.02"):
                continue
            n_raw += 1
            bruto += _dec(api.get("valor_bruto"))
            saldo += rest
            if row.get("fonte_agro") == "Sim":
                n_cong += 1
                saldo_cong += rest
            else:
                saldo_pos += rest

        self.stdout.write(
            f"\nMongo (backup/Excel): qtd={n_raw}  bruto={float(bruto):.2f}  "
            f"a_pagar={float(saldo):.2f}"
        )
        self.stdout.write(
            f"  carimbo 19/06:      qtd={n_cong}  a_pagar={float(saldo_cong):.2f}"
        )
        self.stdout.write(
            f"  sem carimbo:        qtd={n_raw - n_cong}  a_pagar={float(saldo_pos):.2f}"
        )

        delta = _dec(tp.get("saldo_aberto")) - saldo
        self.stdout.write(f"\nDelta PG - Mongo backup: {_fmt(delta)}")
        if abs(float(delta)) > 100:
            self.stdout.write(
                self.style.WARNING(
                    "  → PG e Mongo backup divergem. Se Mongo lista dedup ≈ PG, "
                    "import OK; Excel tinha linhas a mais (sem dedup) ou foto antiga."
                )
            )
        self.stdout.write("")
