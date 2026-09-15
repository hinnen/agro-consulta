"""Auditoria read-only: paridade Mongo espelho vs Postgres Agro (corte ERP seguro)."""
from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal

from django.core.management.base import BaseCommand

from produtos.agro_fonte_config import agro_financeiro_usa_postgres
from produtos.lancamentos_financeiro_pg_util import (
    contas_pagar_buscar_pagina_pg,
    dedup_titulos,
    titulos_financeiro_montar_qs,
)
from produtos.models import EntradaNotaRascunhoAgro, TituloFinanceiroAgro
from produtos.mongo_financeiro_util import (
    COL_DTO_LANCAMENTO,
    lancamentos_buscar_pagina,
    lancamentos_montar_query_mongo,
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
        "Compara totais CP/CR, contagem de títulos e rascunhos NF — Mongo vs Postgres. "
        "Somente leitura; use antes/depois do corte total."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--venc-de",
            default="",
            help="Vencimento de (YYYY-MM-DD). Vazio = mês corrente.",
        )
        parser.add_argument(
            "--venc-ate",
            default="",
            help="Vencimento até (YYYY-MM-DD). Vazio = fim do mês corrente.",
        )

    def handle(self, *args, **options):
        hoje = date.today()
        v_de = date.fromisoformat(str(options["venc_de"])[:10]) if options.get("venc_de") else hoje.replace(day=1)
        if options.get("venc_ate"):
            v_ate = date.fromisoformat(str(options["venc_ate"])[:10])
        elif hoje.month == 12:
            v_ate = date(hoje.year, 12, 31)
        else:
            v_ate = date(hoje.year, hoje.month + 1, 1) - timedelta(days=1)

        self.stdout.write("\n=== AUDITORIA CORTE MONGO → POSTGRES (read-only) ===\n")
        self.stdout.write(f"Financeiro PG ativo: {agro_financeiro_usa_postgres()}\n")

        n_pg = TituloFinanceiroAgro.objects.count()
        n_cp_pg = TituloFinanceiroAgro.objects.filter(despesa=True).count()
        n_cr_pg = TituloFinanceiroAgro.objects.filter(despesa=False).count()
        n_nf_pg = EntradaNotaRascunhoAgro.objects.count()
        self.stdout.write(f"Postgres títulos: total={n_pg}  CP={n_cp_pg}  CR={n_cr_pg}")
        self.stdout.write(f"Postgres rascunhos Entrada NF: {n_nf_pg}\n")

        self.stdout.write(f"\n--- CP abertos · venc. {v_de} — {v_ate} ---")
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
            f"PG (tela CP): qtd={tot_pg}  a_pagar={tp.get('saldo_aberto')}"
        )

        _, db = obter_conexao_mongo()
        if db is None:
            self.stdout.write(self.style.WARNING("\nMongo indisponível — só Postgres acima.\n"))
            return

        col = db[COL_DTO_LANCAMENTO]
        n_mongo = col.estimated_document_count()
        self.stdout.write(f"\nMongo DtoLancamento (estimado): {n_mongo}")

        q_cp = lancamentos_montar_query_mongo(
            despesa=True,
            status="abertos",
            vencimento_de=v_de,
            vencimento_ate=v_ate,
        )
        _, tot_m, totais_m = lancamentos_buscar_pagina(
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
            f"Mongo (lista dedup): qtd={tot_m}  a_pagar={tm.get('saldo_aberto')}"
        )

        delta = _dec(tp.get("saldo_aberto")) - _dec(tm.get("saldo_aberto"))
        self.stdout.write(f"Delta PG − Mongo (a pagar período): {_fmt(delta)}")

        qs_abertos = titulos_financeiro_montar_qs(despesa=True, status="abertos")
        pg_abertos = dedup_titulos(list(qs_abertos[:50_000]))
        mongo_ids = set()
        try:
            for doc in col.find({"Despesa": True}, {"_id": 1}).limit(50_000):
                mongo_ids.add(str(doc.get("_id")))
        except Exception as exc:
            self.stdout.write(self.style.WARNING(f"  Falha scan Mongo ids: {exc}"))
            mongo_ids = set()

        pg_ids = {t.mongo_id for t in pg_abertos if t.mongo_id}
        so_pg = len(pg_ids - mongo_ids) if mongo_ids else 0
        so_mongo = len(mongo_ids - pg_ids) if pg_ids else len(mongo_ids)
        self.stdout.write(
            f"\nCP abertos dedup: PG={len(pg_ids)} · Mongo={len(mongo_ids)} · "
            f"só PG={so_pg} · só Mongo={so_mongo}"
        )

        if abs(float(delta)) > 500:
            self.stdout.write(
                self.style.WARNING(
                    "  ⚠ Divergência > R$ 500 no período — conferir backup/checkpoint antes do corte."
                )
            )
        elif abs(float(delta)) <= 1:
            self.stdout.write(self.style.SUCCESS("  ✓ Totais CP período alinhados (≤ R$ 1)."))
        else:
            self.stdout.write(
                self.style.WARNING("  ⚠ Pequena divergência — pode ser timing de pagamento recente.")
            )

        self.stdout.write(
            "\nComandos úteis: diagnosticar_cp_pg_mongo · auditar_entrada_nfe_financeiro\n"
        )
