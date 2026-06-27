"""Sincroniza Mongo → Postgres (reimport + remove órfãos). Default: dry-run."""
from __future__ import annotations

from datetime import date

from django.core.management.base import BaseCommand

from produtos.lancamentos_financeiro_agro_util import (
    sincronizar_titulos_financeiro_mongo_para_postgres,
)
from produtos.lancamentos_financeiro_pg_util import contas_pagar_buscar_pagina_pg
from produtos.views import obter_conexao_mongo


class Command(BaseCommand):
    help = (
        "Reimporta DtoLancamento → TituloFinanceiroAgro e remove PG órfãos "
        "(mongo_id ausente no Mongo). Default dry-run."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--apply",
            action="store_true",
            help="Grava no Postgres (sem isso, só relatório).",
        )
        parser.add_argument(
            "--sem-orfaos",
            action="store_true",
            help="Não apaga linhas PG sem par no Mongo.",
        )
        parser.add_argument(
            "--conferir-jul",
            action="store_true",
            help="Após sync, mostra totais CP jul/2026 abertos.",
        )

    def handle(self, *args, **options):
        dry = not bool(options.get("apply"))
        remover = not bool(options.get("sem_orfaos"))

        _, db = obter_conexao_mongo()
        if db is None:
            self.stderr.write(self.style.ERROR("Mongo indisponivel."))
            return

        r = sincronizar_titulos_financeiro_mongo_para_postgres(
            db,
            dry_run=dry,
            despesa=None,
            remover_orfaos=remover,
        )
        if not r.get("ok"):
            self.stderr.write(self.style.ERROR(str(r.get("erro") or "Falha")))
            return

        modo = "DRY-RUN" if dry else "APPLY"
        self.stdout.write(self.style.WARNING(f"[{modo}] Sync Mongo -> Postgres financeiro"))
        self.stdout.write(f"  Mongo documentos: {r.get('total_mongo')}")
        self.stdout.write(f"  Lidos: {r.get('lidos')} | criar {r.get('criar')} | atualizar {r.get('atualizar')}")
        self.stdout.write(f"  Orfaos PG (sem Mongo): {r.get('orfaos_pg')}")
        if not dry:
            self.stdout.write(f"  Orfaos removidos: {r.get('orfaos_removidos')}")
        self.stdout.write(
            f"  Postgres: {r.get('pg_antes')} -> {r.get('pg_depois')} titulo(s)"
        )

        if options.get("conferir_jul") or not dry:
            v_de = date(2026, 7, 1)
            v_ate = date(2026, 7, 31)
            _, tot, totais = contas_pagar_buscar_pagina_pg(
                status="abertos",
                vencimento_de=v_de,
                vencimento_ate=v_ate,
                page=1,
                page_size=1,
                skip_totais=False,
                limite_max=25_000,
            )
            tp = totais or {}
            self.stdout.write(
                f"\n  CP jul/2026 PG: qtd={tot}  a_pagar={tp.get('saldo_aberto')}"
            )

        if dry:
            self.stdout.write("")
            self.stdout.write(
                "Seguro: nada gravado. Para aplicar: "
                "python manage.py sincronizar_titulos_financeiro_mongo_pg --apply --conferir-jul"
            )
        else:
            self.stdout.write(self.style.SUCCESS("Sync concluido."))
