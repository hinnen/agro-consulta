"""Importa DtoLancamento (Mongo) → TituloFinanceiroAgro (Postgres). Default: dry-run."""
from __future__ import annotations

from django.core.management.base import BaseCommand

from produtos.lancamentos_financeiro_agro_util import (
    importar_titulos_financeiro_mongo_para_postgres,
)
from produtos.views import obter_conexao_mongo


class Command(BaseCommand):
    help = (
        "Espelha títulos CP/CR do Mongo (DtoLancamento) no Postgres TituloFinanceiroAgro. "
        "Por padrão só conta (dry-run). Use --apply para gravar. "
        "Não altera telas de Lançamentos nem AGRO_FONTE_FINANCEIRO."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--apply",
            action="store_true",
            help="Grava no Postgres (sem isso, só relatório).",
        )
        parser.add_argument(
            "--limite",
            type=int,
            default=0,
            help="Máximo de documentos Mongo a processar (0 = todos).",
        )
        parser.add_argument(
            "--despesa",
            choices=("pagar", "receber", "todos"),
            default="todos",
            help="Filtrar CP (pagar) ou CR (receber).",
        )

    def handle(self, *args, **options):
        dry = not bool(options.get("apply"))
        limite = int(options.get("limite") or 0) or None
        filtro = options.get("despesa") or "todos"
        despesa: bool | None
        if filtro == "pagar":
            despesa = True
        elif filtro == "receber":
            despesa = False
        else:
            despesa = None

        _, db = obter_conexao_mongo()
        if db is None:
            self.stderr.write(self.style.ERROR("Mongo indisponível."))
            return

        r = importar_titulos_financeiro_mongo_para_postgres(
            db,
            dry_run=dry,
            limite=limite,
            despesa=despesa,
        )
        if not r.get("ok"):
            self.stderr.write(self.style.ERROR(str(r.get("erro") or "Falha")))
            return

        modo = "DRY-RUN" if dry else "APPLY"
        self.stdout.write(self.style.WARNING(f"[{modo}] Importacao Mongo -> Postgres financeiro"))
        self.stdout.write(f"  Mongo (filtro): {r.get('total_mongo')} documento(s)")
        self.stdout.write(f"  Lidos nesta execução: {r.get('lidos')}")
        self.stdout.write(f"  CP: {r.get('cp')} | CR: {r.get('cr')}")
        self.stdout.write(f"  Quitados: {r.get('quitados')} | Em aberto: {r.get('abertos')}")
        self.stdout.write(f"  AgroFonteVerdade no Mongo: {r.get('congelados_mongo')}")
        self.stdout.write(
            f"  Bruto total: R$ {r.get('bruto_total'):,.2f} | Restante total: R$ {r.get('restante_total'):,.2f}"
        )
        self.stdout.write(
            f"  Postgres: {r.get('pg_antes')} -> {r.get('pg_depois')} titulo(s) "
            f"(criar {r.get('criar')}, atualizar {r.get('atualizar')})"
        )
        if r.get("ignorados_sem_id"):
            self.stdout.write(
                self.style.ERROR(f"  Ignorados sem _id: {r.get('ignorados_sem_id')}")
            )
        if r.get("erros_amostra"):
            self.stdout.write(f"  Amostra erros: {r.get('erros_amostra')}")

        if dry:
            self.stdout.write("")
            self.stdout.write(
                "Seguro: nada gravado. Para importar de verdade: "
                "python manage.py importar_titulos_financeiro_mongo_pg --apply"
            )
        else:
            self.stdout.write(self.style.SUCCESS("Importação concluída."))
