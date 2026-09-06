"""Importa AgroEntradaNotaRascunho (Mongo) → EntradaNotaRascunhoAgro (Postgres)."""
from __future__ import annotations

from django.core.management.base import BaseCommand

from produtos.entrada_nota_rascunho_pg_util import importar_rascunhos_mongo_batch
from produtos.models import EntradaNotaRascunhoAgro
from produtos.views import obter_conexao_mongo


class Command(BaseCommand):
    help = (
        "Copia rascunhos Entrada NF do Mongo para Postgres (EntradaNotaRascunhoAgro). "
        "Idempotente — upsert por rascunho_id (= _id Mongo)."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--limite",
            type=int,
            default=5000,
            help="Máximo de documentos a importar (default 5000).",
        )

    def handle(self, *args, **options):
        limite = max(1, int(options.get("limite") or 5000))
        antes = EntradaNotaRascunhoAgro.objects.count()
        _, db = obter_conexao_mongo()
        if db is None:
            self.stderr.write(self.style.ERROR("Mongo indisponível — não há origem para importar."))
            return
        r = importar_rascunhos_mongo_batch(db, limit=limite)
        depois = EntradaNotaRascunhoAgro.objects.count()
        self.stdout.write(
            self.style.SUCCESS(
                f"Importados/atualizados: {r.get('ok', 0)} · PG antes={antes} depois={depois}"
            )
        )
        if r.get("erro"):
            self.stderr.write(self.style.WARNING("Houve erro parcial — ver log."))
