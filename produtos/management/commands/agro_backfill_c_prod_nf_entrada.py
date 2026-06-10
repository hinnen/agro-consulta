"""
Reconstrói vínculos cProd (XML) → produto a partir dos rascunhos/notas de entrada NF já salvos.

Use uma vez após deploy do auto-casamento por cProd do fornecedor:

  python manage.py agro_backfill_c_prod_nf_entrada
  python manage.py agro_backfill_c_prod_nf_entrada --limit 500
"""

from __future__ import annotations

from django.core.management.base import BaseCommand

from produtos.nfe_entrada_util import COL_ENTRADA_RASCUNHO, persistir_vinculos_c_prod_entrada_nfe_linhas
from produtos.views import obter_conexao_mongo


class Command(BaseCommand):
    help = "Grava cProd da NF no overlay a partir de AgroEntradaNotaRascunho (histórico)."

    def add_arguments(self, parser):
        parser.add_argument(
            "--limit",
            type=int,
            default=0,
            help="Máximo de rascunhos a ler (0 = todos).",
        )

    def handle(self, *args, **options):
        client, db = obter_conexao_mongo()
        if db is None or client is None:
            self.stderr.write(self.style.ERROR("Mongo indisponível."))
            return
        lim = int(options.get("limit") or 0)
        cur = db[COL_ENTRADA_RASCUNHO].find(
            {"linhas": {"$exists": True, "$ne": []}},
            projection={"linhas": 1},
        ).sort("criado_em", -1)
        if lim > 0:
            cur = cur.limit(lim)
        total_vinc = 0
        docs = 0
        for doc in cur:
            docs += 1
            linhas = doc.get("linhas") if isinstance(doc.get("linhas"), list) else []
            total_vinc += persistir_vinculos_c_prod_entrada_nfe_linhas(db, client.col_p, linhas)
        self.stdout.write(
            self.style.SUCCESS(
                f"Concluído: {docs} rascunho(s) lidos; {total_vinc} vínculo(s) cProd novo(s) gravado(s)."
            )
        )
