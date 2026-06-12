"""
Reconstrói vínculos de entrada NF (cProd fornecedor + descrição) a partir de rascunhos salvos.

Use após deploy — cobre notas antigas que gravaram só código GM (ex.: GM1542) em vez de R0151:

  python manage.py agro_backfill_c_prod_nf_entrada
  python manage.py agro_backfill_c_prod_nf_entrada --limit 500
"""

from __future__ import annotations

from django.core.management.base import BaseCommand

from produtos.nfe_entrada_util import COL_ENTRADA_RASCUNHO, persistir_vinculos_c_prod_entrada_nfe_linhas
from produtos.views import obter_conexao_mongo


class Command(BaseCommand):
    help = "Grava vínculos cProd/descrição NF no overlay e AgroEntradaNfeVinculo (histórico)."

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
        try:
            from produtos.nfe_entrada_util import COL_ENTRADA_VINCULO

            db[COL_ENTRADA_VINCULO].create_index(
                [("tipo", 1), ("chave", 1), ("emit_cnpj", 1)],
                unique=True,
                name="tipo_chave_emit",
            )
        except Exception:
            pass
        lim = int(options.get("limit") or 0)
        cur = db[COL_ENTRADA_RASCUNHO].find(
            {"linhas": {"$exists": True, "$ne": []}},
            projection={"linhas": 1, "cabecalho": 1},
        ).sort("criado_em", -1)
        if lim > 0:
            cur = cur.limit(lim)
        total_vinc = 0
        docs = 0
        for doc in cur:
            docs += 1
            cab = doc.get("cabecalho") if isinstance(doc.get("cabecalho"), dict) else {}
            emit = str(cab.get("emit_cnpj") or "").strip()
            linhas = doc.get("linhas") if isinstance(doc.get("linhas"), list) else []
            total_vinc += persistir_vinculos_c_prod_entrada_nfe_linhas(
                db, client.col_p, linhas, emit_cnpj=emit
            )
        self.stdout.write(
            self.style.SUCCESS(
                f"Concluído: {docs} rascunho(s); {total_vinc} vínculo(s) gravado(s) "
                f"(cProd fornecedor e/ou descrição + CNPJ)."
            )
        )
