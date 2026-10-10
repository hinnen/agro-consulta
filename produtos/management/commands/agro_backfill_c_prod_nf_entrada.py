"""
Reconstrói vínculos de entrada NF (cProd fornecedor + descrição) no **Postgres**.

Cobre notas antigas que gravaram só código GM ou só no Mongo:

  python manage.py agro_backfill_c_prod_nf_entrada
  python manage.py agro_backfill_c_prod_nf_entrada --limit 500
"""

from __future__ import annotations

from django.core.management.base import BaseCommand

from produtos.nfe_entrada_util import persistir_vinculos_c_prod_entrada_nfe_linhas


class Command(BaseCommand):
    help = "Grava vínculos cProd/descrição NF no Postgres (EntradaNfeVinculoAgro) + overlay."

    def add_arguments(self, parser):
        parser.add_argument(
            "--limit",
            type=int,
            default=0,
            help="Máximo de rascunhos a ler (0 = todos).",
        )

    def handle(self, *args, **options):
        from produtos.models import EntradaNotaRascunhoAgro

        from produtos.views import obter_conexao_mongo

        client, db = obter_conexao_mongo()
        col_p = "DtoProduto"
        if client is not None:
            col_p = getattr(client, "col_p", None) or "DtoProduto"

        lim = int(options.get("limit") or 0)
        qs = EntradaNotaRascunhoAgro.objects.exclude(status="descartada").order_by(
            "-atualizado_em", "-criado_em"
        )
        if lim > 0:
            qs = qs[:lim]

        total_vinc = 0
        docs = 0
        for r in qs.iterator(chunk_size=50):
            docs += 1
            cab = r.cabecalho if isinstance(r.cabecalho, dict) else {}
            emit = str(cab.get("emit_cnpj") or "").strip()
            linhas = r.linhas if isinstance(r.linhas, list) else []
            total_vinc += persistir_vinculos_c_prod_entrada_nfe_linhas(
                db, col_p, linhas, emit_cnpj=emit
            )

        # Legado Mongo (se ainda existir coleção de rascunho).
        if db is not None and client is not None:
            try:
                from produtos.nfe_entrada_util import COL_ENTRADA_RASCUNHO

                cur = db[COL_ENTRADA_RASCUNHO].find(
                    {"linhas": {"$exists": True, "$ne": []}},
                    projection={"linhas": 1, "cabecalho": 1},
                ).sort("criado_em", -1)
                if lim > 0:
                    cur = cur.limit(lim)
                for doc in cur:
                    docs += 1
                    cab = doc.get("cabecalho") if isinstance(doc.get("cabecalho"), dict) else {}
                    emit = str(cab.get("emit_cnpj") or "").strip()
                    linhas = doc.get("linhas") if isinstance(doc.get("linhas"), list) else []
                    total_vinc += persistir_vinculos_c_prod_entrada_nfe_linhas(
                        db, col_p, linhas, emit_cnpj=emit
                    )
            except Exception as exc:
                self.stderr.write(f"Mongo legado (ok se cortado): {exc}")

        self.stdout.write(
            self.style.SUCCESS(
                f"Concluído: {docs} rascunho(s); {total_vinc} vínculo(s) gravado(s) no Postgres."
            )
        )
