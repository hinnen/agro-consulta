"""
Preenche ``index_codigos`` e ``AgroIndexCodigosAt`` em ``DtoProduto`` (denormalização para busca rápida).

Inclui similares do ERP (incl. nomes alternativos de array), overlay Agro (barras/NFe) e variações SQLite.
Rode após deploy ou quando o espelho ERP for atualizado
fora do Agro.

  python manage.py agro_rebuild_index_codigos
  python manage.py agro_rebuild_index_codigos --limit 1000
"""

from __future__ import annotations

from django.core.management.base import BaseCommand
from django.utils import timezone
from pymongo import UpdateOne

from produtos.mongo_index_codigos import (
    AGRO_INDEX_AT_CAMPO,
    INDEX_CODIGOS_CAMPO,
    mapa_extras_agro_por_produto_externo_id,
    montar_index_codigos_final,
    projection_documento_para_rebuild_index,
)
from produtos.views import obter_conexao_mongo


class Command(BaseCommand):
    help = "Recalcula index_codigos (multikey) em DtoProduto para busca por código instantânea."

    def add_arguments(self, parser):
        parser.add_argument(
            "--batch",
            type=int,
            default=500,
            help="Tamanho do bulk_write (padrão 500).",
        )
        parser.add_argument(
            "--limit",
            type=int,
            default=0,
            help="Processa no máximo N produtos (0 = todos).",
        )

    def handle(self, *args, **options):
        batch = max(50, min(int(options["batch"] or 500), 5000))
        lim = int(options["limit"] or 0)

        client, db = obter_conexao_mongo()
        if db is None or client is None:
            self.stderr.write(self.style.ERROR("Mongo indisponível."))
            return

        col = db[client.col_p]
        proj = projection_documento_para_rebuild_index()
        extras_map = mapa_extras_agro_por_produto_externo_id()
        now = timezone.now()

        filt: dict = {}
        cur = col.find(filt, proj, batch_size=batch, no_cursor_timeout=True)
        ops: list[UpdateOne] = []
        total = 0
        try:
            for doc in cur:
                pid = str(doc.get("Id") or doc.get("_id") or "")
                extras = extras_map.get(pid)
                idx = montar_index_codigos_final(doc, extras_sqlite=extras)
                ops.append(
                    UpdateOne(
                        {"_id": doc["_id"]},
                        {"$set": {INDEX_CODIGOS_CAMPO: idx, AGRO_INDEX_AT_CAMPO: now}},
                    )
                )
                total += 1
                if len(ops) >= batch:
                    col.bulk_write(ops, ordered=False)
                    ops = []
                    self.stdout.write(f"… {total} documentos")
                if lim and total >= lim:
                    break
        finally:
            cur.close()

        if ops:
            col.bulk_write(ops, ordered=False)

        try:
            col.create_index(INDEX_CODIGOS_CAMPO)
            self.stdout.write(self.style.SUCCESS(f"Índice em {INDEX_CODIGOS_CAMPO} garantido."))
        except Exception as exc:
            self.stdout.write(self.style.WARNING(f"Índice: {exc}"))

        self.stdout.write(
            self.style.SUCCESS(f"Concluído: {total} produto(s) atualizado(s) com {INDEX_CODIGOS_CAMPO}.")
        )
