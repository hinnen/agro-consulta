"""
Invalida o cache do catálogo PDV e força rebuild na próxima requisição.
Opcionalmente executa um ping ao Mongo e atualiza ``EstoqueSyncHealth``.
"""

from django.core.cache import cache
from django.core.management.base import BaseCommand

from estoque.sync_health import registrar_ping_mongo


class Command(BaseCommand):
    help = "Reconciliar: limpa cache do catálogo PDV e opcionalmente ping Mongo."

    def add_arguments(self, parser):
        parser.add_argument(
            "--ping",
            action="store_true",
            help="Executa find_one em DtoProduto e atualiza EstoqueSyncHealth.",
        )

    def handle(self, *args, **options):
        from produtos.views import (
            CATALOGO_PDV_CACHE_ENTRY_KEY,
            CATALOGO_PDV_CACHE_PREV_ENTRY_KEY,
            _catalogo_pdv_entry_atual,
            obter_conexao_mongo,
        )

        cache.delete(CATALOGO_PDV_CACHE_ENTRY_KEY)
        cache.delete(CATALOGO_PDV_CACHE_PREV_ENTRY_KEY)
        self.stdout.write(self.style.SUCCESS("Cache do catálogo PDV invalidado."))

        client, db = obter_conexao_mongo()
        if db is None:
            registrar_ping_mongo(False, "Mongo indisponível (reconciliar)")
            self.stderr.write(self.style.ERROR("Mongo indisponível."))
            return

        if options.get("ping"):
            try:
                db[client.col_p].find_one({}, {"_id": 1})
                registrar_ping_mongo(True)
                self.stdout.write(self.style.SUCCESS("Ping Mongo OK."))
            except Exception as e:
                registrar_ping_mongo(False, str(e))
                self.stderr.write(self.style.ERROR(str(e)))
                return

        try:
            entry = _catalogo_pdv_entry_atual(db, client)
            self.stdout.write(
                self.style.SUCCESS(
                    f"Catálogo reconstruído: versão {entry.get('version')} — {len((entry.get('body') or {}).get('produtos') or [])} produtos."
                )
            )
        except Exception as e:
            self.stderr.write(self.style.ERROR(str(e)))
