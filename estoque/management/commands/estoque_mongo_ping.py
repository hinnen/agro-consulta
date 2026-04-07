"""
Ping leve ao Mongo (um find_one na coleção de produtos). Atualiza EstoqueSyncHealth.
Não invalida cache do catálogo PDV — adequado para cron frequente.
"""

from django.core.management.base import BaseCommand

from estoque.sync_health import registrar_ping_mongo


class Command(BaseCommand):
    help = "Ping Mongo para health de estoque (sem rebuild de catálogo)."

    def handle(self, *args, **options):
        from produtos.views import obter_conexao_mongo

        client, db = obter_conexao_mongo()
        if db is None:
            registrar_ping_mongo(False, "Mongo indisponível (estoque_mongo_ping)")
            self.stderr.write(self.style.ERROR("Mongo indisponível."))
            raise SystemExit(1)
        try:
            db[client.col_p].find_one({}, {"_id": 1})
            registrar_ping_mongo(True)
            self.stdout.write(self.style.SUCCESS("Ping Mongo OK."))
        except Exception as e:
            registrar_ping_mongo(False, str(e))
            self.stderr.write(self.style.ERROR(str(e)))
            raise SystemExit(1)
