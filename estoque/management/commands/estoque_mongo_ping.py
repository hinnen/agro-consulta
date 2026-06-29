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
        self._keep_warm_staging()

    def _keep_warm_staging(self):
        """Acorda o web staging (Render spin-down ~15 min). Falha não derruba o ping Mongo."""
        try:
            from scripts.render_keep_warm import main as keep_warm_main

            if keep_warm_main() == 0:
                self.stdout.write(self.style.SUCCESS("Keep-warm staging OK."))
            else:
                self.stderr.write(
                    self.style.WARNING(
                        "Keep-warm staging: nenhuma URL respondeu (confira AGRO_KEEP_WARM_URLS no Render)."
                    )
                )
        except Exception as exc:
            self.stderr.write(self.style.WARNING(f"Keep-warm staging ignorado: {exc}"))
