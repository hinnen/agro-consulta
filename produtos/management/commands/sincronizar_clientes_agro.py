from django.core.cache import cache
from django.core.management.base import BaseCommand

from produtos.clientes_sync_web_state import (
    marcar_clientes_sync_erp_finalizado,
    mark_done,
    mark_failed,
    mark_running,
)
from produtos.services_clientes_sync import (
    CLIENTES_SYNC_LOCK_KEY,
    CLIENTES_SYNC_RESULT_KEY,
    sincronizar_clientes_fontes_para_agro,
)

API_LIST_CUSTOMERS_CACHE_KEY = "api_list_customers_v2"


class Command(BaseCommand):
    help = (
        "Importa clientes de Mongo (DtoPessoa) e API ERP para ClienteAgro. "
        "Não envia dados ao ERP. Respeita editado_local."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--gravar-resultado-web",
            action="store_true",
            help="Grava resultado no cache para a tela Clientes (uso interno do botão Sincronizar).",
        )

    def handle(self, *args, **options):
        web = bool(options.get("gravar_resultado_web"))
        if web:
            mark_running()
        try:
            r = sincronizar_clientes_fontes_para_agro()
            if web:
                mark_done(r)
                if r.get("ok"):
                    marcar_clientes_sync_erp_finalizado()
                cache.set(CLIENTES_SYNC_RESULT_KEY, r, timeout=600)
                cache.delete(API_LIST_CUSTOMERS_CACHE_KEY)
            self.stdout.write(self.style.SUCCESS(str(r)))
        except Exception as exc:
            if web:
                mark_failed(str(exc))
                cache.set(
                    CLIENTES_SYNC_RESULT_KEY,
                    {"ok": False, "erro": str(exc)},
                    timeout=600,
                )
            raise
        finally:
            if web:
                cache.delete(CLIENTES_SYNC_LOCK_KEY)
