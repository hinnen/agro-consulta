from django.core.management.base import BaseCommand

from produtos.services_clientes_sync import sincronizar_clientes_fontes_para_agro


class Command(BaseCommand):
    help = (
        "Importa clientes de Mongo (DtoPessoa) e API ERP para ClienteAgro. "
        "Não envia dados ao ERP. Respeita editado_local."
    )

    def handle(self, *args, **options):
        r = sincronizar_clientes_fontes_para_agro()
        self.stdout.write(self.style.SUCCESS(str(r)))
