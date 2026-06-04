from django.core.management.base import BaseCommand

from produtos.fiado_gestao_util import backfill_titulos_vendas_fiado


class Command(BaseCommand):
    help = "Gera títulos fiado para vendas PDV antigas que ainda não têm ledger."

    def add_arguments(self, parser):
        parser.add_argument("--limite", type=int, default=500, help="Máximo de vendas a analisar.")

    def handle(self, *args, **options):
        r = backfill_titulos_vendas_fiado(limite=int(options.get("limite") or 500))
        self.stdout.write(self.style.SUCCESS(str(r)))
