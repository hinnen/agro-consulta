from django.core.management.base import BaseCommand

from produtos.fiado_gestao_util import backfill_titulos_vendas_fiado


class Command(BaseCommand):
    help = (
        "Gera títulos fiado para vendas PDV antigas e completa fatia faltante "
        "(ex.: frete na 2ª linha Fiado). Use --pk para uma venda (ex. 3437)."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--limite",
            type=int,
            default=8000,
            help="Máximo de vendas fiado a analisar (ignorado com --pk).",
        )
        parser.add_argument(
            "--pk",
            type=int,
            default=None,
            help="Só esta venda (ex. 3437 da Joelma). Evita complemento em lote.",
        )

    def handle(self, *args, **options):
        pk = options.get("pk")
        r = backfill_titulos_vendas_fiado(
            limite=int(options.get("limite") or 8000),
            venda_pk=int(pk) if pk else None,
        )
        self.stdout.write(self.style.SUCCESS(str(r)))
