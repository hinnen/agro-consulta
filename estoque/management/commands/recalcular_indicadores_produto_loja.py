from django.core.management.base import BaseCommand


class Command(BaseCommand):
    help = (
        "Recalcula snapshots em estoque.IndicadorProdutoLoja "
        "(vendas, saldos, ABC, sugestão transferência/compra). "
        "Implementação completa depende de fonte de venda média e saldos."
    )

    def add_arguments(self, parser):
        parser.add_argument("--empresa-id", type=int, default=None)
        parser.add_argument("--data-base", type=str, default=None)

    def handle(self, *args, **options):
        self.stdout.write(
            self.style.WARNING(
                "Stub: iterar empresa/loja/produto e chamar "
                "IndicadoresProdutoLojaService().upsert_snapshot(...)."
            )
        )
