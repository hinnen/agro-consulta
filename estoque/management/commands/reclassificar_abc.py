from django.core.management.base import BaseCommand


class Command(BaseCommand):
    help = (
        "Reclassifica ABC (90 dias) e pode alimentar próximo "
        "recalcular_indicadores_produto_loja."
    )

    def handle(self, *args, **options):
        self.stdout.write(
            self.style.WARNING(
                "Stub: agregar valor vendido por produto (90d) e "
                "ClassificacaoABCService.classificar(...)."
            )
        )
