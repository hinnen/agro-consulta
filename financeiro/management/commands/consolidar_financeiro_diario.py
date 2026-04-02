from django.core.management.base import BaseCommand


class Command(BaseCommand):
    help = (
        "Reserva para agregação diária de lançamentos no Postgres "
        "(LancamentoFinanceiro). Hoje: sem ETL automático."
    )

    def handle(self, *args, **options):
        self.stdout.write(
            self.style.WARNING(
                "Nada a executar: importe/classifique lançamentos antes "
                "(importar_lancamentos_financeiros + regras de natureza)."
            )
        )
