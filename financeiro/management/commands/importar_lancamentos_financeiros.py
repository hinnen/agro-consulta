from django.core.management.base import BaseCommand


class Command(BaseCommand):
    help = (
        "Reserva para importar lançamentos (ERP / planilha) para "
        "financeiro.LancamentoFinanceiro com natureza gerencial correta."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Não grava (futuro).",
        )

    def handle(self, *args, **options):
        self.stdout.write(
            self.style.WARNING(
                "Stub: implementar leitura da fonte e mapeamento "
                "(classificacao_lancamentos / plano de contas)."
            )
        )
