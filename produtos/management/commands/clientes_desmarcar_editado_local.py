from django.core.management.base import BaseCommand

from produtos.models import ClienteAgro


class Command(BaseCommand):
    help = (
        "Zera editado_local em massa para permitir que a sync ERP/Mongo atualize nomes e endereços. "
        "Use só se os clientes NÃO foram editados manualmente no Agro (ex.: após import de saldos antigo)."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--confirmar",
            action="store_true",
            help="Executa a alteração (sem isso, só mostra a contagem).",
        )

    def handle(self, *args, **options):
        qs = ClienteAgro.objects.filter(editado_local=True)
        n = qs.count()
        self.stdout.write(f"Clientes com «Ajustado no Agro» (editado_local): {n}")
        if not options["confirmar"]:
            self.stdout.write(
                "Dry-run. Para aplicar: python manage.py clientes_desmarcar_editado_local --confirmar"
            )
            return
        atualizados = qs.update(editado_local=False)
        self.stdout.write(self.style.SUCCESS(f"Atualizados: {atualizados}"))
