from pathlib import Path

from django.core.management.base import BaseCommand, CommandError

from produtos.fiado_import_util import aplicar_importacao_fiados


class Command(BaseCommand):
    help = (
        "Importa fiados pendentes (CSV/XLSX ERP) para FiadoTituloAgro. "
        "Colunas: Código, Cliente, Número Documento, Vencimento, Valor, Valor Pago, Situação."
    )

    def add_arguments(self, parser):
        parser.add_argument("--arquivo", required=True, help="Caminho do .csv ou .xlsx exportado.")
        parser.add_argument(
            "--apenas-seco",
            action="store_true",
            help="Simula sem gravar.",
        )

    def handle(self, *args, **options):
        path = Path(str(options["arquivo"])).expanduser()
        if not path.exists():
            raise CommandError(f"Arquivo não encontrado: {path}")
        try:
            r = aplicar_importacao_fiados(path, dry_run=bool(options.get("apenas_seco")))
        except ValueError as exc:
            raise CommandError(str(exc)) from exc
        self.stdout.write(self.style.SUCCESS(str(r)))
        if r.get("sem_cliente"):
            self.stdout.write(
                self.style.WARNING(
                    f"{r['sem_cliente']} linha(s) sem ClienteAgro casado — título criado só com nome/código."
                )
            )
