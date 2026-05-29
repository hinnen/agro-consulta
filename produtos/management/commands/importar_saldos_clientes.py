from pathlib import Path

from django.core.management.base import BaseCommand, CommandError

from produtos.cliente_saldos_import_util import aplicar_importacao


class Command(BaseCommand):
    help = (
        "Importa saldo inicial de clientes (cashback/vale/fiado) a partir de CSV ou XLSX do ERP. "
        "Cliente repetido na planilha: valores são SOMADOS antes de gravar. "
        "Planilha Codigo/Cliente/Valor: use --tipo vale. "
        "Segunda planilha no mesmo cliente: use --somar para acrescentar ao saldo já no Agro."
    )

    def add_arguments(self, parser):
        parser.add_argument("--arquivo", required=True, help="Caminho do .csv ou .xlsx exportado do ERP.")
        parser.add_argument(
            "--apenas-seco",
            action="store_true",
            help="Só simula: mostra quantos casariam, sem gravar.",
        )
        parser.add_argument(
            "--relatorio",
            default="",
            help="CSV de saída com nomes não encontrados ou ambíguos (ex.: import_saldos_pendencias.csv).",
        )
        parser.add_argument("--col-nome", default="", help="Nome exato da coluna de cliente na planilha.")
        parser.add_argument("--col-cashback", default="", help="Nome exato da coluna de saldo cashback.")
        parser.add_argument("--col-vale", default="", help="Nome exato da coluna de vale crédito.")
        parser.add_argument("--col-fiado", default="", help="Nome exato da coluna de limite fiado.")
        parser.add_argument(
            "--tipo",
            default="auto",
            choices=["auto", "cashback", "vale"],
            help="Qual saldo a coluna Valor representa (vale = planilha Codigo/Cliente/Valor).",
        )
        parser.add_argument(
            "--somar",
            action="store_true",
            help="Soma o valor da planilha ao saldo já gravado no cliente (em vez de substituir).",
        )

    def handle(self, *args, **options):
        path = Path(str(options["arquivo"])).expanduser()
        if not path.exists():
            raise CommandError(f"Arquivo não encontrado: {path}")

        rel = (options.get("relatorio") or "").strip()
        rel_path = Path(rel).expanduser() if rel else path.with_name(path.stem + "_pendencias.csv")

        try:
            r = aplicar_importacao(
                path,
                dry_run=bool(options.get("apenas_seco")),
                tipo_saldo=(options.get("tipo") or "auto").strip(),
                modo="somar" if options.get("somar") else "substituir",
                col_nome=(options.get("col_nome") or "").strip() or None,
                col_cashback=(options.get("col_cashback") or "").strip() or None,
                col_vale=(options.get("col_vale") or "").strip() or None,
                col_fiado=(options.get("col_fiado") or "").strip() or None,
                relatorio_path=rel_path,
            )
        except PermissionError as exc:
            raise CommandError(
                f"Sem permissão para ler o arquivo (feche o Excel e aguarde o OneDrive): {path}"
            ) from exc
        except ValueError as exc:
            raise CommandError(str(exc)) from exc

        self.stdout.write(self.style.SUCCESS(str(r)))
        if r.get("nao_encontrados") or r.get("ambiguos"):
            self.stdout.write(
                self.style.WARNING(
                    f"Revise o relatório: {rel_path} "
                    f"(nao_encontrados={r['nao_encontrados']}, ambiguos={r['ambiguos']})"
                )
            )
