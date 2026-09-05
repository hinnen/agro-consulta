"""Importa vendas diárias (Excel) para meta C do BI — planilha Centro."""
from __future__ import annotations

from pathlib import Path

from django.conf import settings
from django.core.management.base import BaseCommand

from produtos.dashboard_vendas_historico_util import importar_dashboard_vendas_historico_xlsx


class Command(BaseCommand):
    help = (
        "Importa vendas diárias da planilha (data + total) para DashboardVendaDiaHistoricoAgro. "
        "Padrão: docs/dados/vendas_centro_nov2025.xlsx"
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "arquivo",
            nargs="?",
            default="",
            help="Caminho .xlsx (default: docs/dados/vendas_centro_nov2025.xlsx na raiz do projeto)",
        )
        parser.add_argument(
            "--limpar-intervalo",
            action="store_true",
            help="Apaga registros do intervalo da planilha antes de importar",
        )
        parser.add_argument(
            "--limpar-tudo",
            action="store_true",
            help="Apaga todos os registros historicos antes de importar",
        )
        parser.add_argument(
            "--deposito",
            default="centro",
            help="Depósito (default: centro)",
        )

    def handle(self, *args, **options):
        raw = (options.get("arquivo") or "").strip()
        if raw:
            path = Path(raw)
        else:
            path = Path(settings.BASE_DIR) / "docs" / "dados" / "vendas_centro_nov2025.xlsx"

        r = importar_dashboard_vendas_historico_xlsx(
            path,
            deposito=options.get("deposito") or "centro",
            limpar_intervalo=bool(options.get("limpar_intervalo")),
            limpar_tudo=bool(options.get("limpar_tudo")),
        )
        if not r.get("ok"):
            self.stderr.write(self.style.ERROR(r.get("erro") or "Falha"))
            return
        self.stdout.write(
            self.style.SUCCESS(
                f"OK · {r.get('linhas')} linhas · {r.get('de')} -> {r.get('ate')} · "
                f"+{r.get('inseridos')} · ~{r.get('atualizados')} atualizados"
            )
        )
