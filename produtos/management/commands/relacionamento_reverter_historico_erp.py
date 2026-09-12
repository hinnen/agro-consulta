"""Reverte import histórico ERP do F8 (FL-042)."""
from __future__ import annotations

from django.core.management.base import BaseCommand

from produtos.relacionamento_historico_erp_util import reverter_historico_erp


class Command(BaseCommand):
    help = "Remove lote (ou tudo) do histórico ERP importado — F8 volta a ignorar essas vendas."

    def add_arguments(self, parser):
        parser.add_argument("--lote", default="", help="ID do lote a apagar")
        parser.add_argument(
            "--tudo",
            action="store_true",
            help="Apaga todo histórico ERP importado",
        )

    def handle(self, *args, **options):
        r = reverter_historico_erp(
            lote_id=str(options.get("lote") or ""),
            tudo=bool(options.get("tudo")),
        )
        if not r.get("ok"):
            self.stderr.write(self.style.ERROR(r.get("erro") or "Falha"))
            return
        self.stdout.write(
            self.style.SUCCESS(
                f"OK · vendas removidas: {r.get('vendas_removidas')} · "
                f"lotes: {r.get('lotes_removidos', 1)}"
            )
        )
