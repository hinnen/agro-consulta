"""Copia catálogo PDV da loja (Postgres produção) → staging."""
from __future__ import annotations

from django.core.management.base import BaseCommand

from produtos.snapshot_pdv_loja_util import executar_snapshot_pdv_loja


class Command(BaseCommand):
    help = (
        "Copia Produto + ProdutoGestaoOverlayAgro (+ ajustes estoque) da loja para o Postgres do staging. "
        "Exige AGRO_STAGING_READONLY=true, AGRO_ERP_PEDIDOS_DRY_RUN=true e AGRO_SNAPSHOT_FONTE_DATABASE_URL."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--sem-ajustes-estoque",
            action="store_true",
            help="Não copia AjusteRapidoEstoque (só catálogo/preço).",
        )

    def handle(self, *args, **options):
        out = executar_snapshot_pdv_loja(
            incluir_ajustes_estoque=not options.get("sem_ajustes_estoque"),
        )
        if not out.get("ok"):
            self.stderr.write(self.style.ERROR(out.get("erro") or "Falha"))
            return
        self.stdout.write(
            self.style.SUCCESS(
                f"OK — produtos={out.get('produtos')} overlays={out.get('overlays')} "
                f"ajustes={out.get('ajustes_estoque')}"
            )
        )
