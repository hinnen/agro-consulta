"""Reatribui manualmente um código 230…; dry-run é o padrão."""
from __future__ import annotations

from django.core.management.base import BaseCommand, CommandError
from django.db.models import Q

from produtos.cb_loja_reatribuir_util import reatribuir_cb_loja_exclusivo


class Command(BaseCommand):
    help = "Gera e atribui EAN-13 230… exclusivo a um único produto Postgres."

    def add_arguments(self, parser):
        parser.add_argument("--codigo-gm", required=True, help="Código exato, ex.: GM4045.")
        parser.add_argument(
            "--esperado-atual",
            required=True,
            help="Código atual esperado; trava contra alteração concorrente.",
        )
        parser.add_argument(
            "--aplicar",
            action="store_true",
            help="Grava a alteração. Sem esta opção, executa somente dry-run.",
        )
        parser.add_argument(
            "--confirmar",
            default="",
            help="Ao aplicar, deve repetir exatamente o valor de --codigo-gm.",
        )

    def handle(self, *args, **options):
        from produtos.agro_fonte_config import (
            agro_catalogo_usa_postgres,
            agro_mongo_erp_desligado,
        )
        from produtos.models import Produto

        codigo_gm = str(options["codigo_gm"] or "").strip()
        esperado = str(options["esperado_atual"] or "").strip()
        aplicar = bool(options["aplicar"])
        if aplicar and str(options.get("confirmar") or "").strip() != codigo_gm:
            raise CommandError("Para aplicar, informe --confirmar igual a --codigo-gm.")
        if aplicar and not (agro_mongo_erp_desligado() or agro_catalogo_usa_postgres()):
            raise CommandError(
                "Aplicação bloqueada: este comando altera o catálogo Postgres e exige modo agro_pg/Mongo desligado."
            )

        qs = Produto.objects.filter(
            Q(codigo_nfe__iexact=codigo_gm) | Q(codigo_interno__iexact=codigo_gm)
        ).order_by("pk")
        produtos = list(qs[:2])
        if not produtos:
            raise CommandError(f"Produto não encontrado: {codigo_gm}")
        if len(produtos) != 1:
            raise CommandError(f"Código ambíguo; mais de um produto encontrado: {codigo_gm}")

        resumo = reatribuir_cb_loja_exclusivo(
            produtos[0],
            esperado_atual=esperado,
            dry_run=not aplicar,
        )
        modo = "APLICADO" if aplicar else "DRY-RUN"
        self.stdout.write(
            self.style.SUCCESS(
                f"{modo}: {codigo_gm} · {resumo['codigo_anterior']} -> {resumo['codigo_novo']}"
            )
        )
        if not aplicar:
            self.stdout.write(
                f"Para gravar: --aplicar --confirmar {codigo_gm}"
            )
