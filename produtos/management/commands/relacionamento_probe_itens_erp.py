"""Diagnóstico Mongo — DtoVendaProduto vs itens embutidos (FL-042)."""
from __future__ import annotations

import json

from django.core.management.base import BaseCommand

from produtos.relacionamento_historico_erp_util import probe_itens_venda_mongo


class Command(BaseCommand):
    help = "Probe: amostra DtoVenda e contagem de itens (Mongo separado vs embutido)."

    def add_arguments(self, parser):
        parser.add_argument(
            "--limite",
            type=int,
            default=3,
            help="Cabeçalhos DtoVenda na amostra (default: 3)",
        )

    def handle(self, *args, **options):
        r = probe_itens_venda_mongo(limite=int(options.get("limite") or 3))
        if not r.get("ok"):
            self.stderr.write(self.style.ERROR(r.get("erro") or "Falha"))
            return
        self.stdout.write(json.dumps(r, ensure_ascii=False, indent=2))
