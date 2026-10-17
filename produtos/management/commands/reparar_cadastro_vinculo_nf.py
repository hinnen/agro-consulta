"""Devolve nomes do cadastro que o vínculo NF trocou pelo texto da nota."""
from __future__ import annotations

from django.core.management.base import BaseCommand

from produtos.reparar_vinculo_nf_cadastro_util import (
    aplicar_reparo_vinculo_nf,
    planejar_reparo_vinculo_nf,
)


class Command(BaseCommand):
    help = "Devolve nome/marca apagados quando o vínculo da NF colou o xProd no cadastro."

    def add_arguments(self, parser):
        parser.add_argument("--aplicar", action="store_true", help="Grava. Sem isto, só lista.")
        parser.add_argument("--pid", type=str, default="", help="Um produto_externo_id só.")

    def handle(self, *args, **options):
        aplicar = bool(options.get("aplicar"))
        pid = str(options.get("pid") or "").strip()
        planos = planejar_reparo_vinculo_nf(pid=pid)
        if not planos:
            self.stdout.write("Nenhum produto com nome da NF (EAN entre colchetes) para devolver.")
            return
        for pl in planos:
            volta = pl.get("nome_volta") or "(só tira overlay)"
            self.stdout.write(
                f"{pl.get('codigo_nfe') or pl['pid']}: {pl.get('nome_agora')} → {volta}"
            )
        if not aplicar:
            self.stdout.write(self.style.WARNING(f"{len(planos)} produto(s). Sem --aplicar não grava."))
            return
        n = aplicar_reparo_vinculo_nf(planos)
        self.stdout.write(self.style.SUCCESS(f"Corrigidos: {n}"))
