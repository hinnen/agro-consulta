"""Recalcula ``busca_palavras_agro`` em todos os overlays (apelidos já salvos antes do índice)."""
from __future__ import annotations

from django.core.management.base import BaseCommand

from produtos.medicamento_vet_taxonomia import (
    aplicar_palavras_e_classes_em_extras,
    ler_busca_de_cadastro_extras,
)
from produtos.models import ProdutoGestaoOverlayAgro


class Command(BaseCommand):
    help = "Reindexa palavras-chave do overlay para busca PDV/cadastro (Postgres JSON)."

    def add_arguments(self, parser):
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Só conta quantos overlays seriam atualizados.",
        )
        parser.add_argument("--limit", type=int, default=0, help="Máximo de linhas (0 = todas).")

    def handle(self, *args, **options):
        dry = bool(options.get("dry_run"))
        lim = int(options.get("limit") or 0)
        qs = ProdutoGestaoOverlayAgro.objects.all().order_by("pk")
        if lim > 0:
            qs = qs[:lim]
        atualizados = 0
        for ov in qs.iterator(chunk_size=200):
            ex = ov.cadastro_extras if isinstance(ov.cadastro_extras, dict) else {}
            pk, cv, ev = ler_busca_de_cadastro_extras(ex)
            if not pk and not cv and not ev:
                continue
            novo = dict(ex)
            aplicar_palavras_e_classes_em_extras(
                novo,
                palavras_chave=pk,
                classes_vet=cv,
                especies_vet=ev,
                payload_tem_palavras=bool(pk),
                payload_tem_classes=bool(cv),
                payload_tem_especies=bool(ev),
            )
            if novo == ex:
                continue
            atualizados += 1
            if not dry:
                ov.cadastro_extras = novo
                ov.save(update_fields=["cadastro_extras"])
        self.stdout.write(
            self.style.SUCCESS(
                f"backfill_busca_palavras_overlay: {atualizados} overlay(s)"
                + (" (dry-run)" if dry else " gravado(s)")
            )
        )
