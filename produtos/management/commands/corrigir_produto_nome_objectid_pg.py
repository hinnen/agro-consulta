"""Corrige produtos Postgres com ``nome`` = ObjectId Mongo (fantasma de importação)."""
from __future__ import annotations

from django.core.management.base import BaseCommand

from produtos.catalogo_nome_util import (
    nome_parece_objectid_corrupto,
    queryset_produtos_nome_corrupto,
    reparar_produto_nome_corrupto_persist,
)
from produtos.models import Produto


class Command(BaseCommand):
    help = "Repara nome/marca/GM de produtos cujo nome ficou como Id Mongo (24 hex)."

    def add_arguments(self, parser):
        parser.add_argument("--dry-run", action="store_true", help="Só lista o que corrigiria.")
        parser.add_argument("--pid", type=str, default="", help="Corrigir um produto_externo_id só.")

    def handle(self, *args, **options):
        dry = bool(options.get("dry_run"))
        pid_f = str(options.get("pid") or "").strip()
        qs = Produto.objects.all()
        if pid_f:
            qs = qs.filter(produto_externo_id=pid_f)
        else:
            qs = queryset_produtos_nome_corrupto(qs)

        total = 0
        ok = 0
        falha = 0
        for p in qs.iterator(chunk_size=100):
            pid = (p.produto_externo_id or "").strip()
            if not nome_parece_objectid_corrupto(p.nome or "", pid):
                continue
            total += 1
            patch = reparar_produto_nome_corrupto_persist(p, dry_run=dry)
            if patch:
                ok += 1
                self.stdout.write(
                    f"{'[dry] ' if dry else ''}OK {pid} → {patch.get('nome')} · {patch.get('codigo_nfe')}"
                )
            else:
                falha += 1
                self.stdout.write(self.style.WARNING(f"Sem correção para {pid} (pv={p.preco_venda})"))

        self.stdout.write(
            self.style.SUCCESS(
                f"Concluído — candidatos={total} corrigidos={ok} sem_patch={falha}{' (dry-run)' if dry else ''}"
            )
        )
