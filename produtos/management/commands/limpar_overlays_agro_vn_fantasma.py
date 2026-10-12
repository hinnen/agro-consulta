"""Funde overlays agro:vn duplicados no cadastro real (mesmo nome + GM no Postgres)."""
from __future__ import annotations

from django.core.management.base import BaseCommand

from produtos.cadastro_overlay_orfao_util import limpar_todos_fantasmas_agro_vn_com_canonico


class Command(BaseCommand):
    help = (
        "Remove cadastros fantasma agro:vn: quando já existe produto real com o mesmo nome. "
        "Migra lotes/validade para o overlay do produto GM e apaga linha fantasma no Postgres."
    )

    def handle(self, *args, **options):
        stats = limpar_todos_fantasmas_agro_vn_com_canonico()
        self.stdout.write(
            self.style.SUCCESS(
                f"Fundidos: {stats.get('fundidos', 0)} | "
                f"agro:vn sem par real (mantidos): {stats.get('sem_canon', 0)}"
            )
        )
