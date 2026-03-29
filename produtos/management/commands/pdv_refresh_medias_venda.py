"""Pré-calcula o cache de médias de venda (30d) — agendar no cron às 00:00 se quiser aquecer antes do primeiro acesso."""

from django.core.cache import cache
from django.core.management.base import BaseCommand

from produtos.views import (
    _CACHE_MEDIAS_VENDA_ENTRY,
    _media_diaria_vendas_por_produto,
    obter_conexao_mongo,
)


class Command(BaseCommand):
    help = "Invalida e recalcula o mapa de médias de venda (30d) usado no PDV."

    def handle(self, *args, **options):
        cache.delete(_CACHE_MEDIAS_VENDA_ENTRY)
        _, db = obter_conexao_mongo()
        if db is None:
            self.stderr.write(self.style.ERROR("Mongo indisponível"))
            return
        m = _media_diaria_vendas_por_produto(db, dias=30)
        from django.utils import timezone

        hoje = timezone.localdate().isoformat()
        cache.set(
            _CACHE_MEDIAS_VENDA_ENTRY,
            {"day": hoje, "map": m},
            timeout=86400 * 2,
        )
        self.stdout.write(self.style.SUCCESS(f"Médias recalculadas: {len(m)} produtos (dia {hoje})."))
