from django.core.cache import cache
from django.db.models.signals import post_delete, post_save
from django.dispatch import receiver

from produtos.models import ClienteAgro

# Mesma chave que produtos.views.API_LIST_CUSTOMERS_CACHE_KEY (evita import circular).
_LISTA_CLIENTES_PDV_CACHE = "api_list_customers_v1"


@receiver([post_save, post_delete], sender=ClienteAgro)
def _invalidar_cache_lista_clientes_pdv(sender, **kwargs):
    cache.delete(_LISTA_CLIENTES_PDV_CACHE)
