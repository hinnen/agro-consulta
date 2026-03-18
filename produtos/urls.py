from django.urls import path

from .views import (
    consulta_produtos,
    api_buscar_produtos,
    api_sugestoes_produtos,
    api_ajustar_estoque,
)

urlpatterns = [
    path("consulta/", consulta_produtos, name="consulta_produtos"),
    path("api/buscar/", api_buscar_produtos, name="api_buscar_produtos"),
    path("api/sugestoes/", api_sugestoes_produtos, name="api_sugestoes_produtos"),
    path("api/ajustar/", api_ajustar_estoque, name="api_ajustar_estoque"),
]