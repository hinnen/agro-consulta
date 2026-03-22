from django.urls import path
from . import views

urlpatterns = [
    path('', views.consulta_produtos, name='consulta_produtos'),
    path('historico/', views.historico_ajustes, name='historico_ajustes'),
    path('transferencias/', views.sugestao_transferencia, name='sugestao_transferencia'),
    # APIs
    path('api/buscar/', views.api_buscar_produtos, name='api_buscar_produtos'),
    path('api/clientes/', views.api_buscar_clientes, name='api_buscar_clientes'),
    path('api/ajustar/', views.api_ajustar_estoque, name='api_ajustar_estoque'),
    path('api/autocomplete/', views.api_autocomplete_produtos, name='api_autocomplete_produtos'),
]