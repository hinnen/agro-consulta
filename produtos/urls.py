from django.urls import path
from . import views

urlpatterns = [
    path('', views.consulta_produtos, name='consulta_produtos'),
    path('historico/', views.historico_ajustes, name='historico_ajustes'),
    path('transferencias/', views.sugestao_transferencia, name='sugestao_transferencia'),
    
    # Rota da API de Busca
    path('api/buscar/', views.api_buscar_produtos, name='api_buscar_produtos'),
    path('api/ajustar-estoque/', views.api_ajustar_estoque, name='api_ajustar_estoque'),
]