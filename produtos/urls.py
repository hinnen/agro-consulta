from django.urls import path
from . import views

urlpatterns = [
    path('', views.consulta_produtos, name='consulta_produtos'),
    path('historico-ajustes/', views.historico_ajustes, name='historico_ajustes'),
    path('transferencias/', views.sugestao_transferencia, name='sugestao_transferencia'),
    path('api/sugestoes/', views.api_sugestoes_produtos, name='api_sugestoes_produtos'),
    path('api/buscar/', views.api_buscar_produtos, name='api_buscar_produtos'),
    path('api/ajustar-estoque/', views.api_ajustar_estoque, name='api_ajustar_estoque'),
    path('api/deletar-ajuste/<int:ajuste_id>/', views.api_deletar_ajuste, name='api_deletar_ajuste'),
    path('api/limpar-historico/', views.api_limpar_historico, name='api_limpar_historico'),
    path('api/gerar-orcamento/', views.api_gerar_orcamento, name='api_gerar_orcamento'),
    path('api/config-logistica/', views.api_salvar_config_logistica, name='api_salvar_config_logistica'), # NOVA ROTA
]