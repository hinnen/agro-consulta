from django.urls import path
from . import views

urlpatterns = [
    # --- PÁGINAS ---
    path('', views.consulta_produtos, name='consulta_produtos'),
    path('historico/', views.historico_ajustes, name='historico_ajustes'),
    path('transferencias/', views.sugestao_transferencia, name='sugestao_transferencia'),
    path('ajuste-mobile/', views.ajuste_mobile_view, name='ajuste_mobile'), # <-- A rota que faltava

    # --- APIs ---
    path('api/login-mobile/', views.api_login_mobile, name='api_login_mobile'),
    path('api/buscar/', views.api_buscar_produtos, name='api_buscar_mobile'),
    path('api/ajustar/', views.api_ajustar_estoque, name='api_ajustar_estoque'),
    path('api/todos-produtos/', views.api_todos_produtos_local, name='api_todos_produtos_local'),
    path('api/autocomplete/', views.api_autocomplete_produtos, name='api_autocomplete_produtos'),
    path('api/buscar-clientes/', views.api_buscar_clientes, name='api_buscar_clientes'),
    path('api/listar-clientes/', views.api_list_customers, name='api_list_customers'),
    path('api/enviar-pedido-erp/', views.api_enviar_pedido_erp, name='api_enviar_pedido_erp'),
    path('api/buscar-produto-id/<str:id>/', views.api_buscar_produto_id, name='api_buscar_produto_id'),
]