from django.contrib import admin
from django.urls import path, include
from estoque import views as estoque_views

urlpatterns = [
    # O include já vai tratar a rota vazia se ela estiver no produtos/urls.py
    path('', include('produtos.urls')), 
    path('admin/', admin.site.urls),
    path('estoque/api_sugestoes_transferencia/', estoque_views.api_sugestoes_transferencia, name='api_sugestoes_transferencia'),
    path('estoque/api_salvar_config_transferencia/', estoque_views.api_salvar_config_transferencia, name='api_salvar_config_transferencia'),
    path('estoque/api_importar_planilha/', estoque_views.api_importar_planilha_transferencia, name='api_importar_planilha'),
]