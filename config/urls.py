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
    path('estoque/api_atualizar_pin/', estoque_views.api_atualizar_pin, name='api_atualizar_pin'),
    path('estoque/api_listar_usuarios/', estoque_views.api_listar_usuarios, name='api_listar_usuarios'),
    path('estoque/api_atualizar_medias/', estoque_views.api_atualizar_medias, name='api_atualizar_medias'),
    path('estoque/api_registrar_impressao/', estoque_views.api_registrar_impressao, name='api_registrar_impressao'),
    path('estoque/api_cancelar_separacao/<str:id>/', estoque_views.api_cancelar_separacao, name='api_cancelar_separacao'),
]