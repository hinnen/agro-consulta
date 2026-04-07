from django.contrib import admin
from django.http import HttpResponse
from django.urls import path, include
from estoque import views as estoque_views


def healthz(_request):
    """Resposta mínima para health check do Render (evita GET / pesado no cold start)."""
    return HttpResponse("ok", content_type="text/plain; charset=utf-8")


urlpatterns = [
    path("healthz", healthz, name="healthz"),
    path("api/financeiro/", include("financeiro.api.urls")),
    path("api/indicadores/", include("estoque.api.urls")),
    path("api/transferencias/", include("transferencias.api.urls")),
    path("pdv/", include("pdv.urls")),
    # O include já vai tratar a rota vazia se ela estiver no produtos/urls.py
    path('', include('produtos.urls')), 
    path('admin/', admin.site.urls),
    path('estoque/api_sugestoes_transferencia/', estoque_views.api_sugestoes_transferencia, name='api_sugestoes_transferencia'),
    path('estoque/api_salvar_config_transferencia/', estoque_views.api_salvar_config_transferencia, name='api_salvar_config_transferencia'),
    path('estoque/api_importar_planilha/', estoque_views.api_importar_planilha_transferencia, name='api_importar_planilha'),
    path('estoque/api_atualizar_pin/', estoque_views.api_atualizar_pin, name='api_atualizar_pin'),
    path('estoque/api_listar_usuarios/', estoque_views.api_listar_usuarios, name='api_listar_usuarios'),
    path('estoque/api_definir_pin_rh/', estoque_views.api_definir_pin_rh, name='api_definir_pin_rh'),
    path('estoque/api_atualizar_medias/', estoque_views.api_atualizar_medias, name='api_atualizar_medias'),
    path('estoque/api_registrar_impressao/', estoque_views.api_registrar_impressao, name='api_registrar_impressao'),
    path('estoque/api_cancelar_separacao/<str:id>/', estoque_views.api_cancelar_separacao, name='api_cancelar_separacao'),
    path(
        'api/estoque/sync-health/',
        estoque_views.api_estoque_sync_health,
        name='api_estoque_sync_health',
    ),
    path(
        'api/estoque/divergencia-ajustes/',
        estoque_views.api_estoque_divergencia_ajustes,
        name='api_estoque_divergencia_ajustes',
    ),
]