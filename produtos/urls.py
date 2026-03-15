from django.urls import path
from .views import consulta_produtos, api_buscar_produtos

urlpatterns = [
    path('consulta/', consulta_produtos, name='consulta_produtos'),
    path('api/buscar/', api_buscar_produtos, name='api_buscar_produtos'),
]