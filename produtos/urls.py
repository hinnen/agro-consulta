from django.urls import path
from .views import consulta_produtos

urlpatterns = [
    path('consulta/', consulta_produtos, name='consulta_produtos'),
]