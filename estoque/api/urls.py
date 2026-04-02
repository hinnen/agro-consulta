from django.urls import path

from estoque.api.views import IndicadoresReposicaoAPIView

urlpatterns = [
    path(
        "reposicao/",
        IndicadoresReposicaoAPIView.as_view(),
        name="api-indicadores-reposicao",
    ),
]
