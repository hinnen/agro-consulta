from django.urls import path

from transferencias.api.views import SugestoesInteligentesAPIView

urlpatterns = [
    path(
        "sugestoes-inteligentes/",
        SugestoesInteligentesAPIView.as_view(),
        name="api-transferencias-sugestoes-inteligentes",
    ),
]
