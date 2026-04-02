from django.urls import path

from financeiro.api.views import GapEquilibrioAPIView, ResumoOperacionalAPIView

urlpatterns = [
    path(
        "resumo-operacional",
        ResumoOperacionalAPIView.as_view(),
        name="financeiro-resumo-operacional",
    ),
    path(
        "gap-equilibrio",
        GapEquilibrioAPIView.as_view(),
        name="financeiro-gap-equilibrio",
    ),
]
