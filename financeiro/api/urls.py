from django.urls import path

from financeiro.api.views import (
    DebugMongoResumoAPIView,
    GapEquilibrioAPIView,
    ResumoOperacionalAPIView,
    SaldoDiarioMesAPIView,
)

urlpatterns = [
    path(
        "debug-mongo-resumo",
        DebugMongoResumoAPIView.as_view(),
        name="financeiro-debug-mongo-resumo",
    ),
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
    path(
        "saldo-diario-mes",
        SaldoDiarioMesAPIView.as_view(),
        name="financeiro-saldo-diario-mes",
    ),
]
