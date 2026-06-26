from django.urls import path

from financeiro import views

urlpatterns = [
    path("grafico-gastos/", views.grafico_gastos_view, name="grafico_gastos"),
    path(
        "api/dados-grafico-gastos/",
        views.api_dados_grafico_gastos,
        name="api_dados_grafico_gastos",
    ),
]
