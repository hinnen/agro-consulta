from django.urls import path

from financeiro import views

urlpatterns = [
    path("grafico-gastos/", views.grafico_gastos_view, name="grafico_gastos"),
    path(
        "api/dados-grafico-gastos/",
        views.api_dados_grafico_gastos,
        name="api_dados_grafico_gastos",
    ),
    path(
        "api/grafico-gastos-atalhos/",
        views.api_grafico_gastos_atalhos,
        name="api_grafico_gastos_atalhos",
    ),
    path(
        "api/grafico-gastos-atalhos/<int:slot>/",
        views.api_grafico_gastos_atalho_salvar,
        name="api_grafico_gastos_atalho_salvar",
    ),
    path(
        "api/grafico-gastos-atalhos/<int:slot>/padrao/",
        views.api_grafico_gastos_atalho_padrao,
        name="api_grafico_gastos_atalho_padrao",
    ),
    path(
        "interno/planos-despesa-classificacao/",
        views.classificacao_despesas_lista,
        name="financeiro_classificacao_despesas",
    ),
]
