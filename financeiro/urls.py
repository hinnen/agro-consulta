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
    path(
        "interno/planos-despesa-simulacao-unificar/",
        views.simulacao_unificar_planos_despesa,
        name="financeiro_simulacao_unificar_planos",
    ),
    path(
        "interno/planos-despesa-aplicar-unificar/",
        views.aplicar_unificar_planos_despesa,
        name="financeiro_aplicar_unificar_planos",
    ),
]
