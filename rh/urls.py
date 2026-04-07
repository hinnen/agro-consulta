from django.urls import path
from django.views.generic import RedirectView

from rh import views

urlpatterns = [
    path("", views.rh_painel, name="rh_painel"),
    path("operadores/", views.rh_operadores_pins, name="rh_operadores_pins"),
    path("gestao-funcionarios/", views.rh_gestao_funcionarios, name="rh_gestao_funcionarios"),
    path(
        "ferramentas-restritas/",
        RedirectView.as_view(pattern_name="rh_gestao_funcionarios", permanent=False),
    ),
    path("gestao/funcionarios/", views.rh_funcionarios_lista, name="rh_funcionarios_lista"),
    path("gestao/funcionarios/novo/", views.rh_funcionario_novo, name="rh_funcionario_novo"),
    path("gestao/funcionarios/<int:pk>/editar/", views.rh_funcionario_editar, name="rh_funcionario_editar"),
    path(
        "gestao/funcionarios/<int:pk>/salario/",
        views.rh_funcionario_salario_adicionar,
        name="rh_funcionario_salario_adicionar",
    ),
    path(
        "gestao/funcionarios/<int:pk>/vale/",
        views.rh_funcionario_vale_manual,
        name="rh_funcionario_vale_manual",
    ),
    path(
        "gestao/funcionarios/<int:pk>/vale/<int:vale_id>/cancelar/",
        views.rh_funcionario_vale_cancelar,
        name="rh_funcionario_vale_cancelar",
    ),
    path(
        "gestao/funcionarios/<int:pk>/garantir-fechamento-mes/",
        views.rh_funcionario_garantir_fechamento_mes_atual,
        name="rh_funcionario_garantir_fechamento_mes_atual",
    ),
    path("gestao/funcionarios/<int:pk>/", views.rh_funcionario_ficha, name="rh_funcionario_ficha"),
    path("gestao/fechamentos/", views.rh_fechamentos_lista, name="rh_fechamentos_lista"),
    path(
        "gestao/fechamentos/<int:pk>/salvar/",
        views.rh_fechamento_salvar,
        name="rh_fechamento_salvar",
    ),
    path(
        "gestao/fechamentos/<int:pk>/fechar/",
        views.rh_fechamento_fechar,
        name="rh_fechamento_fechar",
    ),
    path(
        "gestao/fechamentos/<int:pk>/marcar-pago/",
        views.rh_fechamento_marcar_pago,
        name="rh_fechamento_marcar_pago",
    ),
    path(
        "gestao/fechamentos/<int:pk>/reabrir/",
        views.rh_fechamento_reabrir,
        name="rh_fechamento_reabrir",
    ),
    path(
        "gestao/fechamentos/<int:pk>/excluir/",
        views.rh_fechamento_excluir,
        name="rh_fechamento_excluir",
    ),
    path(
        "gestao/fechamentos/<int:pk>/titulo-financeiro/",
        views.rh_fechamento_titulo_financeiro,
        name="rh_fechamento_titulo_financeiro",
    ),
    path("gestao/fechamentos/<int:pk>/", views.rh_fechamento_detalhe, name="rh_fechamento_detalhe"),
    path("gestao/inconsistencias/", views.rh_inconsistencias_lista, name="rh_inconsistencias_lista"),
    path(
        "gestao/inconsistencias/<int:pk>/resolver/",
        views.rh_inconsistencia_resolver,
        name="rh_inconsistencia_resolver",
    ),
    path("gestao/relatorios/", views.rh_relatorios, name="rh_relatorios"),
    path("gestao/importar-vales/", views.rh_importar_vales, name="rh_importar_vales"),
    path("gestao/conferencia-tecnica/", views.rh_conferencia_tecnica, name="rh_conferencia_tecnica"),
    path(
        "gestao/funcionarios/<int:pk>/reconciliar-cliente/",
        views.rh_funcionario_reconciliar_cliente,
        name="rh_funcionario_reconciliar_cliente",
    ),
    path("funcionarios/criar/", views.api_rh_funcionario_create, name="api_rh_funcionario_create"),
    path("funcionarios/<int:pk>/vale-api/", views.api_rh_funcionario_vale, name="api_rh_funcionario_vale"),
    path("funcionarios/<int:pk>/", views.api_rh_funcionario_detail, name="api_rh_funcionario_detail"),
    path("funcionarios/", views.api_rh_funcionarios, name="api_rh_funcionarios"),
    path("fechamentos/recalcular/", views.api_rh_fechamentos_recalcular, name="api_rh_fechamentos_recalcular"),
    path("fechamentos/<int:pk>/fechar-api/", views.api_rh_fechamento_fechar, name="api_rh_fechamento_fechar"),
    path(
        "fechamentos/<int:pk>/marcar-pago-api/",
        views.api_rh_fechamento_marcar_pago,
        name="api_rh_fechamento_marcar_pago",
    ),
    path("fechamentos/", views.api_rh_fechamentos, name="api_rh_fechamentos"),
    path(
        "integracoes/importar-vales-caixa/",
        views.api_rh_importar_vales_caixa,
        name="api_rh_importar_vales_caixa",
    ),
    path("caixa/quem-leva/", views.api_rh_caixa_quem_leva, name="api_rh_caixa_quem_leva"),
]
