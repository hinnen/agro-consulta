from django.urls import include, path

from financeiro.views import (
    api_dashboard_financeiro_restrito,
    dashboard_financeiro_completo,
    dashboard_financeiro_restrito,
)

from . import promocoes_views, views, views_mp_point, views_nfce, pg_backup_views
from . import fiado_gestao_views as fiado_views
from . import relatorios_central_views as relatorios_views
from . import views_catalogo_delivery
from . import views_dispenser_a6
from . import views_uso_loja
from . import views_pdv_transf_loja
from . import views_pdv_chat_loja
from . import views_atendimento_whatsapp
from . import views_pdv_topbar
from . import views_repasse_vila
from . import views_tabela_preco_forma
from . import views_cliente_cadastro
from . import bug_report_views
from . import ajuste_codigo_pendente_views
from . import views_planos_conta
from . import contagem_ciclica_views

urlpatterns = [
    # --- PÁGINAS ---
    path("", views.dashboard_gerencial_view, name="home"),
    path("atalhos/", views.home, name="home_atalhos"),
    path(
        "atendimento-whatsapp/",
        views_atendimento_whatsapp.atendimento_whatsapp_view,
        name="atendimento_whatsapp",
    ),
    path(
        "atendimento-whatsapp/celular/manifest.webmanifest",
        views_atendimento_whatsapp.atendimento_whatsapp_celular_manifest,
        name="atendimento_whatsapp_celular_manifest",
    ),
    path(
        "atendimento-whatsapp/celular/sw.js",
        views_atendimento_whatsapp.atendimento_whatsapp_celular_sw,
        name="atendimento_whatsapp_celular_sw",
    ),
    path(
        "atendimento-whatsapp/celular/",
        views_atendimento_whatsapp.atendimento_whatsapp_celular_view,
        name="atendimento_whatsapp_celular",
    ),
    path(
        "atendimento-whatsapp/bot/",
        views_atendimento_whatsapp.atendimento_whatsapp_bot_view,
        name="atendimento_whatsapp_bot",
    ),
    path("consulta/", views.consulta_produtos, name="consulta_produtos"),
    path("gestao/bugs/", bug_report_views.bug_reports_lista_view, name="bug_reports_lista"),
    path("gestao/bugs/<int:pk>/", bug_report_views.bug_report_detalhe_view, name="bug_report_detalhe"),
    path("gestao/bugs/<int:pk>/print/", bug_report_views.bug_report_print_view, name="bug_report_print"),
    path(
        "gestao/codigos-pendentes-ajuste/",
        ajuste_codigo_pendente_views.ajuste_codigos_pendentes_lista_view,
        name="ajuste_codigos_pendentes_lista",
    ),
    path(
        "api/ajuste-mobile/codigo-pendente/",
        ajuste_codigo_pendente_views.api_ajuste_codigo_pendente_criar,
        name="api_ajuste_codigo_pendente_criar",
    ),
    path(
        "api/ajuste-mobile/codigo-pendente/<int:pk>/status/",
        ajuste_codigo_pendente_views.api_ajuste_codigo_pendente_status,
        name="api_ajuste_codigo_pendente_status",
    ),
    path(
        "api/ajuste-mobile/ciclica/sessoes/",
        contagem_ciclica_views.api_ciclica_sessoes,
        name="api_ciclica_sessoes",
    ),
    path(
        "api/ajuste-mobile/ciclica/categorias/",
        contagem_ciclica_views.api_ciclica_categorias,
        name="api_ciclica_categorias",
    ),
    path(
        "api/ajuste-mobile/ciclica/abrir/",
        contagem_ciclica_views.api_ciclica_abrir,
        name="api_ciclica_abrir",
    ),
    path(
        "api/ajuste-mobile/ciclica/<int:pk>/",
        contagem_ciclica_views.api_ciclica_detalhe,
        name="api_ciclica_detalhe",
    ),
    path(
        "api/ajuste-mobile/ciclica/<int:pk>/entrar/",
        contagem_ciclica_views.api_ciclica_entrar,
        name="api_ciclica_entrar",
    ),
    path(
        "api/ajuste-mobile/ciclica/<int:pk>/contar/",
        contagem_ciclica_views.api_ciclica_contar,
        name="api_ciclica_contar",
    ),
    path(
        "api/ajuste-mobile/ciclica/<int:pk>/fechar-pass1/",
        contagem_ciclica_views.api_ciclica_fechar_pass1,
        name="api_ciclica_fechar_pass1",
    ),
    path(
        "api/ajuste-mobile/ciclica/<int:pk>/gravar/",
        contagem_ciclica_views.api_ciclica_gravar,
        name="api_ciclica_gravar",
    ),
    path(
        "api/ajuste-mobile/ciclica/<int:pk>/cancelar/",
        contagem_ciclica_views.api_ciclica_cancelar,
        name="api_ciclica_cancelar",
    ),
    path("api/bug-report/", bug_report_views.api_bug_report_criar, name="api_bug_report_criar"),
    path("api/bug-report/<int:pk>/status/", bug_report_views.api_bug_report_status, name="api_bug_report_status"),
    path('historico/', views.historico_ajustes, name='historico_ajustes'),
    path('transferencias/', views.sugestao_transferencia, name='sugestao_transferencia'),
    path('entregas/', views.entregas_painel_view, name='entregas_painel'),
    path('entregas/api/registrar/', views.api_entrega_registrar, name='api_entrega_registrar'),
    path('entregas/api/listar/', views.api_entregas_listar, name='api_entregas_listar'),
    path('entregas/api/atualizar/', views.api_entrega_atualizar, name='api_entrega_atualizar'),
    path("catalogo/", views_catalogo_delivery.catalogo_delivery_view, name="catalogo_delivery"),
    path("catalogo/og-image/", views_catalogo_delivery.catalogo_og_image_view, name="catalogo_og_image"),
    path("catalogo/pedido-ok/", views_catalogo_delivery.catalogo_pedido_ok_view, name="catalogo_pedido_ok"),
    path("catalogo/gestao/", views_catalogo_delivery.catalogo_gestao_view, name="catalogo_gestao"),
    path("catalogo/api/pedido/", views_catalogo_delivery.api_catalogo_pedido, name="api_catalogo_pedido"),
    path(
        "catalogo/api/localizacao/",
        views_catalogo_delivery.api_catalogo_localizacao,
        name="api_catalogo_localizacao",
    ),
    path("catalogo/api/cliente/", views_catalogo_delivery.api_catalogo_cliente, name="api_catalogo_cliente"),
    path("catalogo/api/saldo/", views_catalogo_delivery.api_catalogo_saldo_produto, name="api_catalogo_saldo"),
    path("catalogo/api/categorias/", views_catalogo_delivery.api_catalogo_categorias, name="api_catalogo_categorias"),
    path(
        "catalogo/api/categorias/criar/",
        views_catalogo_delivery.api_catalogo_categoria_criar,
        name="api_catalogo_categoria_criar",
    ),
    path(
        "catalogo/api/categorias/excluir/",
        views_catalogo_delivery.api_catalogo_categoria_excluir,
        name="api_catalogo_categoria_excluir",
    ),
    path(
        "catalogo/api/categorias/foto/",
        views_catalogo_delivery.api_catalogo_categoria_foto,
        name="api_catalogo_categoria_foto",
    ),
    path(
        "catalogo/cat-img/<int:pk>/",
        views_catalogo_delivery.catalogo_categoria_imagem_view,
        name="catalogo_categoria_imagem",
    ),
    path(
        'api/pdv/cliente-credito-fiado/',
        views.api_pdv_cliente_credito_fiado,
        name='api_pdv_cliente_credito_fiado',
    ),
    path(
        'api/pdv/relacionamento-cliente/',
        views.api_pdv_relacionamento_cliente,
        name='api_pdv_relacionamento_cliente',
    ),
    path(
        'api/pdv/relacionamento-cliente/extras/',
        views.api_pdv_relacionamento_cliente_extras,
        name='api_pdv_relacionamento_cliente_extras',
    ),
    path(
        'api/pdv/entregas-pendentes/',
        views.api_pdv_entregas_pendentes,
        name='api_pdv_entregas_pendentes',
    ),
    path(
        'api/pdv/entrega-pendente/<int:pk>/',
        views.api_pdv_entrega_pendente_detalhe,
        name='api_pdv_entrega_pendente_detalhe',
    ),
    path(
        'api/pdv/entrega-pendente/<int:pk>/assumir/',
        views.api_pdv_entrega_pendente_assumir,
        name='api_pdv_entrega_pendente_assumir',
    ),
    path(
        'api/pdv/entrega-pendente/<int:pk>/finalizar/',
        views.api_pdv_entrega_pendente_finalizar,
        name='api_pdv_entrega_pendente_finalizar',
    ),
    path(
        'api/pdv/entrega-pendente/<int:pk>/cancelar/',
        views.api_pdv_entrega_pendente_cancelar,
        name='api_pdv_entrega_pendente_cancelar',
    ),
    path(
        'entregas/api/ordenar-rota/',
        views.api_entregas_ordenar_rota,
        name='api_entregas_ordenar_rota',
    ),
    path('ajuste-mobile/manifest.webmanifest', views.ajuste_mobile_manifest, name='ajuste_mobile_manifest'),
    path('ajuste-mobile/sw.js', views.ajuste_mobile_sw, name='ajuste_mobile_sw'),
    path('ajuste-mobile/', views.ajuste_mobile_view, name='ajuste_mobile'), # <-- A rota que faltava
    path('compras/', views.compras_view, name='compras_view'),
    path('promocoes/', promocoes_views.promocoes_lista_view, name='promocoes_lista'),
    path('promocoes/nova/', promocoes_views.promocoes_nova_view, name='promocoes_nova'),
    path('promocoes/<int:pk>/editar/', promocoes_views.promocoes_editar_view, name='promocoes_editar'),
    path(
        'produtos/tabelas-preco-forma/',
        views_tabela_preco_forma.tabelas_preco_forma_view,
        name='tabelas_preco_forma',
    ),
    path(
        'api/tabelas-preco-forma/',
        views_tabela_preco_forma.api_tabelas_preco_forma_estado,
        name='api_tabelas_preco_forma_estado',
    ),
    path(
        'api/tabelas-preco-forma/salvar/',
        views_tabela_preco_forma.api_tabelas_preco_forma_salvar,
        name='api_tabelas_preco_forma_salvar',
    ),
    path(
        'api/tabelas-preco-forma/resolucoes/',
        views_tabela_preco_forma.api_tabelas_preco_forma_resolucoes,
        name='api_tabelas_preco_forma_resolucoes',
    ),
    path(
        'api/tabelas-preco-forma/pdv/',
        views_tabela_preco_forma.api_tabelas_preco_forma_pdv,
        name='api_tabelas_preco_forma_pdv',
    ),
    path(
        'api/tabelas-preco-forma/buscar-produto/',
        views_tabela_preco_forma.api_tabelas_preco_forma_buscar_produto,
        name='api_tabelas_preco_forma_buscar_produto',
    ),
    path('api/promocoes/salvar/', promocoes_views.api_promocoes_salvar, name='api_promocoes_salvar'),
    path('api/promocoes/<int:pk>/excluir/', promocoes_views.api_promocoes_excluir, name='api_promocoes_excluir'),
    path('api/promocoes/buscar-produto/', promocoes_views.api_promocoes_buscar_produto, name='api_promocoes_buscar_produto'),
    path('api/promocoes/ativas-pdv/', promocoes_views.api_promocoes_ativas_pdv, name='api_promocoes_ativas_pdv'),
    path('compras/relatorio-a4/', views.compras_relatorio_a4_view, name='compras_relatorio_a4'),
    path(
        'compras/relatorio-planilha-categoria/',
        views.compras_relatorio_planilha_categoria_view,
        name='compras_relatorio_planilha_categoria',
    ),
    path(
        'compras/relatorio-planilha-unidade/',
        views.compras_relatorio_planilha_unidade_view,
        name='compras_relatorio_planilha_unidade',
    ),
    path(
        'compras/relatorio-planilha-fornecedor/',
        views.compras_relatorio_planilha_fornecedor_view,
        name='compras_relatorio_planilha_fornecedor',
    ),
    path(
        'compras/relatorio-saldo/',
        views.compras_relatorio_saldo_view,
        name='compras_relatorio_saldo',
    ),
    path(
        'api/compras/relatorio-saldo/',
        views.api_compras_relatorio_saldo,
        name='api_compras_relatorio_saldo',
    ),
    path(
        'api/compras/folha-saldo-presets/',
        views.api_compras_folha_saldo_presets,
        name='api_compras_folha_saldo_presets',
    ),
    path(
        'api/compras/folha-saldo-presets/<int:pk>/',
        views.api_compras_folha_saldo_preset_detail,
        name='api_compras_folha_saldo_preset_detail',
    ),
    path(
        'api/compras/relatorio-fornecedor/',
        views.api_compras_relatorio_fornecedor,
        name='api_compras_relatorio_fornecedor',
    ),
    path(
        'api/compras/relatorio-dim/',
        views.api_compras_relatorio_dim_sugestao,
        name='api_compras_relatorio_dim_sugestao',
    ),
    path(
        'api/compras/relatorio-categoria/',
        views.api_compras_relatorio_categoria,
        name='api_compras_relatorio_categoria',
    ),
    path(
        'api/compras/relatorio-unidade/',
        views.api_compras_relatorio_unidade,
        name='api_compras_relatorio_unidade',
    ),
    path(
        'produtos/cadastro-erp/produto/<str:produto_id>/',
        views.produtos_cadastro_erp_produto_view,
        name='produtos_cadastro_erp_produto',
    ),
    path(
        'produtos/cadastro-erp/',
        views.produtos_cadastro_erp_view,
        name='produtos_cadastro_erp',
    ),
    path('produtos/gestao/', views.produtos_gestao_view, name='produtos_gestao'),
    path('produtos/etiquetas/', views.produtos_etiquetas_view, name='produtos_etiquetas'),
    path(
        'produtos/etiquetas/lote/',
        views.produtos_etiquetas_lote_view,
        name='produtos_etiquetas_lote',
    ),
    path(
        'api/produtos/etiquetas/presets/',
        views.api_etiquetas_presets,
        name='api_etiquetas_presets',
    ),
    path(
        'api/produtos/etiquetas/presets/<str:client_key>/',
        views.api_etiquetas_preset_detail,
        name='api_etiquetas_preset_detail',
    ),
    path(
        'api/produtos/etiquetas/historico/',
        views.api_etiquetas_historico_lista,
        name='api_etiquetas_historico_lista',
    ),
    path(
        'api/produtos/etiquetas/historico/salvar/',
        views.api_etiquetas_historico_salvar,
        name='api_etiquetas_historico_salvar',
    ),
    path(
        'api/produtos/etiquetas/historico/<int:pk>/',
        views.api_etiquetas_historico_detalhe,
        name='api_etiquetas_historico_detalhe',
    ),
    path(
        'api/produtos/etiquetas/lote/',
        views.api_etiquetas_lote,
        name='api_etiquetas_lote',
    ),
    path(
        'api/produtos/etiquetas/lote/<int:pk>/',
        views.api_etiquetas_lote_detalhe,
        name='api_etiquetas_lote_detalhe',
    ),
    path(
        'api/produtos/etiquetas/lote/<int:pk>/proxima-folha/',
        views.api_etiquetas_lote_proxima_folha,
        name='api_etiquetas_lote_proxima_folha',
    ),
    path(
        'api/produtos/etiquetas/lote/<int:pk>/confirmar-folha/',
        views.api_etiquetas_lote_confirmar_folha,
        name='api_etiquetas_lote_confirmar_folha',
    ),
    path(
        'api/produtos/etiquetas/lote/<int:pk>/desfazer-folha/',
        views.api_etiquetas_lote_desfazer_folha,
        name='api_etiquetas_lote_desfazer_folha',
    ),
    path(
        'api/produtos/etiquetas/lote/<int:pk>/cancelar/',
        views.api_etiquetas_lote_cancelar,
        name='api_etiquetas_lote_cancelar',
    ),
    path('relatorios/', views.relatorios_hub, name='relatorios_hub'),
    path('contabilidade/login/', views_nfce.contabilidade_login, name='contabilidade_login'),
    path('contabilidade/logout/', views_nfce.contabilidade_logout, name='contabilidade_logout'),
    path('contabilidade/', views_nfce.contabilidade_painel, name='contabilidade_painel'),
    path('relatorios/validade/', views.relatorios_validade, name='relatorios_validade'),
    path('relatorios/mais-vendidos/', relatorios_views.relatorios_mais_vendidos, name='relatorios_mais_vendidos'),
    path('relatorios/vendas-grupo/', relatorios_views.relatorios_vendas_grupo, name='relatorios_vendas_grupo'),
    path('relatorios/vendas-marca/', relatorios_views.relatorios_vendas_marca, name='relatorios_vendas_marca'),
    path('relatorios/curva-abc/', relatorios_views.relatorios_curva_abc, name='relatorios_curva_abc'),
    path('relatorios/giro-estoque/', relatorios_views.relatorios_giro_estoque, name='relatorios_giro_estoque'),
    path('relatorios/margem/', relatorios_views.relatorios_margem, name='relatorios_margem'),
    path('relatorios/vendas-operador/', relatorios_views.relatorios_vendas_operador, name='relatorios_vendas_operador'),
    path('relatorios/ranking-clientes/', relatorios_views.relatorios_ranking_clientes, name='relatorios_ranking_clientes'),
    path('relatorios/comparativo/', relatorios_views.relatorios_comparativo, name='relatorios_comparativo'),
    path('relatorios/formas-pagamento/', relatorios_views.relatorios_formas_pagamento, name='relatorios_formas_pagamento'),
    path('relatorios/ruptura/', relatorios_views.relatorios_ruptura, name='relatorios_ruptura'),
    path('relatorios/comissao/', relatorios_views.relatorios_comissao, name='relatorios_comissao'),
    path('relatorios/inventario/', relatorios_views.relatorios_inventario, name='relatorios_inventario'),
    path('relatorios/estoque-min-max/', relatorios_views.relatorios_estoque_min_max, name='relatorios_estoque_min_max'),
    path('relatorios/estoque-resumo/', relatorios_views.relatorios_estoque_resumo, name='relatorios_estoque_resumo'),
    path('relatorios/estoque-sem-custo/', relatorios_views.relatorios_estoque_sem_custo, name='relatorios_estoque_sem_custo'),
    path('relatorios/estoque-zerados/', relatorios_views.relatorios_estoque_zerados, name='relatorios_estoque_zerados'),

    path(
        'entrada-nota/',
        views.entrada_nota_view,
        name='entrada_nota',
    ),
    path(
        'lancamentos/',
        views.lancamentos_financeiros_view,
        name='lancamentos_financeiros',
    ),
    path(
        'financeiro/resumo-gerencial/',
        views.resumo_financeiro_gerencial_view,
        name='resumo_financeiro_gerencial',
    ),
    path(
        'financeiro/dashboard-gerencial/',
        dashboard_financeiro_completo,
        name='dashboard_financeiro_completo',
    ),
    path(
        'financeiro/dashboard-restrito/',
        dashboard_financeiro_restrito,
        name='dashboard_financeiro_restrito',
    ),
    path(
        'api/financeiro/dashboard-restrito/',
        api_dashboard_financeiro_restrito,
        name='api_dashboard_financeiro_restrito',
    ),
    path('dashboard/gerencial/', views.dashboard_gerencial_view, name='dashboard_gerencial'),
    path(
        'interno/preview-gastos-bi/',
        views.dashboard_interno_preview_view,
        name='dashboard_interno_preview',
    ),

    path(
        'interno/dispenser-a6/',
        views.dispenser_a6_studio_view,
        name='dispenser_a6_studio',
    ),
    path(
        'interno/dispenser-a6/api/biblioteca/',
        views_dispenser_a6.api_dispenser_biblioteca,
        name='api_dispenser_biblioteca',
    ),
    path(
        'interno/dispenser-a6/api/midia/',
        views_dispenser_a6.api_dispenser_midia,
        name='api_dispenser_midia',
    ),
    path(
        'interno/dispenser-a6/api/documento/',
        views_dispenser_a6.api_dispenser_documento,
        name='api_dispenser_documento',
    ),
    path(
        'interno/dispenser-a6/api/migrar/',
        views_dispenser_a6.api_dispenser_migrar,
        name='api_dispenser_migrar',
    ),
    path(
        'interno/teste-busca/',
        views.interno_teste_busca_view,
        name='interno_teste_busca',
    ),
    path(
        'interno/pg-backup/',
        pg_backup_views.pg_backup_painel,
        name='pg_backup_painel',
    ),
    path(
        'interno/importar-catalogo-faltantes/',
        pg_backup_views.importar_catalogo_faltantes,
        name='interno_importar_catalogo_faltantes',
    ),
    path(
        'interno/recuperar-produtos-vendas/',
        pg_backup_views.recuperar_produtos_vendas,
        name='interno_recuperar_produtos_vendas',
    ),
    path(
        'interno/inspecionar-produto/',
        pg_backup_views.inspecionar_produto_pg,
        name='interno_inspecionar_produto',
    ),
    path(
        'lancamentos/dre/',
        views.lancamentos_dre_view,
        name='lancamentos_dre',
    ),
    path(
        'lancamentos/contas-pagar/',
        views.lancamentos_contas_pagar_view,
        name='lancamentos_contas_pagar',
    ),
    path(
        'lancamentos/contas-pagar/classico/',
        views.lancamentos_contas_pagar_classico_view,
        name='lancamentos_contas_pagar_classico',
    ),
    path(
        'lancamentos/contas-pagar/teste/',
        views.lancamentos_contas_pagar_teste_view,
        name='lancamentos_contas_pagar_teste',
    ),
    path(
        'lancamentos/contas-pagar/calendario/',
        views.lancamentos_contas_pagar_calendario_view,
        name='lancamentos_contas_pagar_calendario',
    ),
    path(
        'lancamentos/contas-receber/',
        views.lancamentos_contas_receber_view,
        name='lancamentos_contas_receber',
    ),
    path(
        'lancamentos/novo-manual/',
        views.lancamentos_manual_view,
        name='lancamentos_manual',
    ),
    path(
        'emprestimos/',
        views.emprestimos_gestao_view,
        name='emprestimos_gestao',
    ),
    path(
        'repasse-vila/',
        views_repasse_vila.repasse_vila_view,
        name='repasse_vila',
    ),
    path(
        'api/repasse-vila/calc/',
        views_repasse_vila.api_repasse_vila_calc,
        name='api_repasse_vila_calc',
    ),
    path(
        'api/repasse-vila/historico/',
        views_repasse_vila.api_repasse_vila_historico,
        name='api_repasse_vila_historico',
    ),
    path(
        'api/repasse-vila/config/',
        views_repasse_vila.api_repasse_vila_config,
        name='api_repasse_vila_config',
    ),
    path(
        'api/repasse-vila/reserva-log/',
        views_repasse_vila.api_repasse_vila_reserva_log,
        name='api_repasse_vila_reserva_log',
    ),
    path(
        'api/repasse-vila/cofrinho/',
        views_repasse_vila.api_repasse_vila_cofrinho,
        name='api_repasse_vila_cofrinho',
    ),
    path(
        'api/repasse-vila/cofrinho/separar/',
        views_repasse_vila.api_repasse_vila_cofrinho_separar,
        name='api_repasse_vila_cofrinho_separar',
    ),
    path(
        'api/repasse-vila/cofrinho/movimento/',
        views_repasse_vila.api_repasse_vila_cofrinho_movimento,
        name='api_repasse_vila_cofrinho_movimento',
    ),
    path(
        'api/repasse-vila/cofrinho/estornar/',
        views_repasse_vila.api_repasse_vila_cofrinho_estornar,
        name='api_repasse_vila_cofrinho_estornar',
    ),
    path(
        'api/repasse-vila/meta/',
        views_repasse_vila.api_repasse_vila_meta,
        name='api_repasse_vila_meta',
    ),
    path(
        'api/repasse-vila/confirmar/',
        views_repasse_vila.api_repasse_vila_confirmar,
        name='api_repasse_vila_confirmar',
    ),
    path(
        'api/repasse-vila/acumulado/',
        views_repasse_vila.api_repasse_vila_acumulado,
        name='api_repasse_vila_acumulado',
    ),
    path(
        'api/repasse-vila/acumulado/ajuste/',
        views_repasse_vila.api_repasse_vila_acumulado_ajuste,
        name='api_repasse_vila_acumulado_ajuste',
    ),
    path(
        'api/repasse-vila/acumulado/zerar/',
        views_repasse_vila.api_repasse_vila_acumulado_zerar,
        name='api_repasse_vila_acumulado_zerar',
    ),
    path(
        'emprestimos/externo/',
        views.emprestimos_externo_view,
        name='emprestimos_externo',
    ),
    path(
        'emprestimos/interno/',
        views.emprestimos_interno_view,
        name='emprestimos_interno',
    ),
    path(
        'emprestimos/consulta/',
        views.emprestimos_consulta_view,
        name='emprestimos_consulta',
    ),
    path(
        'lancamentos/fluxo-calendario/',
        views.lancamentos_fluxo_calendario_view,
        name='lancamentos_fluxo_calendario',
    ),
    path('estoque/sincronizacao/', views.estoque_sincronizacao_view, name='estoque_sincronizacao'),
    path('pdv/checkout/', views.pdv_checkout, name='pdv_checkout'),
    path('vendas/exportar-csv/', views.vendas_exportar_csv, name='vendas_exportar_csv'),
    path('vendas/lojas/manifest.webmanifest', views.vendas_lojas_manifest, name='vendas_lojas_manifest'),
    path('vendas/lojas/sw.js', views.vendas_lojas_sw, name='vendas_lojas_sw'),
    path('vendas/lojas/', views.vendas_lojas_resumo, name='vendas_lojas_resumo'),
    path('vendas/', views.vendas_lista, name='vendas_lista'),
    path('fiado/', fiado_views.fiado_gestao, name='fiado_gestao'),
    path('api/fiado/resumo/', fiado_views.api_fiado_resumo, name='api_fiado_resumo'),
    path('api/fiado/clientes/', fiado_views.api_fiado_clientes, name='api_fiado_clientes'),
    path('api/fiado/titulos/', fiado_views.api_fiado_titulos, name='api_fiado_titulos'),
    path('api/fiado/cliente-credito/', fiado_views.api_fiado_cliente_credito, name='api_fiado_cliente_credito'),
    path('api/fiado/buscar-cliente/', fiado_views.api_fiado_buscar_cliente, name='api_fiado_buscar_cliente'),
    path('api/fiado/baixa/', fiado_views.api_fiado_baixa, name='api_fiado_baixa'),
    path('api/fiado/baixa-selecionados/', fiado_views.api_fiado_baixa_selecionados, name='api_fiado_baixa_selecionados'),
    path('api/fiado/baixa-cliente/', fiado_views.api_fiado_baixa_cliente, name='api_fiado_baixa_cliente'),
    path('api/fiado/cobranca-pdv/', fiado_views.api_fiado_cobranca_pdv, name='api_fiado_cobranca_pdv'),
    path('api/fiado/baixa-pdv/', fiado_views.api_fiado_baixa_pdv, name='api_fiado_baixa_pdv'),
    path('api/fiado/recibos/', fiado_views.api_fiado_recibos, name='api_fiado_recibos'),
    path('api/fiado/recibo/<int:recibo_id>/', fiado_views.api_fiado_recibo, name='api_fiado_recibo'),
    path('api/fiado/recibo/', fiado_views.api_fiado_recibo, name='api_fiado_recibo_baixas'),
    path('api/fiado/titulo-editar/', fiado_views.api_fiado_titulo_editar, name='api_fiado_titulo_editar'),
    path('api/fiado/limite/', fiado_views.api_fiado_limite, name='api_fiado_limite'),
    path('api/fiado/importar/', fiado_views.api_fiado_importar_planilha, name='api_fiado_importar_planilha'),
    path('api/fiado/backup/', fiado_views.api_fiado_backup_export, name='api_fiado_backup_export'),
    path('vendas-hoje/', views.vendas_hoje_redirect, name='vendas_hoje'),
    path(
        'venda/<int:pk>/cupom/',
        views.api_venda_agro_cupom,
        name='api_venda_agro_cupom',
    ),
    path(
        'venda/<int:pk>/nfce/cupom/',
        views_nfce.api_venda_agro_nfce_cupom,
        name='api_venda_agro_nfce_cupom',
    ),
    path(
        'venda/<int:pk>/nfce/',
        views_nfce.api_venda_agro_nfce_info,
        name='api_venda_agro_nfce_info',
    ),
    path(
        'venda/<int:pk>/nfce/emitir/',
        views_nfce.api_venda_agro_nfce_emitir,
        name='api_venda_agro_nfce_emitir',
    ),
    path(
        'venda/<int:pk>/nfce/cancelar/',
        views_nfce.api_venda_agro_nfce_cancelar,
        name='api_venda_agro_nfce_cancelar',
    ),
    path('api/nfce/status/', views_nfce.api_nfce_status, name='api_nfce_status'),
    path(
        'api/nfce/contabilidade/resumo/',
        views_nfce.api_nfce_contabilidade_resumo,
        name='api_nfce_contabilidade_resumo',
    ),
    path(
        'api/nfce/export-planilha/',
        views_nfce.api_nfce_export_planilha,
        name='api_nfce_export_planilha',
    ),
    path(
        'api/nfce/export-pendencias/',
        views_nfce.api_nfce_export_pendencias,
        name='api_nfce_export_pendencias',
    ),
    path('api/nfce/export-xml/', views_nfce.api_nfce_export_xml_zip, name='api_nfce_export_xml_zip'),
    path(
        'venda/<int:pk>/erp-envio/',
        views.api_venda_agro_erp_envio_info,
        name='api_venda_agro_erp_envio_info',
    ),
    path(
        'venda/<int:pk>/reverter-erp/',
        views.api_venda_agro_reverter_erp,
        name='api_venda_agro_reverter_erp',
    ),
    path(
        'venda/<int:pk>/reenviar-erp/',
        views.api_venda_agro_reenviar_erp,
        name='api_venda_agro_reenviar_erp',
    ),
    path(
        'venda/<int:pk>/devolver/',
        views.api_venda_agro_devolver,
        name='api_venda_agro_devolver',
    ),
    path('venda/<int:pk>/', views.venda_agro_detalhe, name='venda_agro_detalhe'),
    path('clientes/', views.clientes_lista, name='clientes_lista'),
    path('clientes/sincronizar/', views.clientes_sincronizar, name='clientes_sincronizar'),
    path('clientes/novo/', views.cliente_novo, name='cliente_novo'),
    path('clientes/<int:pk>/editar/', views.cliente_editar, name='cliente_editar'),
    path('rh/', include('rh.urls')),
    path('caixa/', views.caixa_painel, name='caixa_painel'),
    path('caixa/saida/', views.caixa_saida_view, name='caixa_saida'),
    path('caixa/abrir/', views.caixa_abrir, name='caixa_abrir'),
    path('caixa/fechar/', views.caixa_fechar, name='caixa_fechar'),
    path('caixa/retiradas/', views.caixa_retiradas_historico, name='caixa_retiradas_historico'),
    path(
        'api/caixa/retiradas/export-xlsx/',
        views.api_caixa_retiradas_export_xlsx,
        name='api_caixa_retiradas_export_xlsx',
    ),
    path('caixa/relatorio/', views.caixa_relatorio, name='caixa_relatorio'),
    path(
        'caixa/relatorio-conferencias/',
        views.caixa_relatorio_conferencias,
        name='caixa_relatorio_conferencias',
    ),
    path('api/caixa/assumir-sessao/', views.api_caixa_assumir_sessao, name='api_caixa_assumir_sessao'),
    path(
        'api/caixa/conferencia-rascunho/',
        views.api_caixa_conferencia_rascunho,
        name='api_caixa_conferencia_rascunho',
    ),
    path(
        'api/caixa/conferencia-rascunho/salvar/',
        views.api_caixa_conferencia_rascunho_salvar,
        name='api_caixa_conferencia_rascunho_salvar',
    ),
    path(
        'api/caixa/conferencia-estado/',
        views.api_caixa_conferencia_estado,
        name='api_caixa_conferencia_estado',
    ),
    path('api/caixa/movimento/', views.api_caixa_movimento, name='api_caixa_movimento'),
    path('api/caixa/vincular-vendas/', views.api_caixa_vincular_vendas, name='api_caixa_vincular_vendas'),

    # --- APIs ---
    path('api/login-mobile/', views.api_login_mobile, name='api_login_mobile'),
    path('api/pdv/operador/', views.api_pdv_registrar_operador, name='api_pdv_registrar_operador'),
    path('api/pdv/deposito/', views.api_pdv_deposito, name='api_pdv_deposito'),
    path(
        'api/pdv/uso-loja/meta/',
        views_uso_loja.api_pdv_uso_loja_meta,
        name='api_pdv_uso_loja_meta',
    ),
    path(
        'api/pdv/uso-loja/confirmar/',
        views_uso_loja.api_pdv_uso_loja_confirmar,
        name='api_pdv_uso_loja_confirmar',
    ),
    path(
        'api/pdv/uso-loja/historico/',
        views_uso_loja.api_pdv_uso_loja_historico,
        name='api_pdv_uso_loja_historico',
    ),
    path(
        'api/pdv/uso-loja/estornar/<int:pk>/',
        views_uso_loja.api_pdv_uso_loja_estornar,
        name='api_pdv_uso_loja_estornar',
    ),
    path(
        'api/pdv/chat-loja/lista/',
        views_pdv_chat_loja.api_pdv_chat_loja_lista,
        name='api_pdv_chat_loja_lista',
    ),
    path(
        'api/pdv/chat-loja/enviar/',
        views_pdv_chat_loja.api_pdv_chat_loja_enviar,
        name='api_pdv_chat_loja_enviar',
    ),
    path(
        'api/atendimento-whatsapp/bot/',
        views_atendimento_whatsapp.api_atendimento_whatsapp_bot_get,
        name='api_atendimento_whatsapp_bot_get',
    ),
    path(
        'api/atendimento-whatsapp/bot/salvar/',
        views_atendimento_whatsapp.api_atendimento_whatsapp_bot_salvar,
        name='api_atendimento_whatsapp_bot_salvar',
    ),
    path(
        'api/atendimento-whatsapp/estado/',
        views_atendimento_whatsapp.api_atendimento_whatsapp_estado,
        name='api_atendimento_whatsapp_estado',
    ),
    path(
        'api/atendimento-whatsapp/conversas/',
        views_atendimento_whatsapp.api_atendimento_whatsapp_conversas,
        name='api_atendimento_whatsapp_conversas',
    ),
    path(
        'api/atendimento-whatsapp/mensagens/',
        views_atendimento_whatsapp.api_atendimento_whatsapp_mensagens,
        name='api_atendimento_whatsapp_mensagens',
    ),
    path(
        'api/atendimento-whatsapp/enviar/',
        views_atendimento_whatsapp.api_atendimento_whatsapp_enviar,
        name='api_atendimento_whatsapp_enviar',
    ),
    path(
        'api/atendimento-whatsapp/marcar-lida/',
        views_atendimento_whatsapp.api_atendimento_whatsapp_marcar_lida,
        name='api_atendimento_whatsapp_marcar_lida',
    ),
    path(
        'api/atendimento-whatsapp/definir-loja/',
        views_atendimento_whatsapp.api_atendimento_whatsapp_definir_loja,
        name='api_atendimento_whatsapp_definir_loja',
    ),
    path(
        'api/atendimento-whatsapp/transferir/',
        views_atendimento_whatsapp.api_atendimento_whatsapp_transferir,
        name='api_atendimento_whatsapp_transferir',
    ),
    path(
        'api/atendimento-whatsapp/contatos/',
        views_atendimento_whatsapp.api_atendimento_whatsapp_contatos,
        name='api_atendimento_whatsapp_contatos',
    ),
    path(
        'api/atendimento-whatsapp/agenda-vcf/',
        views_atendimento_whatsapp.api_atendimento_whatsapp_agenda_vcf,
        name='api_atendimento_whatsapp_agenda_vcf',
    ),
    path(
        'api/atendimento-whatsapp/abrir/',
        views_atendimento_whatsapp.api_atendimento_whatsapp_abrir,
        name='api_atendimento_whatsapp_abrir',
    ),
    path(
        'api/atendimento-whatsapp/novo/',
        views_atendimento_whatsapp.api_atendimento_whatsapp_novo,
        name='api_atendimento_whatsapp_novo',
    ),
    path(
        'api/atendimento-whatsapp/agenda-zap/',
        views_atendimento_whatsapp.api_atendimento_whatsapp_agenda_zap,
        name='api_atendimento_whatsapp_agenda_zap',
    ),
    path(
        'api/atendimento-whatsapp/historico/',
        views_atendimento_whatsapp.api_atendimento_whatsapp_historico,
        name='api_atendimento_whatsapp_historico',
    ),
    path(
        'api/atendimento-whatsapp/excluir/',
        views_atendimento_whatsapp.api_atendimento_whatsapp_excluir,
        name='api_atendimento_whatsapp_excluir',
    ),
    path(
        'api/atendimento-whatsapp/pairing/',
        views_atendimento_whatsapp.api_atendimento_whatsapp_pairing,
        name='api_atendimento_whatsapp_pairing',
    ),
    path(
        'api/atendimento-whatsapp/midia/<int:pk>/',
        views_atendimento_whatsapp.api_atendimento_whatsapp_midia,
        name='api_atendimento_whatsapp_midia',
    ),
    path(
        'api/atendimento-whatsapp/bridge/estado/',
        views_atendimento_whatsapp.api_atendimento_whatsapp_bridge_estado,
        name='api_atendimento_whatsapp_bridge_estado',
    ),
    path(
        'api/atendimento-whatsapp/bridge/lids/',
        views_atendimento_whatsapp.api_atendimento_whatsapp_bridge_lids,
        name='api_atendimento_whatsapp_bridge_lids',
    ),
    path(
        'api/atendimento-whatsapp/bridge/entrada/',
        views_atendimento_whatsapp.api_atendimento_whatsapp_bridge_entrada,
        name='api_atendimento_whatsapp_bridge_entrada',
    ),
    path(
        'api/atendimento-whatsapp/bridge/midia/<int:pk>/',
        views_atendimento_whatsapp.api_atendimento_whatsapp_bridge_midia,
        name='api_atendimento_whatsapp_bridge_midia',
    ),
    path(
        'api/atendimento-whatsapp/bridge/saida/',
        views_atendimento_whatsapp.api_atendimento_whatsapp_bridge_saida,
        name='api_atendimento_whatsapp_bridge_saida',
    ),
    path(
        'api/atendimento-whatsapp/bridge/saida-ok/',
        views_atendimento_whatsapp.api_atendimento_whatsapp_bridge_saida_ok,
        name='api_atendimento_whatsapp_bridge_saida_ok',
    ),
    path(
        'api/atendimento-whatsapp/bridge/contatos/',
        views_atendimento_whatsapp.api_atendimento_whatsapp_bridge_contatos,
        name='api_atendimento_whatsapp_bridge_contatos',
    ),
    path(
        'api/atendimento-whatsapp/bridge/pedido-ok/',
        views_atendimento_whatsapp.api_atendimento_whatsapp_bridge_pedido_ok,
        name='api_atendimento_whatsapp_bridge_pedido_ok',
    ),
    path(
        'api/pdv/topbar-clique/',
        views_pdv_topbar.api_pdv_topbar_clique,
        name='api_pdv_topbar_clique',
    ),
    path(
        'api/pdv/topbar-cliques/',
        views_pdv_topbar.api_pdv_topbar_cliques_resumo,
        name='api_pdv_topbar_cliques_resumo',
    ),
    path(
        'api/pdv/topbar-layout/',
        views_pdv_topbar.api_pdv_topbar_layout,
        name='api_pdv_topbar_layout',
    ),
    path(
        'api/pdv/transf-loja/resumo/',
        views_pdv_transf_loja.api_pdv_transf_loja_resumo,
        name='api_pdv_transf_loja_resumo',
    ),
    path(
        'api/pdv/transf-loja/saldos/',
        views_pdv_transf_loja.api_pdv_transf_loja_saldos,
        name='api_pdv_transf_loja_saldos',
    ),
    path(
        'api/pdv/transf-loja/lista/',
        views_pdv_transf_loja.api_pdv_transf_loja_lista,
        name='api_pdv_transf_loja_lista',
    ),
    path(
        'api/pdv/transf-loja/criar/',
        views_pdv_transf_loja.api_pdv_transf_loja_criar,
        name='api_pdv_transf_loja_criar',
    ),
    path(
        'api/pdv/transf-loja/ajustar/',
        views_pdv_transf_loja.api_pdv_transf_loja_ajustar,
        name='api_pdv_transf_loja_ajustar',
    ),
    path(
        'api/pdv/transf-loja/<int:pk>/acao/',
        views_pdv_transf_loja.api_pdv_transf_loja_acao,
        name='api_pdv_transf_loja_acao',
    ),
    path('api/produtos/cadastro/', views.api_produtos_cadastro, name='api_produtos_cadastro'),
    path(
        'api/produtos/cadastro/export-xlsx/',
        views.api_produtos_cadastro_export_xlsx,
        name='api_produtos_cadastro_export_xlsx',
    ),
    path(
        'api/produtos/cadastro/import-preview/',
        views.api_produtos_cadastro_import_preview,
        name='api_produtos_cadastro_import_preview',
    ),
    path(
        'api/produtos/cadastro/import-preview/status/',
        views.api_produtos_cadastro_import_preview_status,
        name='api_produtos_cadastro_import_preview_status',
    ),
    path(
        'api/produtos/cadastro/import-aplicar/',
        views.api_produtos_cadastro_import_aplicar,
        name='api_produtos_cadastro_import_aplicar',
    ),
    path(
        'api/produtos/cadastro/import-historico/',
        views.api_produtos_cadastro_import_historico,
        name='api_produtos_cadastro_import_historico',
    ),
    path(
        'api/produtos/cadastro/import-reverter/',
        views.api_produtos_cadastro_import_reverter,
        name='api_produtos_cadastro_import_reverter',
    ),
    path(
        'api/produtos/cadastro/estoque/export-xlsx/',
        views.api_produtos_cadastro_estoque_export_xlsx,
        name='api_produtos_cadastro_estoque_export_xlsx',
    ),
    path(
        'api/produtos/cadastro/estoque/import-preview/',
        views.api_produtos_cadastro_estoque_import_preview,
        name='api_produtos_cadastro_estoque_import_preview',
    ),
    path(
        'api/produtos/cadastro/estoque/import-aplicar/',
        views.api_produtos_cadastro_estoque_import_aplicar,
        name='api_produtos_cadastro_estoque_import_aplicar',
    ),
    path(
        'api/produtos/cadastro/estoque/import-historico/',
        views.api_produtos_cadastro_estoque_import_historico,
        name='api_produtos_cadastro_estoque_import_historico',
    ),
    path(
        'api/produtos/cadastro/estoque/import-reverter/',
        views.api_produtos_cadastro_estoque_import_reverter,
        name='api_produtos_cadastro_estoque_import_reverter',
    ),
    path('api/agro/fonte-status/', views.api_agro_fonte_status, name='api_agro_fonte_status'),
    path('api/produtos/gestao/lista/', views.api_produtos_gestao_lista, name='api_produtos_gestao_lista'),
    path('api/produtos/gestao/facetas/', views.api_produtos_gestao_facetas, name='api_produtos_gestao_facetas'),
    path(
        'api/produtos/gestao/ajuste-estoque/',
        views.api_produtos_gestao_ajuste_estoque,
        name='api_produtos_gestao_ajuste_estoque',
    ),
    path(
        'api/produtos/pdv-edicao-rapida/<str:produto_id>/',
        views.api_pdv_produto_edicao_rapida,
        name='api_pdv_produto_edicao_rapida',
    ),
    path(
        'api/produtos/pdv-ajuste-estoque/',
        views.api_pdv_produto_ajuste_estoque,
        name='api_pdv_produto_ajuste_estoque',
    ),
    path(
        'api/produtos/pdv-cadastro-rapido/checar/',
        views.api_pdv_cadastro_rapido_checar,
        name='api_pdv_cadastro_rapido_checar',
    ),
    path(
        'api/produtos/pdv-cadastro-rapido/gm-preview/',
        views.api_pdv_cadastro_rapido_gm_preview,
        name='api_pdv_cadastro_rapido_gm_preview',
    ),
    path(
        'api/produtos/pdv-cadastro-rapido/criar/',
        views.api_pdv_cadastro_rapido_criar,
        name='api_pdv_cadastro_rapido_criar',
    ),
    path(
        'api/produtos/cadastro/pendentes-pdv/',
        views.api_cadastro_pendentes_pdv,
        name='api_cadastro_pendentes_pdv',
    ),
    path(
        'api/produtos/cadastro/pendentes-pdv/marcar-conferido/',
        views.api_cadastro_pendente_pdv_marcar_conferido,
        name='api_cadastro_pendente_pdv_marcar_conferido',
    ),
    path(
        'api/produtos/gestao/overlay/',
        views.api_produtos_gestao_overlay_salvar,
        name='api_produtos_gestao_overlay_salvar',
    ),
    path(
        'api/produtos/custo-familia/propagar/',
        views.api_custo_familia_propagar,
        name='api_custo_familia_propagar',
    ),
    path(
        'api/produtos/gestao/mongo-codigo-sistema-reparar/',
        views.api_produtos_gestao_mongo_codigo_sistema_reparar,
        name='api_produtos_gestao_mongo_codigo_sistema_reparar',
    ),
    path(
        'api/produtos/gestao/erp-pendentes/',
        views.api_produtos_gestao_erp_pendentes,
        name='api_produtos_gestao_erp_pendentes',
    ),
    path(
        'api/produtos/gestao/erp-sincronizar-pendentes/',
        views.api_produtos_gestao_erp_sincronizar_pendentes,
        name='api_produtos_gestao_erp_sincronizar_pendentes',
    ),
    path(
        'api/overlay/lote/adicionar/',
        views.api_overlay_lote_adicionar,
        name='api_overlay_lote_adicionar',
    ),
    path(
        'api/overlay/lote/<int:lote_id>/remover/',
        views.api_overlay_lote_remover,
        name='api_overlay_lote_remover',
    ),
    path(
        'api/relatorios/validade/baixa/',
        views.api_relatorio_validade_baixa,
        name='api_relatorio_validade_baixa',
    ),
    path(
        'api/produtos/cadastro/detalhe/<str:produto_id>/',
        views.api_produtos_cadastro_detalhe,
        name='api_produtos_cadastro_detalhe',
    ),
    path(
        'api/produtos/cadastro/proximo-cb-loja/',
        views.api_produtos_cadastro_proximo_cb_loja,
        name='api_produtos_cadastro_proximo_cb_loja',
    ),
    path(
        'api/produtos/cadastro/faceta-nova/',
        views.api_produtos_cadastro_faceta_nova,
        name='api_produtos_cadastro_faceta_nova',
    ),
    path(
        'api/produtos/cadastro/somente-agro/excluir/',
        views.api_produtos_somente_agro_excluir,
        name='api_produtos_somente_agro_excluir',
    ),
    path(
        'api/produtos/cadastro/compras-historico/',
        views.api_produtos_cadastro_compras_historico,
        name='api_produtos_cadastro_compras_historico',
    ),
    path(
        'api/produtos/cadastro/estoque-movimentos/',
        views.api_produtos_cadastro_estoque_movimentos,
        name='api_produtos_cadastro_estoque_movimentos',
    ),
    path(
        'api/produtos/cadastro/alteracoes-historico/',
        views.api_produtos_cadastro_alteracoes_historico,
        name='api_produtos_cadastro_alteracoes_historico',
    ),
    path('api/produtos/grupos/', views.api_produtos_grupos_listar, name='api_produtos_grupos_listar'),
    path('api/produtos/grupos/salvar/', views.api_produtos_grupo_salvar, name='api_produtos_grupo_salvar'),
    path('api/produtos/grupos/<int:pk>/', views.api_produtos_grupo_obter, name='api_produtos_grupo_obter'),
    path(
        'api/produtos/grupos/<int:pk>/excluir/',
        views.api_produtos_grupo_excluir,
        name='api_produtos_grupo_excluir',
    ),
    path('api/buscar/', views.api_buscar_produtos, name='api_buscar_mobile'),
    path('api/buscar-v2/', views.api_buscar_produtos_v2, name='api_buscar_v2'),
    path('api/buscar-compras/', views.api_buscar_compras, name='api_buscar_compras'),
    path('api/lancamentos/', views.api_lancamentos_lista, name='api_lancamentos_lista'),
    path(
        'api/lancamentos/log/',
        views.api_lancamentos_log,
        name='api_lancamentos_log',
    ),
    path(
        'api/lancamentos/export-csv/',
        views.api_lancamentos_export_csv,
        name='api_lancamentos_export_csv',
    ),
    path(
        'api/lancamentos/export-financeiro-xlsx/',
        views.api_lancamentos_export_financeiro_xlsx,
        name='api_lancamentos_export_financeiro_xlsx',
    ),
    path(
        'api/lancamentos/export-financeiro-pdf/',
        views.api_lancamentos_export_financeiro_pdf,
        name='api_lancamentos_export_financeiro_pdf',
    ),
    path(
        'api/lancamentos/backup-completo.xlsx',
        views.api_lancamentos_backup_completo_xlsx,
        name='api_lancamentos_backup_completo_xlsx',
    ),
    path(
        'api/lancamentos/backup-abertos.zip',
        views.api_lancamentos_backup_abertos_xlsx,
        name='api_lancamentos_backup_abertos_xlsx',
    ),
    path(
        'api/lancamentos/backup-ultimo/',
        views.api_lancamentos_backup_ultimo,
        name='api_lancamentos_backup_ultimo',
    ),
    path(
        'api/lancamentos/congelamento-status/',
        views.api_lancamentos_congelamento_status,
        name='api_lancamentos_congelamento_status',
    ),
    path(
        'api/lancamentos/congelar-pre-corte/',
        views.api_lancamentos_congelar_pre_corte,
        name='api_lancamentos_congelar_pre_corte',
    ),
    path(
        'api/lancamentos/opcoes-baixa/',
        views.api_lancamentos_opcoes_baixa,
        name='api_lancamentos_opcoes_baixa',
    ),
    path(
        'api/lancamentos/opcoes-baixa/extra/',
        views.api_lancamentos_opcoes_baixa_extra_criar,
        name='api_lancamentos_opcoes_baixa_extra_criar',
    ),
    path(
        'api/lancamentos/opcoes-baixa/extra/<int:pk>/excluir/',
        views.api_lancamentos_opcoes_baixa_extra_excluir,
        name='api_lancamentos_opcoes_baixa_extra_excluir',
    ),
    path(
        'api/lancamentos/baixa/',
        views.api_lancamentos_baixa,
        name='api_lancamentos_baixa',
    ),
    path(
        'api/lancamentos/baixa-parcial/',
        views.api_lancamentos_baixa_parcial,
        name='api_lancamentos_baixa_parcial',
    ),
    path(
        'api/lancamentos/alterar/',
        views.api_lancamentos_alterar,
        name='api_lancamentos_alterar',
    ),
    path(
        'api/lancamentos/excluir/',
        views.api_lancamentos_excluir,
        name='api_lancamentos_excluir',
    ),
    path(
        'api/lancamentos/saida-caixa/',
        views.api_lancamentos_saida_caixa,
        name='api_lancamentos_saida_caixa',
    ),
    path(
        'api/lancamentos/dre-resumo/',
        views.api_lancamentos_dre_resumo,
        name='api_lancamentos_dre_resumo',
    ),
    path(
        'api/lancamentos/sugestoes/',
        views.api_lancamentos_sugestoes,
        name='api_lancamentos_sugestoes',
    ),
    path(
        'api/lancamentos/interpretar-texto/',
        views.api_lancamentos_interpretar_texto,
        name='api_lancamentos_interpretar_texto',
    ),
    path(
        'api/lancamentos/criar-manual-lote/',
        views.api_lancamentos_criar_manual_lote,
        name='api_lancamentos_criar_manual_lote',
    ),
    path(
        'api/lancamentos/definir-recorrente/',
        views.api_lancamentos_definir_recorrente,
        name='api_lancamentos_definir_recorrente',
    ),
    path(
        'api/emprestimos/listar/',
        views.api_emprestimos_listar,
        name='api_emprestimos_listar',
    ),
    path(
        'api/emprestimos/criar/',
        views.api_emprestimos_criar,
        name='api_emprestimos_criar',
    ),
    path(
        'api/emprestimos/interno-pagamento/',
        views.api_emprestimos_interno_pagamento,
        name='api_emprestimos_interno_pagamento',
    ),
    path(
        'api/emprestimos/interno-pagamento-excluir/',
        views.api_emprestimos_interno_pagamento_excluir,
        name='api_emprestimos_interno_pagamento_excluir',
    ),
    path(
        'api/emprestimos/defaults/',
        views.api_emprestimos_defaults,
        name='api_emprestimos_defaults',
    ),
    path(
        'api/emprestimos/erp-lancamentos/',
        views.api_emprestimos_erp_lancamentos,
        name='api_emprestimos_erp_lancamentos',
    ),
    path(
        'api/lancamentos/planos-distintos/',
        views.api_lancamentos_planos_distintos,
        name='api_lancamentos_planos_distintos',
    ),
    path(
        'api/lancamentos/planos-cadastro/',
        views.api_lancamentos_planos_cadastro,
        name='api_lancamentos_planos_cadastro',
    ),
    path(
        'api/lancamentos/planos-cadastro/criar/',
        views.api_lancamentos_planos_cadastro_criar,
        name='api_lancamentos_planos_cadastro_criar',
    ),
    path(
        'api/lancamentos/planos-orfaos/',
        views.api_lancamentos_planos_orfaos,
        name='api_lancamentos_planos_orfaos',
    ),
    path(
        'api/lancamentos/planos-mapear/',
        views.api_lancamentos_planos_mapear,
        name='api_lancamentos_planos_mapear',
    ),
    path(
        'api/lancamentos/atalhos-filtro/',
        views.api_lancamentos_atalhos_filtro,
        name='api_lancamentos_atalhos_filtro',
    ),
    path(
        'api/lancamentos/contas-pagar/calendario/',
        views.api_lancamentos_contas_pagar_calendario,
        name='api_lancamentos_contas_pagar_calendario',
    ),
    path(
        'api/lancamentos/fluxo-calendario/',
        views.api_lancamentos_fluxo_calendario,
        name='api_lancamentos_fluxo_calendario',
    ),
    path(
        'api/dashboard/gerencial/conteudo/',
        views.dashboard_gerencial_conteudo,
        name='dashboard_gerencial_conteudo',
    ),
    path(
        'api/dashboard/gerencial/feed/',
        views.dashboard_gerencial_feed,
        name='dashboard_gerencial_feed',
    ),
    path(
        'api/dashboard/gerencial/sincronizar/',
        views.dashboard_gerencial_sincronizar,
        name='dashboard_gerencial_sincronizar',
    ),
    path(
        'api/interno/preview-gastos-bi/conteudo/',
        views.dashboard_interno_preview_conteudo,
        name='dashboard_interno_preview_conteudo',
    ),
    path(
        'api/interno/preview-gastos-bi/sincronizar/',
        views.dashboard_interno_preview_sincronizar,
        name='dashboard_interno_preview_sincronizar',
    ),
    path(
        'api/lancamentos/contas-pagar/',
        views.api_lancamentos_contas_pagar,
        name='api_lancamentos_contas_pagar',
    ),
    path('api/ajustar/', views.api_ajustar_estoque, name='api_ajustar_estoque'),
    path(
        'api/deletar-ajuste/<int:id>/',
        views.api_deletar_ajuste,
        name='api_deletar_ajuste',
    ),
    path(
        'api/limpar-historico/',
        views.api_limpar_historico_ajustes,
        name='api_limpar_historico_ajustes',
    ),
    path('api/todos-produtos/', views.api_todos_produtos_local, name='api_todos_produtos_local'),
    path('api/todos-produtos/delta/', views.api_todos_produtos_delta, name='api_todos_produtos_delta'),
    path('api/pdv/catalogo-slim/', views.api_pdv_catalogo_slim, name='api_pdv_catalogo_slim'),
    path('api/pdv/racoes-overlay/', views.api_pdv_racoes_overlay, name='api_pdv_racoes_overlay'),
    path('api/pdv/saldos/', views.api_pdv_saldos_compacto, name='api_pdv_saldos'),
    path('api/pdv/metricas-produtos/', views.api_pdv_metricas_produtos, name='api_pdv_metricas_produtos'),
    path('api/pdv/top-vendidos/', views.api_pdv_top_vendidos, name='api_pdv_top_vendidos'),
    path('api/pdv/invalidar-catalogo/', views.api_pdv_invalidar_cache_catalogo, name='api_pdv_invalidar_catalogo'),
    path('api/cron/enviar-alerta-vendas-dia/', views.api_cron_enviar_alerta_vendas_dia, name='api_cron_enviar_alerta_vendas_dia'),
    path('api/cron/estoque-mongo-ping/', views.api_cron_estoque_mongo_ping, name='api_cron_estoque_mongo_ping'),
    path(
        'api/cron/rh-envio-cp-automatico/',
        views.api_cron_rh_envio_cp_automatico,
        name='api_cron_rh_envio_cp_automatico',
    ),
    path(
        'api/cron/pg-backup-nightly/',
        views.api_cron_pg_backup_nightly,
        name='api_cron_pg_backup_nightly',
    ),
    path(
        'api/cron/importar-catalogo-mongo/',
        views.api_cron_importar_catalogo_mongo,
        name='api_cron_importar_catalogo_mongo',
    ),
    path(
        'api/cron/importar-titulos-financeiro-mongo-pg/',
        views.api_cron_importar_titulos_financeiro_mongo_pg,
        name='api_cron_importar_titulos_financeiro_mongo_pg',
    ),
    path(
        'api/cron/sincronizar-titulos-financeiro-mongo-pg/',
        views.api_cron_sincronizar_titulos_financeiro_mongo_pg,
        name='api_cron_sincronizar_titulos_financeiro_mongo_pg',
    ),
    path(
        'api/agro/financeiro-pg-conferencia/',
        views.api_agro_financeiro_pg_conferencia,
        name='api_agro_financeiro_pg_conferencia',
    ),
    path(
        'api/cron/copiar-snapshot-pdv-loja/',
        views.api_cron_copiar_snapshot_pdv_loja,
        name='api_cron_copiar_snapshot_pdv_loja',
    ),
    path('api/autocomplete/', views.api_autocomplete_produtos, name='api_autocomplete_produtos'),
    path('api/buscar-clientes/', views.api_buscar_clientes, name='api_buscar_clientes'),
    path('api/listar-clientes/', views.api_list_customers, name='api_list_customers'),
    path('api/enviar-pedido-erp/', views.api_enviar_pedido_erp, name='api_enviar_pedido_erp'),
    path('api/pdv/cliente-rapido/', views.api_pdv_cliente_rapido, name='api_pdv_cliente_rapido'),
    path(
        'api/pdv/cliente/<int:pk>/editar/',
        views.api_pdv_cliente_editar,
        name='api_pdv_cliente_editar',
    ),
    path(
        'api/pdv/cliente/whatsapp-duplicado/',
        views_cliente_cadastro.api_cliente_whatsapp_duplicado,
        name='api_cliente_whatsapp_duplicado',
    ),
    path(
        'api/pdv/cliente/<int:pk>/limpar-whatsapp/',
        views_cliente_cadastro.api_cliente_limpar_whatsapp,
        name='api_cliente_limpar_whatsapp',
    ),
    path(
        'api/pdv/cliente/<int:pk>/exclusao-preview/',
        views_cliente_cadastro.api_cliente_exclusao_preview,
        name='api_cliente_exclusao_preview',
    ),
    path(
        'api/pdv/cliente/<int:pk>/transferir-saldos/',
        views_cliente_cadastro.api_cliente_transferir_saldos,
        name='api_cliente_transferir_saldos',
    ),
    path(
        'api/pdv/cliente/<int:pk>/excluir/',
        views_cliente_cadastro.api_cliente_excluir,
        name='api_cliente_excluir',
    ),
    path(
        'api/pdv/cliente/<int:pk>/vale-credito-manual/',
        views_cliente_cadastro.api_cliente_vale_credito_manual,
        name='api_cliente_vale_credito_manual',
    ),
    path(
        'api/pdv/cliente/<int:pk>/eventos/',
        views_cliente_cadastro.api_cliente_eventos,
        name='api_cliente_eventos',
    ),
    path('api/pdv/geocode-plus/', views.api_pdv_geocode_plus, name='api_pdv_geocode_plus'),
    path('api/pdv/checkout-draft/', views.api_pdv_salvar_checkout_draft, name='api_pdv_salvar_checkout_draft'),
    path('api/pdv/checkout-draft/clear/', views.api_pdv_limpar_checkout_draft, name='api_pdv_limpar_checkout_draft'),
    path('api/pdv/orcamentos/', views.api_pdv_orcamentos, name='api_pdv_orcamentos'),
    path('api/pdv/orcamentos/<int:orc_local_id>/', views.api_pdv_orcamento_detalhe, name='api_pdv_orcamento_detalhe'),
    path(
        'api/pdv/mp-point/criar/',
        views_mp_point.api_pdv_mp_point_criar,
        name='api_pdv_mp_point_criar',
    ),
    path(
        'api/pdv/mp-point/status/',
        views_mp_point.api_pdv_mp_point_status,
        name='api_pdv_mp_point_status',
    ),
    path(
        'api/pdv/mp-point/confirmar-tranche/',
        views_mp_point.api_pdv_mp_point_confirmar_tranche,
        name='api_pdv_mp_point_confirmar_tranche',
    ),
    path(
        'api/pdv/mp-point/finalizar/',
        views_mp_point.api_pdv_mp_point_finalizar,
        name='api_pdv_mp_point_finalizar',
    ),
    path(
        'api/pdv/mp-point/abandonar/',
        views_mp_point.api_pdv_mp_point_abandon,
        name='api_pdv_mp_point_abandon',
    ),
    path(
        'api/pdv/mp-point/forcar-liberar/',
        views_mp_point.api_pdv_mp_point_forcar_liberar,
        name='api_pdv_mp_point_forcar_liberar',
    ),
    path('api/buscar-produto-id/<str:id>/', views.api_buscar_produto_id, name='api_buscar_produto_id'),
    path(
        'api/entrada-nota/sefaz-status/',
        views.api_entrada_nota_sefaz_status,
        name='api_entrada_nota_sefaz_status',
    ),
    path(
        'api/entrada-nota/parse-xml/',
        views.api_entrada_nota_parse_xml,
        name='api_entrada_nota_parse_xml',
    ),
    path(
        'api/entrada-nota/salvar/',
        views.api_entrada_nota_salvar,
        name='api_entrada_nota_salvar',
    ),
    path(
        'api/entrada-nota/rascunhos/',
        views.api_entrada_nota_rascunhos,
        name='api_entrada_nota_rascunhos',
    ),
    path(
        'api/entrada-nota/rascunho/',
        views.api_entrada_nota_rascunho_obter,
        name='api_entrada_nota_rascunho_obter',
    ),
    path(
        'api/entrada-nota/rascunho/excluir/',
        views.api_entrada_nota_rascunho_excluir,
        name='api_entrada_nota_rascunho_excluir',
    ),
    path(
        'api/entrada-nota/rascunho/atualizar/',
        views.api_entrada_nota_rascunho_atualizar,
        name='api_entrada_nota_rascunho_atualizar',
    ),
    path(
        'api/entrada-nota/rascunho/acao/',
        views.api_entrada_nota_rascunho_acao,
        name='api_entrada_nota_rascunho_acao',
    ),
    path(
        'api/entrada-nota/fornecedores/',
        views.api_entrada_nota_fornecedores,
        name='api_entrada_nota_fornecedores',
    ),
    path(
        'api/entrada-nota/auditoria-financeiro/',
        views.api_entrada_nota_auditoria_financeiro,
        name='api_entrada_nota_auditoria_financeiro',
    ),
    path(
        'api/entrada-nota/conferir-codigo/',
        views.api_entrada_nota_conferir_codigo,
        name='api_entrada_nota_conferir_codigo',
    ),
    path(
        'api/entrada-nota/preview-custo/',
        views.api_entrada_nota_preview_custo,
        name='api_entrada_nota_preview_custo',
    ),
    path(
        'api/entrada-nota/aprovar-wizard/',
        views.api_entrada_nota_aprovar_wizard,
        name='api_entrada_nota_aprovar_wizard',
    ),
    path(
        'api/entrada-nota/reabrir-nota/',
        views.api_entrada_nota_reabrir_nota,
        name='api_entrada_nota_reabrir_nota',
    ),
    path(
        'api/entrada-nota/financeiro/',
        views.api_entrada_nota_financeiro,
        name='api_entrada_nota_financeiro',
    ),
    path(
        'api/entrada-nota/estoque-agro/',
        views.api_entrada_nota_estoque_agro,
        name='api_entrada_nota_estoque_agro',
    ),
    path(
        'api/entrada-nota/dist-dfe/',
        views.api_entrada_nota_dist_dfe,
        name='api_entrada_nota_dist_dfe',
    ),
    path(
        'api/entrada-nota/dfe-por-chave/',
        views.api_entrada_nota_dfe_por_chave,
        name='api_entrada_nota_dfe_por_chave',
    ),
    path(
        'api/entrada-nota/dfe-inbox/',
        views.api_entrada_nota_dfe_inbox,
        name='api_entrada_nota_dfe_inbox',
    ),
    path(
        'api/entrada-nota/dfe-inbox/<int:doc_id>/',
        views.api_entrada_nota_dfe_inbox_detalhe,
        name='api_entrada_nota_dfe_inbox_detalhe',
    ),
    path(
        'api/entrada-nota/dfe-inbox/<int:doc_id>/ignorar/',
        views.api_entrada_nota_dfe_inbox_ignorar,
        name='api_entrada_nota_dfe_inbox_ignorar',
    ),
    path(
        'api/entrada-nota/dfe-inbox/<int:doc_id>/ciencia/',
        views.api_entrada_nota_dfe_inbox_ciencia,
        name='api_entrada_nota_dfe_inbox_ciencia',
    ),
    path(
        'api/cron/dfe-consultar-inbox/',
        views.api_cron_dfe_consultar_inbox,
        name='api_cron_dfe_consultar_inbox',
    ),
    path(
        'api/entrada-nota/produto-margem/',
        views.api_entrada_nota_produto_margem,
        name='api_entrada_nota_produto_margem',
    ),

# Planos de contas — Config (F11). Modelo já na loja (0065); sem migrate neste pacote.
path(
    "configuracao/planos-conta/",
    views_planos_conta.planos_conta_config_view,
    name="planos_conta_config",
),
path(
    "api/configuracao/planos-conta/",
    views_planos_conta.api_planos_conta_lista,
    name="api_planos_conta_lista",
),
path(
    "api/configuracao/planos-conta/salvar/",
    views_planos_conta.api_planos_conta_salvar,
    name="api_planos_conta_salvar",
),
path(
    "api/configuracao/planos-conta/<int:pk>/toggle/",
    views_planos_conta.api_planos_conta_toggle,
    name="api_planos_conta_toggle",
),
path(
    "api/configuracao/planos-conta/carregar-padrao/",
    views_planos_conta.api_planos_conta_seed,
    name="api_planos_conta_seed",
),

]
