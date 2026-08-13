"""Registro de categorias para backup/restore Postgres Agro (FL-048)."""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class PgBackupCategory:
    slug: str
    label: str
    warning: str
    models: tuple[str, ...]


PG_BACKUP_FORMAT = "agro_pg_backup_v1"

PG_BACKUP_CATEGORIES: tuple[PgBackupCategory, ...] = (
    PgBackupCategory(
        slug="sistema",
        label="Sistema e usuários",
        warning="Inclui usuários Django e perfis (PIN). Restaurar substitui logins e senhas desta categoria.",
        models=(
            "base.Empresa",
            "base.Loja",
            "lojas.Loja",
            "base.IntegracaoERP",
            "auth.User",
            "base.PerfilUsuario",
        ),
    ),
    PgBackupCategory(
        slug="catalogo",
        label="Catálogo Agro",
        warning="Produtos, overlay, grupos, promoções e lotes. PDV e gestão leem estes dados.",
        models=(
            "produtos.Produto",
            "produtos.ProdutoGestaoOverlayAgro",
            "produtos.ProdutoGrupoAgro",
            "produtos.ProdutoGrupoVarianteAgro",
            "produtos.ProdutoMarcaVariacaoAgro",
            "produtos.PromocaoAgro",
            "produtos.PromocaoProdutoAgro",
            "produtos.EstoqueLote",
        ),
    ),
    PgBackupCategory(
        slug="clientes",
        label="Clientes",
        warning="Substitui cadastro de clientes Agro. Vendas e fiado dependem destes registros.",
        models=("produtos.ClienteAgro",),
    ),
    PgBackupCategory(
        slug="vendas",
        label="Vendas e NFC-e",
        warning="Substitui vendas Agro, itens, numeração e documentos fiscais.",
        models=(
            "produtos.VendaAgro",
            "produtos.ItemVendaAgro",
            "produtos.NfceNumeracaoAgro",
            "produtos.NfceDocumentoAgro",
            "produtos.PdvMercadoPagoPointOrder",
        ),
    ),
    PgBackupCategory(
        slug="caixa",
        label="Caixa",
        warning="Substitui sessões e movimentos de caixa.",
        models=(
            "produtos.SessaoCaixa",
            "produtos.MovimentoCaixa",
        ),
    ),
    PgBackupCategory(
        slug="financeiro",
        label="Financeiro (CP/CR Postgres)",
        warning="Substitui títulos financeiros Agro e lançamentos do app financeiro.",
        models=(
            "financeiro.GrupoEmpresarial",
            "financeiro.GrupoEmpresarialEmpresa",
            "produtos.TituloFinanceiroAgro",
            "financeiro.LancamentoFinanceiro",
            "produtos.OpcaoBaixaFinanceiroExtra",
            "produtos.PlanoContaAgro",
            "produtos.PlanoContaAliasAgro",
            "produtos.LancamentoAtalhoFiltro",
            "financeiro.GraficoGastosAtalhoAgro",
        ),
    ),
    PgBackupCategory(
        slug="fiado",
        label="Fiado",
        warning="Substitui títulos, baixas e eventos de fiado.",
        models=(
            "produtos.FiadoTituloAgro",
            "produtos.FiadoBaixaAgro",
            "produtos.FiadoEventoAgro",
        ),
    ),
    PgBackupCategory(
        slug="estoque",
        label="Estoque Agro",
        warning="Substitui ajustes, políticas, transferências e indicadores de estoque.",
        models=(
            "estoque.Estoque",
            "estoque.EstoqueSyncHealth",
            "estoque.AjusteRapidoEstoque",
            "estoque.ContagemCiclicaSessao",
            "estoque.ContagemCiclicaLinha",
            "estoque.ContagemCiclicaParticipante",
            "estoque.ConfiguracaoTransferencia",
            "estoque.PedidoTransferencia",
            "estoque.HistoricoTransferencia",
            "estoque.PoliticaEstoque",
            "estoque.IndicadorProdutoLoja",
        ),
    ),
    PgBackupCategory(
        slug="entregas",
        label="Entregas",
        warning="Substitui pedidos de entrega pós-venda.",
        models=("produtos.PedidoEntrega",),
    ),
    PgBackupCategory(
        slug="entrada_nf",
        label="Entrada NF (rascunhos)",
        warning="Substitui rascunhos de entrada de nota salvos no Postgres.",
        models=("produtos.EntradaNotaRascunhoAgro",),
    ),
    PgBackupCategory(
        slug="rh",
        label="RH",
        warning="Substitui funcionários, folhas, vales e pagamentos de salário.",
        models=(
            "rh.Funcionario",
            "rh.HistoricoSalarial",
            "rh.FechamentoFolhaSimplificado",
            "rh.ItemFechamentoFolha",
            "rh.ValeFuncionario",
            "rh.PagamentoSalarioFuncionario",
            "rh.InconsistenciaIntegracaoRh",
        ),
    ),
    PgBackupCategory(
        slug="orcamentos",
        label="Orçamentos PDV",
        warning="Substitui orçamentos salvos no servidor (sync multi-PC).",
        models=("produtos.OrcamentoPdvAgro",),
    ),
    PgBackupCategory(
        slug="historico",
        label="Histórico e auditoria",
        warning="Imports de planilha, etiquetas, BI dia e histórico ERP no F8.",
        models=(
            "produtos.CadastroPlanilhaImportHistoricoAgro",
            "produtos.ProdutoCadastroAlteracaoAgro",
            "produtos.EtiquetaImpressaoHistoricoAgro",
            "produtos.DashboardVendaDiaHistoricoAgro",
            "produtos.RelacionamentoHistoricoImportLoteAgro",
            "produtos.RelacionamentoVendaHistoricoErpAgro",
            "produtos.RelacionamentoItemHistoricoErpAgro",
        ),
    ),
)

PG_BACKUP_CATEGORY_BY_SLUG = {c.slug: c for c in PG_BACKUP_CATEGORIES}

PG_BACKUP_ALL_SLUGS = tuple(c.slug for c in PG_BACKUP_CATEGORIES)

RESTORE_CONFIRM_PHRASE = "RESTAURAR BACKUP PG"
