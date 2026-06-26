"""Feature flags — desvinculação ERP. Default ``legacy`` = produção inalterada."""
from __future__ import annotations

from django.conf import settings

_FONTE_CATALOGO_LEGADO = "legacy"
_FONTE_CATALOGO_AGRO = "agro_pg"
_FONTE_ESTOQUE_LEGADO = "legacy"
_FONTE_ESTOQUE_LEDGER = "ledger"
_FONTE_FINANCEIRO_LEGADO = "legacy"
_FONTE_FINANCEIRO_AGRO = "agro_pg"


def _norm(v: object, default: str) -> str:
    s = (str(v or default)).strip().lower()
    return s or default


def agro_fonte_catalogo() -> str:
    return _norm(getattr(settings, "AGRO_FONTE_CATALOGO", _FONTE_CATALOGO_LEGADO), _FONTE_CATALOGO_LEGADO)


def agro_fonte_estoque() -> str:
    return _norm(getattr(settings, "AGRO_FONTE_ESTOQUE", _FONTE_ESTOQUE_LEGADO), _FONTE_ESTOQUE_LEGADO)


def agro_fonte_financeiro() -> str:
    return _norm(
        getattr(settings, "AGRO_FONTE_FINANCEIRO", _FONTE_FINANCEIRO_LEGADO),
        _FONTE_FINANCEIRO_LEGADO,
    )


def agro_catalogo_usa_postgres() -> bool:
    return agro_fonte_catalogo() == _FONTE_CATALOGO_AGRO


def agro_pdv_merge_catalogo_postgres() -> bool:
    """PDV busca/cache: com ``AGRO_FONTE_CATALOGO=agro_pg`` liga merge (igual loja). Desligar: ``AGRO_PDV_MERGE_CATALOGO_POSTGRES=false``."""
    if agro_pdv_catalogo_somente_postgres():
        return True
    raw = getattr(settings, "AGRO_PDV_MERGE_CATALOGO_POSTGRES", None)
    if raw is not None:
        return bool(raw)
    return agro_catalogo_usa_postgres()


def agro_pdv_catalogo_somente_postgres() -> bool:
    """PDV catálogo 100 % Postgres (staging após snapshot). **Off** na loja."""
    return bool(getattr(settings, "AGRO_PDV_CATALOGO_SOMENTE_POSTGRES", False))


def agro_gestao_usa_postgres() -> bool:
    """Gestão operacional lista/facetas no Postgres (staging — mesma flag Fase B PDV)."""
    return agro_pdv_catalogo_somente_postgres()


def agro_estoque_ledger_ativo() -> bool:
    """Atalho — implementação em ``produtos.estoque_agro_util``."""
    from produtos.estoque_agro_util import agro_estoque_ledger_ativo as _ativo

    return _ativo()


def agro_estoque_usa_ledger() -> bool:
    return agro_fonte_estoque() == _FONTE_ESTOQUE_LEDGER


def agro_financeiro_usa_postgres() -> bool:
    if agro_fonte_financeiro() == _FONTE_FINANCEIRO_AGRO:
        return True
    # Staging teste: após bootstrap (import na build), CP lê Postgres sem env manual.
    if (
        getattr(settings, "AGRO_ERP_PEDIDOS_DRY_RUN", False)
        and getattr(settings, "AGRO_STAGING_READONLY", False)
        and getattr(settings, "AGRO_FINANCEIRO_PG_LEITURA_STAGING", True)
    ):
        return _staging_financeiro_cp_pg_pronto()
    # Loja: após import CP no Postgres (bootstrap build/boot), liga sem env manual.
    if (
        not getattr(settings, "AGRO_ERP_PEDIDOS_DRY_RUN", False)
        and not getattr(settings, "AGRO_STAGING_READONLY", False)
        and getattr(settings, "AGRO_FINANCEIRO_PG_LOJA_AUTO", True)
    ):
        return _producao_financeiro_cp_pg_pronto()
    return False


def _producao_financeiro_cp_pg_pronto() -> bool:
    try:
        from produtos.models import TituloFinanceiroAgro

        return TituloFinanceiroAgro.objects.filter(despesa=True).exists()
    except Exception:
        return False


def _staging_financeiro_cp_pg_pronto() -> bool:
    try:
        from produtos.models import TituloFinanceiroAgro

        return TituloFinanceiroAgro.objects.filter(despesa=True).exists()
    except Exception:
        return False


def agro_financeiro_erp_sync_env_ligado() -> bool:
    """Variável Render/.env — intenção de enviar lançamento/baixa ao ERP."""
    return bool(getattr(settings, "AGRO_FINANCEIRO_ERP_SYNC_HABILITADO", False))


def agro_financeiro_erp_sync_habilitado() -> bool:
    """Envio Agro → ERP (API). False após checkpoint ou se env desligado."""
    if not agro_financeiro_erp_sync_env_ligado():
        return False
    try:
        from produtos.mongo_financeiro_util import financeiro_checkpoint_ativo

        if financeiro_checkpoint_ativo():
            return False
    except Exception:
        pass
    return True


def agro_cadastro_produto_erp_sync_habilitado() -> bool:
    """Cadastro SisVale ↔ ERP legado (``Produtos/Salvar``). Desligado = só Agro/Mongo local."""
    return bool(getattr(settings, "AGRO_CADASTRO_PRODUTO_ERP_SYNC_HABILITADO", False))


def agro_financeiro_mongo_congelado() -> bool:
    """Após ``congelar_lancamentos_financeiro_agro``: títulos marcados como fonte Agro."""
    return bool(getattr(settings, "AGRO_FINANCEIRO_MONGO_CONGELADO", False))


def agro_erp_pedidos_dry_run() -> bool:
    return bool(getattr(settings, "AGRO_ERP_PEDIDOS_DRY_RUN", False))


def agro_staging_readonly() -> bool:
    from produtos.agro_mongo_guard import agro_mongo_escrita_bloqueada

    return agro_mongo_escrita_bloqueada()


def agro_compras_metricas_postgres() -> bool:
    """Compras: média/sugestão via VendaAgro (Postgres). Default = mesmo gate Fase B/C."""
    raw = getattr(settings, "AGRO_COMPRAS_METRICAS_POSTGRES", None)
    if raw is not None:
        return bool(raw)
    return agro_pdv_catalogo_somente_postgres() or agro_catalogo_usa_postgres()


def agro_fonte_status_dict() -> dict:
    from produtos.agro_mongo_guard import agro_mongo_guard_status

    return {
        "catalogo": agro_fonte_catalogo(),
        "estoque": agro_fonte_estoque(),
        "financeiro": agro_fonte_financeiro(),
        "erp_pedidos_dry_run": agro_erp_pedidos_dry_run(),
        "staging_readonly": agro_staging_readonly(),
        **agro_mongo_guard_status(),
        "catalogo_postgres": agro_catalogo_usa_postgres(),
        "pdv_merge_catalogo_postgres": agro_pdv_merge_catalogo_postgres(),
        "pdv_catalogo_somente_postgres": agro_pdv_catalogo_somente_postgres(),
        "gestao_somente_postgres": agro_gestao_usa_postgres(),
        "compras_metricas_postgres": agro_compras_metricas_postgres(),
        "estoque_ledger": agro_estoque_usa_ledger(),
        "estoque_ledger_ativo": agro_estoque_ledger_ativo(),
        "financeiro_postgres": agro_financeiro_usa_postgres(),
        "financeiro_pg_leitura_staging": bool(
            getattr(settings, "AGRO_FINANCEIRO_PG_LEITURA_STAGING", True)
            and getattr(settings, "AGRO_ERP_PEDIDOS_DRY_RUN", False)
            and getattr(settings, "AGRO_STAGING_READONLY", False)
        ),
        "financeiro_pg_loja_auto": bool(
            getattr(settings, "AGRO_FINANCEIRO_PG_LOJA_AUTO", True)
            and not getattr(settings, "AGRO_ERP_PEDIDOS_DRY_RUN", False)
            and not getattr(settings, "AGRO_STAGING_READONLY", False)
        ),
        "titulos_financeiro_pg": agro_titulos_financeiro_pg_count(),
        "financeiro_erp_sync": agro_financeiro_erp_sync_habilitado(),
        "financeiro_erp_sync_env": agro_financeiro_erp_sync_env_ligado(),
        "cadastro_produto_erp_sync": agro_cadastro_produto_erp_sync_habilitado(),
        "financeiro_mongo_congelado": agro_financeiro_mongo_congelado(),
    }


def agro_titulos_financeiro_pg_count() -> int:
    try:
        from produtos.models import TituloFinanceiroAgro

        return int(TituloFinanceiroAgro.objects.count())
    except Exception:
        return 0
