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


def agro_estoque_usa_ledger() -> bool:
    return agro_fonte_estoque() == _FONTE_ESTOQUE_LEDGER


def agro_financeiro_usa_postgres() -> bool:
    return agro_fonte_financeiro() == _FONTE_FINANCEIRO_AGRO


def agro_financeiro_erp_sync_habilitado() -> bool:
    """Envio Agro → ERP (lançamento/baixa via API). Desligado = só Mongo."""
    return bool(getattr(settings, "AGRO_FINANCEIRO_ERP_SYNC_HABILITADO", False))


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
        "estoque_ledger": agro_estoque_usa_ledger(),
        "financeiro_postgres": agro_financeiro_usa_postgres(),
        "financeiro_erp_sync": agro_financeiro_erp_sync_habilitado(),
        "cadastro_produto_erp_sync": agro_cadastro_produto_erp_sync_habilitado(),
        "financeiro_mongo_congelado": agro_financeiro_mongo_congelado(),
    }
