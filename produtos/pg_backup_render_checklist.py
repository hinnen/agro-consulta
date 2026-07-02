"""
Checklist Render / disaster recovery — fonte única para painel FL-048 e banana.md.

Atualizar este arquivo quando mudar env de produção ou staging (espelhar .env.example / docs/DEPLOY-AMBIENTES.md).
"""
from __future__ import annotations

from dataclasses import dataclass

CHECKLIST_REV = "2026-07-03"


@dataclass(frozen=True)
class RenderEnvRow:
    key: str
    loja: str
    teste: str
    novo_servidor: str
    no_backup_pg: bool = False


@dataclass(frozen=True)
class DisasterStep:
    ordem: int
    texto: str


RENDER_ENV_ROWS: tuple[RenderEnvRow, ...] = (
    RenderEnvRow(
        "DATABASE_URL",
        "Postgres loja (Render agro-db)",
        "Postgres staging (outro)",
        "Novo Postgres vazio → migrate → restore ZIP",
        no_backup_pg=True,
    ),
    RenderEnvRow(
        "VENDA_ERP_MONGO_URL + VENDA_ERP_MONGO_DB",
        "Mongo espelho ERP",
        "Mesmo Mongo (leitura)",
        "Mesmas credenciais Mongo",
        no_backup_pg=True,
    ),
    RenderEnvRow(
        "AGRO_STAGING_READONLY",
        "false ou omitir",
        "true",
        "false (produção) · true só se for homolog",
        no_backup_pg=True,
    ),
    RenderEnvRow(
        "AGRO_ERP_PEDIDOS_DRY_RUN",
        "false",
        "true",
        "false em produção",
        no_backup_pg=True,
    ),
    RenderEnvRow(
        "AGRO_FONTE_CATALOGO",
        "agro_pg (loja)",
        "agro_pg",
        "Igual loja",
        no_backup_pg=True,
    ),
    RenderEnvRow(
        "AGRO_FONTE_ESTOQUE",
        "ledger (loja)",
        "ledger",
        "Igual loja",
        no_backup_pg=True,
    ),
    RenderEnvRow(
        "AGRO_FONTE_FINANCEIRO / financeiro PG",
        "agro_pg na loja",
        "Conferir fonte-status",
        "Igual loja",
        no_backup_pg=True,
    ),
    RenderEnvRow(
        "AGRO_PDV_CATALOGO_SOMENTE_POSTGRES",
        "Conferir fonte-status loja",
        "true após snapshot ou restore",
        "Igual loja pós-restore",
        no_backup_pg=True,
    ),
    RenderEnvRow(
        "REDIS_URL",
        "Se usar no Render loja",
        "Staging próprio",
        "Opcional — cache",
        no_backup_pg=True,
    ),
    RenderEnvRow(
        "NFC_E_* / MP_POINT_*",
        "Certificado / token produção",
        "Homolog ou cópia",
        "Recadastrar no painel Render",
        no_backup_pg=True,
    ),
    RenderEnvRow(
        "SECRET_KEY / ALLOWED_HOSTS",
        "Render loja",
        "Render teste",
        "Novo serviço",
        no_backup_pg=True,
    ),
)

DISASTER_RECOVERY_STEPS: tuple[DisasterStep, ...] = (
    DisasterStep(1, "Guardar no PC: ZIP dados (todas categorias) + kit recuperação + export .env do Render"),
    DisasterStep(2, "Novo Render/host · Postgres vazio · deploy branch producao (versão = manifest version_app)"),
    DisasterStep(3, "Environment: render-env-modelo.env preenchido (Mongo, NFC, MP, SECRET_KEY…)"),
    DisasterStep(4, "migrate · createsuperuser · Admin → Restore ZIP dados"),
    DisasterStep(5, "fonte-status · venda teste · CP amostra"),
)

ROLLBACK_NOITE_STEPS: tuple[DisasterStep, ...] = (
    DisasterStep(1, "Antes de mudança: backup geral na loja"),
    DisasterStep(2, "Deu ruim nos DADOS: Restore ZIP (loja fechada)"),
    DisasterStep(3, "Deu ruim no CÓDIGO: deploy versão antiga (git producao / tag banana) — restore não troca código"),
)

NOTAS_CURTAS: tuple[str, ...] = (
    "Backup = só Postgres Agro. Mongo e .env não entram no ZIP.",
    "Dois bancos: após restore cada um segue seu rumo.",
    "Rollback de código: git/deploy separado do restore.",
    "Conferência: GET /api/agro/fonte-status/",
)
