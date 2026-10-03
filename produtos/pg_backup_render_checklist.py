"""
Checklist Render / disaster recovery — fonte única para painel FL-048 e banana.md.

Atualizar este arquivo quando mudar env de produção ou staging (espelhar .env.example / docs/DEPLOY-AMBIENTES.md).
"""
from __future__ import annotations

from dataclasses import dataclass

CHECKLIST_REV = "2026-06-30"


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
        "Novo Postgres vazio → trocar só esta linha no .env exportado",
    ),
    RenderEnvRow(
        "VENDA_ERP_MONGO_URL + VENDA_ERP_MONGO_DB",
        "Mongo espelho ERP",
        "Mesmo Mongo (leitura)",
        "Copiar do render-env-atual.env no kit/",
    ),
    RenderEnvRow(
        "AGRO_STAGING_READONLY",
        "false ou omitir",
        "true",
        "false (produção) · true só se for homolog",
    ),
    RenderEnvRow(
        "AGRO_ERP_PEDIDOS_DRY_RUN",
        "false",
        "true",
        "false em produção",
    ),
    RenderEnvRow(
        "AGRO_FONTE_CATALOGO",
        "agro_pg (loja)",
        "agro_pg",
        "Copiar do kit/render-env-atual.env",
    ),
    RenderEnvRow(
        "AGRO_FONTE_ESTOQUE",
        "ledger (loja)",
        "ledger",
        "Copiar do kit/render-env-atual.env",
    ),
    RenderEnvRow(
        "AGRO_FONTE_FINANCEIRO / financeiro PG",
        "agro_pg na loja",
        "Conferir fonte-status",
        "Copiar do kit/render-env-atual.env",
    ),
    RenderEnvRow(
        "AGRO_PDV_CATALOGO_SOMENTE_POSTGRES",
        "Conferir fonte-status loja",
        "true após snapshot ou restore",
        "Copiar do kit/render-env-atual.env",
    ),
    RenderEnvRow(
        "REDIS_URL",
        "Se usar no Render loja",
        "Staging próprio",
        "Copiar do kit ou omitir",
    ),
    RenderEnvRow(
        "NFC_E_* / MP_POINT_*",
        "Certificado / token produção",
        "Homolog ou cópia",
        "No kit/render-env-atual.env",
    ),
    RenderEnvRow(
        "SECRET_KEY / ALLOWED_HOSTS",
        "Render loja",
        "Render teste",
        "No kit/render-env-atual.env",
    ),
)

DISASTER_RECOVERY_STEPS: tuple[DisasterStep, ...] = (
    DisasterStep(1, "Tenha o ZIP backup completo guardado (dados + pasta kit/ com env real e guias)"),
    DisasterStep(2, "Novo Render/host · Postgres vazio · deploy branch producao (versão = manifest version_app)"),
    DisasterStep(3, "Environment: colar kit/render-env-atual.env — trocar só DATABASE_URL pelo Postgres novo"),
    DisasterStep(4, "migrate · createsuperuser · Admin → Restore ZIP dados"),
    DisasterStep(5, "fonte-status · venda teste · CP amostra"),
)

ROLLBACK_NOITE_STEPS: tuple[DisasterStep, ...] = (
    DisasterStep(1, "Antes de mudança: backup geral na loja"),
    DisasterStep(2, "Deu ruim nos DADOS: Restore ZIP (loja fechada)"),
    DisasterStep(3, "Deu ruim no CÓDIGO: deploy versão antiga (git producao / tag banana) — restore não troca código"),
)

NOTAS_CURTAS: tuple[str, ...] = (
    "Backup ZIP = dados Postgres + pasta kit/ (guias + render-env-atual.env com secrets reais).",
    "Um ZIP completo basta para desastre — não precisa acessar /interno/pg-backup/ depois.",
    "ZIP é CONFIDENCIAL (senhas). Guarde em local seguro / nuvem criptografada.",
    "Fora do ZIP: código Git (GitHub) e dados dentro do Mongo ERP (credenciais vêm no .env).",
    "Restore parcial: marque categorias + envie ZIP — só as marcadas são substituídas.",
    "Rollback de código: git/deploy separado do restore.",
    "Backup noturno (cron): AGRO_PG_BACKUP_NIGHTLY_ENABLED + upload webhook ou S3.",
    "Conferência: GET /api/agro/fonte-status/",
)
