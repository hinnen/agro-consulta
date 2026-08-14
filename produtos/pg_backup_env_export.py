"""Exporta Environment Render/servidor para dentro do ZIP de backup (FL-048)."""
from __future__ import annotations

import json
import os
import re
from typing import Any

from decouple import UndefinedValueError, config
from django.conf import settings
from django.utils import timezone

from config.app_build_util import read_app_version

# Chaves lidas via decouple em settings.py — garante valor mesmo fora do Render.
_KNOWN_CONFIG_KEYS: tuple[str, ...] = (
    "SECRET_KEY",
    "DEBUG",
    "ALLOWED_HOSTS",
    "ALLOWED_HOSTS_EXTRA",
    "CSRF_TRUSTED_ORIGINS",
    "CSRF_TRUSTED_ORIGINS_EXTRA",
    "DATABASE_URL",
    "REDIS_URL",
    "VENDA_ERP_MONGO_URL",
    "VENDA_ERP_MONGO_DB",
    "VENDA_ERP_API_BASE_URL",
    "VENDA_ERP_API_TOKEN",
    "VENDA_ERP_PEDIDO_PLANO_CONTA",
    "VENDA_ERP_PEDIDO_PLANO_CONTA_ID",
    "VENDA_ERP_PEDIDO_STATUS_SISTEMA",
    "AGRO_CANONICAL_ORIGIN",
    "AGRO_FONTE_CATALOGO",
    "AGRO_FONTE_ESTOQUE",
    "AGRO_FONTE_FINANCEIRO",
    "AGRO_FINANCEIRO_ERP_SYNC_HABILITADO",
    "AGRO_FINANCEIRO_MONGO_CONGELADO",
    "AGRO_ERP_PEDIDOS_DRY_RUN",
    "AGRO_STAGING_READONLY",
    "AGRO_SNAPSHOT_FONTE_DATABASE_URL",
    "AGRO_PDV_CATALOGO_SOMENTE_POSTGRES",
    "AGRO_PG_BACKUP_NIGHTLY_ENABLED",
    "AGRO_PG_BACKUP_UPLOAD_MODE",
    "AGRO_PG_BACKUP_WEBHOOK_URL",
    "AGRO_PG_BACKUP_WEBHOOK_TOKEN",
    "AGRO_PG_BACKUP_S3_ENDPOINT",
    "AGRO_PG_BACKUP_S3_BUCKET",
    "AGRO_PG_BACKUP_S3_ACCESS_KEY",
    "AGRO_PG_BACKUP_S3_SECRET_KEY",
    "AGRO_PG_BACKUP_S3_REGION",
    "AGRO_PG_BACKUP_S3_PREFIX",
    "ALERTA_VENDAS_CRON_TOKEN",
    "MP_POINT_ENABLED",
    "MP_POINT_ACCESS_TOKEN",
    "MP_POINT_TERMINAL_ID",
    "MP_POINT_EXPIRATION",
    "MP_POINT_PRINT_ON_TERMINAL",
    "MP_OAUTH_CLIENT_ID",
    "MP_OAUTH_CLIENT_SECRET",
    "NFC_E_ENABLED",
    "NFC_E_TP_AMB",
    "NFC_E_CERT_PATH",
    "NFC_E_CERT_PASSWORD",
    "NFC_E_CERT_BASE64",
    "NFC_E_CNPJ",
    "NFC_E_IE",
    "NFC_E_RAZAO_SOCIAL",
    "NFC_E_FANTASIA",
    "NFC_E_LOGRADOURO",
    "NFC_E_NUMERO",
    "NFC_E_BAIRRO",
    "NFC_E_CMUN",
    "NFC_E_CIDADE",
    "NFC_E_UF",
    "NFC_E_CEP",
    "NFC_E_FONE",
    "NFC_E_CSC_ID",
    "NFC_E_CSC_TOKEN",
    "NFC_E_SERIE",
    "NFC_E_PROXIMO_NUMERO",
    "NFC_E_MODO",
    "NFC_E_VILA_CNPJ",
    "NFC_E_VILA_IE",
    "NFC_E_VILA_FANTASIA",
    "NFC_E_VILA_LOGRADOURO",
    "NFC_E_VILA_NUMERO",
    "NFC_E_VILA_BAIRRO",
    "NFC_E_VILA_CEP",
    "NFC_E_VILA_SERIE",
    "NFC_E_VILA_PROXIMO_NUMERO",
    "NFC_E_IBPT_TOKEN",
    "NFC_E_IBPT_CNPJ",
    "NFC_E_IBPT_ALIQ_NAC",
    "NFC_E_IBPT_ALIQ_EST",
    "NFC_E_IBPT_ALIQ_MUN",
    "GOOGLE_MAPS_API_KEY",
)

_ENV_PREFIX_RE = re.compile(
    r"^(AGRO_|VENDA_|NFC_|MP_|DATABASE|REDIS|SECRET|ALLOWED|CSRF|DJANGO|ALERTA|"
    r"RENDER|PORT|WEB_|GUNICORN|DRE_|LANCAMENTOS_|FINANCEIRO_|PDV_|TRANSFERENCIA_|"
    r"LOJA_|GOOGLE_|DEBUG|SAVEINCLOUD|PYTHONUNBUFFERED|PYTHON_VERSION)"
)


def _read_config_value(key: str) -> str | None:
    if key in os.environ:
        return os.environ.get(key) or ""
    try:
        return str(config(key))
    except UndefinedValueError:
        return None


def collect_environment_snapshot() -> dict[str, str]:
    """Valores atuais do servidor — inclui secrets (ZIP confidencial)."""
    out: dict[str, str] = {}

    for key, val in os.environ.items():
        if _ENV_PREFIX_RE.match(key):
            out[key] = val

    for key in _KNOWN_CONFIG_KEYS:
        val = _read_config_value(key)
        if val is not None and val != "":
            out[key] = val

    db = settings.DATABASES.get("default", {})
    if db.get("ENGINE", "").endswith("postgresql"):
        if "DATABASE_URL" not in out and db.get("NAME"):
            user = db.get("USER", "")
            password = db.get("PASSWORD", "")
            host = db.get("HOST", "")
            port = db.get("PORT", "5432")
            name = db.get("NAME", "")
            if password:
                out.setdefault(
                    "DATABASE_URL",
                    f"postgres://{user}:{password}@{host}:{port}/{name}",
                )

    return dict(sorted(out.items()))


def _quote_env_value(value: str) -> str:
    if not value:
        return ""
    if any(c in value for c in "\n\r\"' #"):
        escaped = value.replace("\\", "\\\\").replace('"', '\\"')
        return f'"{escaped}"'
    return value


def format_render_env_file(snapshot: dict[str, str] | None = None) -> str:
    snap = snapshot if snapshot is not None else collect_environment_snapshot()
    stamp = timezone.now().strftime("%Y-%m-%d %H:%M:%S %Z")
    ver = read_app_version()
    lines = [
        "# SisVale — export Environment do servidor no momento do backup",
        f"# Gerado em: {stamp}",
        f"# Versão app: v{ver}",
        "# CONFIDENCIAL — contém senhas e tokens. Guarde o ZIP em local seguro.",
        "# No desastre: crie Postgres NOVO no Render e substitua só DATABASE_URL.",
        "# Demais linhas: copiar para Environment do novo serviço.",
        "",
    ]
    for key, val in snap.items():
        lines.append(f"{key}={_quote_env_value(val)}")
    lines.append("")
    return "\n".join(lines)


def format_render_env_json(snapshot: dict[str, str] | None = None) -> str:
    snap = snapshot if snapshot is not None else collect_environment_snapshot()
    payload: dict[str, Any] = {
        "exported_at": timezone.now().isoformat(),
        "version_app": read_app_version(),
        "warning": "Confidencial — não commitar no Git.",
        "variables": snap,
    }
    return json.dumps(payload, ensure_ascii=False, indent=2)
