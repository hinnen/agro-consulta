"""Kit recuperação zero — ZIP de instruções + modelo env (FL-048)."""
from __future__ import annotations

import io
import json
import zipfile
from typing import Any

from config.app_build_util import read_app_version
from produtos.pg_backup_render_checklist import (
    CHECKLIST_REV,
    DISASTER_RECOVERY_STEPS,
    NOTAS_CURTAS,
    RENDER_ENV_ROWS,
    ROLLBACK_NOITE_STEPS,
)

# Preencher no Render e guardar cópia no PC (fora do ZIP de dados).
SECRETS_GUARDAR_NO_PC: tuple[str, ...] = (
    "Export completo Environment do Render (painel → copiar/colar em arquivo .env)",
    "VENDA_ERP_MONGO_URL e VENDA_ERP_MONGO_DB",
    "DATABASE_URL (só referência — no desastre cria Postgres NOVO)",
    "SECRET_KEY Django",
    "NFC_E_CERT_BASE64 ou arquivo .pfx + senha",
    "MP_POINT_ACCESS_TOKEN e MP_POINT_TERMINAL_ID",
    "Domínio / ALLOWED_HOSTS / AGRO_CANONICAL_ORIGIN",
    "ALERTA_VENDAS_CRON_TOKEN (crons)",
    "Acesso GitHub repositório hinnen/agro-consulta",
)


def _render_env_template() -> str:
    lines = [
        "# Modelo — preencha e cole no Environment do NOVO serviço Render (produção).",
        f"# Checklist rev. {CHECKLIST_REV} · branch deploy: producao",
        "# NÃO commite este arquivo preenchido no Git.",
        "",
    ]
    for row in RENDER_ENV_ROWS:
        key = row.key.split()[0].split("/")[0].split("+")[0].strip()
        if "*" in key:
            lines.append(f"# {row.key}")
            lines.append(f"# {row.novo_servidor}")
            lines.append("")
            continue
        lines.append(f"# Loja ref: {row.loja}")
        lines.append(f"{key}=")
        lines.append("")
    return "\n".join(lines)


def _leia_me_zero() -> str:
    ver = read_app_version()
    steps = "\n".join(f"  {s.ordem}. {s.texto}" for s in DISASTER_RECOVERY_STEPS)
    rollback = "\n".join(f"  - {s.texto}" for s in ROLLBACK_NOITE_STEPS)
    notas = "\n".join(f"  - {n}" for n in NOTAS_CURTAS)
    secrets = "\n".join(f"  - {s}" for s in SECRETS_GUARDAR_NO_PC)
    env_table = "\n".join(
        f"  {r.key}\n    loja: {r.loja}\n    novo: {r.novo_servidor}\n"
        for r in RENDER_ENV_ROWS
    )
    return f"""SisVale — RECUPERAÇÃO DO ZERO
================================
Versão SisVale no momento do kit: v{ver}
Checklist rev.: {CHECKLIST_REV}

CENÁRIO: sumiu Render, sumiu PC, perdeu tudo — só tem este ZIP + backup de DADOS.

VOCÊ PRECISA DE 2 ARQUIVOS (guarde os dois no PC / nuvem):
  1) sistvale-pg-backup-completo-XXXX.zip  (dados Postgres — botão «Baixar ZIP» todas categorias)
  2) sistvale-kit-recuperacao-zero-XXXX.zip  (este kit — instruções e modelo env)

O QUE O BACKUP DE DADOS NÃO TRAZ (você precisa ter salvo antes):
{secrets}

PASSO A PASSO — SERVIDOR NOVO
{steps}

ROLLBACK SÓ DADOS (loja fechada, código novo deu ruim)
{rollback}

VARIÁVEIS RENDER (referência)
{env_table}

DEPLOY CÓDIGO (obrigatório — restore não troca código)
  1. GitHub → branch producao (ou tag rollback no banana.md)
  2. Render → New Web Service → mesmo repo · branch producao
  3. Novo Postgres → copiar DATABASE_URL para Environment
  4. Colar demais env do arquivo render-env-modelo.env (preenchido)
  5. Deploy Live → python manage.py migrate (automático no Render)
  6. Criar superuser: python manage.py createsuperuser (Shell Render)
  7. Admin → /interno/pg-backup/ → Restaurar ZIP de DADOS (todas categorias)
  8. Conferir /api/agro/fonte-status/ · venda teste · CP amostra

MANIFEST DO BACKUP DE DADOS
  Campo version_app no manifest.json = versão SisVale na hora do backup.
  Ideal: deploy no servidor novo a MESMA versão (ou cherry-pick pacote do banana).

NOTAS
{notas}
"""


def _ps1_recuperacao() -> str:
    return r"""# SisVale — auxiliar recuperação (PowerShell)
# Não recria servidor sozinho — siga LEIA-ME-RECUPERACAO-ZERO.txt

Write-Host "=== SisVale recuperação zero ===" -ForegroundColor Cyan
Write-Host "1. Tenha o ZIP de DADOS (backup completo Postgres)"
Write-Host "2. Tenha o .env preenchido (export antigo do Render)"
Write-Host "3. Crie serviço Render + Postgres novo"
Write-Host "4. Deploy branch producao"
Write-Host "5. Admin -> Restaurar ZIP de dados"
Write-Host ""
Write-Host "Repo: https://github.com/hinnen/agro-consulta"
Write-Host "Doc: docs/DEPLOY-AMBIENTES.md"
"""


def disaster_kit_file_map() -> dict[str, bytes]:
    """Arquivos do kit (também embutidos no ZIP de backup completo)."""
    checklist_json = {
        "checklist_rev": CHECKLIST_REV,
        "version_app": read_app_version(),
        "disaster_steps": [s.texto for s in DISASTER_RECOVERY_STEPS],
        "rollback_steps": [s.texto for s in ROLLBACK_NOITE_STEPS],
        "secrets_guardar_no_pc": list(SECRETS_GUARDAR_NO_PC),
        "render_env": [
            {
                "key": r.key,
                "loja": r.loja,
                "teste": r.teste,
                "novo_servidor": r.novo_servidor,
                "no_backup_pg": r.no_backup_pg,
            }
            for r in RENDER_ENV_ROWS
        ],
    }
    return {
        "LEIA-ME-RECUPERACAO-ZERO.txt": _leia_me_zero().encode("utf-8"),
        "render-env-modelo.env": _render_env_template().encode("utf-8"),
        "render-env-checklist.json": json.dumps(checklist_json, ensure_ascii=False, indent=2).encode(
            "utf-8"
        ),
        "scripts/recuperar_producao_zero.ps1": _ps1_recuperacao().encode("utf-8"),
        "GIT-DEPLOY.txt": (
            "branch: producao\n"
            "repo: hinnen/agro-consulta\n"
            "render: render.yaml + Procfile\n"
            "migrate: automático no deploy\n"
            f"version_ref: v{read_app_version()}\n"
        ).encode("utf-8"),
    }


def build_disaster_kit_zip() -> tuple[bytes, str]:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for path, data in disaster_kit_file_map().items():
            zf.writestr(path, data)
    from django.utils import timezone

    stamp = timezone.now().strftime("%Y%m%d-%H%M%S")
    return buf.getvalue(), f"sistvale-kit-recuperacao-zero-{stamp}.zip"


def append_kit_to_zipfile(zf: zipfile.ZipFile) -> None:
    """Inclui kit dentro do ZIP de backup de dados."""
    for path, data in disaster_kit_file_map().items():
        zf.writestr(f"kit/{path}", data)
