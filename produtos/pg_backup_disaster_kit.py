"""Kit recuperação zero — ZIP de instruções + env real do servidor (FL-048)."""
from __future__ import annotations

import io
import json
import zipfile
from typing import Any

from config.app_build_util import read_app_version
from produtos.pg_backup_env_export import (
    collect_environment_snapshot,
    format_render_env_file,
    format_render_env_json,
)
from produtos.pg_backup_guia_text import build_guia_painel_text
from produtos.pg_backup_render_checklist import (
    CHECKLIST_REV,
    DISASTER_RECOVERY_STEPS,
    NOTAS_CURTAS,
    RENDER_ENV_ROWS,
    ROLLBACK_NOITE_STEPS,
)

# Ainda fora do ZIP — não são Postgres nem env do Render.
FORA_DO_ZIP: tuple[str, ...] = (
    "Código-fonte Git (branch producao — GitHub hinnen/agro-consulta)",
    "Dados dentro do Mongo ERP (catálogo espelho, financeiro legado) — credenciais vêm no .env",
    "Certificado .pfx em disco local (se não usou NFC_E_CERT_BASE64 no Render)",
)


def _leia_me_zero() -> str:
    ver = read_app_version()
    steps = "\n".join(f"  {s.ordem}. {s.texto}" for s in DISASTER_RECOVERY_STEPS)
    rollback = "\n".join(f"  - {s.texto}" for s in ROLLBACK_NOITE_STEPS)
    notas = "\n".join(f"  - {n}" for n in NOTAS_CURTAS)
    fora = "\n".join(f"  - {s}" for s in FORA_DO_ZIP)
    return f"""SisVale — RECUPERAÇÃO DO ZERO
================================
Versão SisVale: v{ver}
Checklist rev.: {CHECKLIST_REV}

CENÁRIO: sumiu Render, sumiu PC — só tem o ZIP de backup.

UM ZIP COMPLETO BASTA
  - Dados Postgres (data/*.jsonl)
  - Pasta kit/ com guias + render-env-atual.env (senhas reais do servidor)

ABRA PRIMEIRO (pasta kit/)
  - GUIA-BACKUP-PAINEL.txt — espelho do painel /interno/pg-backup/
  - render-env-atual.env — cole no Environment do novo Render (troque DATABASE_URL)
  - LEIA-ME-RECUPERACAO-ZERO.txt — este arquivo

AINDA FORA DO ZIP
{fora}

PASSO A PASSO — SERVIDOR NOVO
{steps}

ROLLBACK SÓ DADOS (loja fechada)
{rollback}

NOTAS
{notas}
"""


def _ps1_recuperacao() -> str:
    return r"""# SisVale — auxiliar recuperação (PowerShell)

Write-Host "=== SisVale recuperação zero ===" -ForegroundColor Cyan
Write-Host "1. Abra o ZIP de backup completo"
Write-Host "2. kit/render-env-atual.env -> Environment do novo Render (troque DATABASE_URL)"
Write-Host "3. kit/GUIA-BACKUP-PAINEL.txt -> passo a passo completo"
Write-Host "4. Deploy branch producao -> migrate -> superuser -> Restore ZIP dados"
Write-Host ""
Write-Host "Repo: https://github.com/hinnen/agro-consulta"
"""


def disaster_kit_file_map() -> dict[str, bytes]:
    """Arquivos do kit (embutidos em todo ZIP de backup)."""
    env_snap = collect_environment_snapshot()
    checklist_json = {
        "checklist_rev": CHECKLIST_REV,
        "version_app": read_app_version(),
        "disaster_steps": [s.texto for s in DISASTER_RECOVERY_STEPS],
        "rollback_steps": [s.texto for s in ROLLBACK_NOITE_STEPS],
        "fora_do_zip": list(FORA_DO_ZIP),
        "render_env": [
            {
                "key": r.key,
                "loja": r.loja,
                "teste": r.teste,
                "novo_servidor": r.novo_servidor,
            }
            for r in RENDER_ENV_ROWS
        ],
        "environment_snapshot_keys": sorted(env_snap.keys()),
    }
    return {
        "GUIA-BACKUP-PAINEL.txt": build_guia_painel_text().encode("utf-8"),
        "LEIA-ME-RECUPERACAO-ZERO.txt": _leia_me_zero().encode("utf-8"),
        "render-env-atual.env": format_render_env_file(env_snap).encode("utf-8"),
        "render-env-atual.json": format_render_env_json(env_snap).encode("utf-8"),
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
