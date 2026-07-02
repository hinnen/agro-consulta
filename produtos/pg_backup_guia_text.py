"""Texto do guia do painel backup — espelho offline para o ZIP (FL-048)."""
from __future__ import annotations

from django.conf import settings

from produtos.pg_backup_render_checklist import (
    CHECKLIST_REV,
    DISASTER_RECOVERY_STEPS,
    NOTAS_CURTAS,
    RENDER_ENV_ROWS,
    ROLLBACK_NOITE_STEPS,
)
from produtos.pg_backup_upload import upload_status_resumo


def build_guia_painel_text() -> str:
    nightly = getattr(settings, "AGRO_PG_BACKUP_NIGHTLY_ENABLED", False)
    upload = upload_status_resumo()

    disaster = "\n".join(f"  {s.ordem}. {s.texto}" for s in DISASTER_RECOVERY_STEPS)
    rollback = "\n".join(f"  - {s.texto}" for s in ROLLBACK_NOITE_STEPS)
    notas = "\n".join(f"  - {n}" for n in NOTAS_CURTAS)
    env_table = "\n".join(
        f"  {r.key}\n    loja: {r.loja}\n    teste: {r.teste}\n    novo: {r.novo_servidor}\n"
        for r in RENDER_ENV_ROWS
    )

    nightly_line = (
        f"Backup automático: ATIVO · upload modo {upload.get('mode', 'none')}"
        + (" (configurado)" if upload.get("configured") else " (sem destino)")
        if nightly
        else "Backup automático: desligado neste servidor."
    )

    return f"""SisVale — GUIA DO PAINEL BACKUP (offline)
==========================================
Checklist rev.: {CHECKLIST_REV}
Este arquivo espelha o painel /interno/pg-backup/ — use sem acesso ao site.

ROTINA — SÓ BAIXAR E GUARDAR
  1. Marque todas as categorias → «Baixar ZIP» (backup completo).
  2. Um ZIP basta: dados Postgres + pasta kit/ com tudo abaixo.
  3. Guarde cópias no PC e nuvem (OneDrive etc.). ZIP contém SENHAS.

O QUE VEM DENTRO DO ZIP (pasta kit/)
  - GUIA-BACKUP-PAINEL.txt (este arquivo)
  - LEIA-ME-RECUPERACAO-ZERO.txt
  - render-env-atual.env — Environment REAL do servidor (senhas, Mongo, NFC, MP…)
  - render-env-atual.json — mesmo conteúdo em JSON
  - render-env-checklist.json — tabela de referência
  - scripts/recuperar_producao_zero.ps1
  - GIT-DEPLOY.txt

O QUE AINDA NÃO VEM NO ZIP (fora do Postgres Agro)
  - Código-fonte Git (branch producao no GitHub)
  - Conteúdo do banco Mongo ERP (só as credenciais vêm no .env)
  - Certificado .pfx em arquivo — se usou path local; no Render vem NFC_E_CERT_BASE64

BACKUP PARCIAL POR CATEGORIA
  Desmarque categorias → ZIP menor. Restore: marque só a categoria → só ela é substituída.
  Código novo pode ficar; rollback de código = deploy git separado.

RESTORE (precisa de um servidor Django funcionando)
  Frase: RESTAURAR BACKUP PG + senha admin superuser.
  Desastre: novo Render → deploy código → colar kit/render-env-atual.env (trocar DATABASE_URL) → migrate → superuser → restore ZIP dados.

DESASTRE — SERVIDOR NOVO
{disaster}

ROLLBACK DE NOITE (dados)
{rollback}

TABELA VARIÁVEIS RENDER (referência)
{env_table}

{nightly_line}

NOTAS
{notas}
"""
