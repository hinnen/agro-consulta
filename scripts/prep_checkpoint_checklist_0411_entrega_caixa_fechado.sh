#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
PROD_REF="origin/producao"
PREP_REF="origin/deploy/prep-checklist-0411-entrega-caixa-fechado"
ROLLBACK_BR="rollback/pre-v272650-0411-entrega-caixa-fechado"
BACKUP_BR="producao-backup-pre-v272650-0411-entrega-caixa-fechado"
git fetch origin producao
PROD_SHA="$(git rev-parse "$PROD_REF")"
echo "produção=$(git rev-parse --short "$PROD_REF") VERSION=$(git show "$PROD_REF:VERSION" | tr -d '[:space:]')"
git push origin "$PROD_REF:refs/heads/$ROLLBACK_BR"
git push origin "$PROD_REF:refs/heads/$BACKUP_BR"
git fetch origin "$ROLLBACK_BR" "$BACKUP_BR"
[[ "$(git rev-parse "origin/$ROLLBACK_BR")" == "$PROD_SHA" ]]
[[ "$(git rev-parse "origin/$BACKUP_BR")" == "$PROD_SHA" ]]
echo "CHECKPOINT_OK $ROLLBACK_BR = $(git rev-parse --short "$PROD_SHA")"
if git show-ref --verify --quiet "refs/remotes/origin/deploy/prep-checklist-0411-entrega-caixa-fechado"; then
  ./scripts/cutover_loja_checklist_0411_entrega_caixa_fechado.sh
fi
