#!/usr/bin/env bash
# Checkpoint pré-cutover — NFE-SAIDA-55 · 17/10 · v27.26 (sem alterar produção)
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

PROD_REF="origin/producao"
PREP_REF="origin/deploy/prep-checklist-1710-nfe-saida-55"
ROLLBACK_BR="rollback/pre-v2725-nfe-saida-55"
BACKUP_BR="producao-backup-pre-v2726-nfe-saida-55"

git fetch origin producao deploy/prep-checklist-1710-nfe-saida-55 "$ROLLBACK_BR" "$BACKUP_BR" \
  2>/dev/null || git fetch origin producao deploy/prep-checklist-1710-nfe-saida-55

PROD_SHA="$(git rev-parse "$PROD_REF")"
PROD_SHORT="$(git rev-parse --short "$PROD_REF")"
PREP_SHORT="$(git rev-parse --short "$PREP_REF")"
VER="$(git show "$PREP_REF:VERSION" | tr -d '[:space:]')"

echo "checkpoint produção=$PROD_SHORT PREP=$PREP_SHORT VERSION=$VER"

_ensure_ref_matches_prod() {
  local ref_name="$1"
  local remote="origin/$ref_name"
  if git show-ref --verify --quiet "refs/remotes/$remote"; then
    local got
    got="$(git rev-parse "$remote")"
    if [[ "$got" != "$PROD_SHA" ]]; then
      echo "atualizando $ref_name ($got -> $PROD_SHA)"
      git push origin "$PROD_REF:refs/heads/$ref_name"
    else
      echo "OK $ref_name @ $PROD_SHORT"
    fi
  else
    echo "criando $ref_name @ $PROD_SHORT"
    git push origin "$PROD_REF:refs/heads/$ref_name"
  fi
}

_ensure_ref_matches_prod "$ROLLBACK_BR"
_ensure_ref_matches_prod "$BACKUP_BR"

export AGRO_PIN_TESTE="${AGRO_PIN_TESTE:-9973}"
./scripts/cutover_loja_checklist_1710_nfe_saida_55.sh
./scripts/post_deploy_checklist_1710_nfe_saida_55.sh

echo "PREP_CHECKPOINT_OK rollback=$ROLLBACK_BR backup=$BACKUP_BR produção=$PROD_SHORT PREP=$PREP_SHORT"
