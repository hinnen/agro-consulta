#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
PROD_REF="origin/producao"
PREP_REF="origin/deploy/prep-checklist-1111b-entrada-nf-js"
ROLLBACK_BR="rollback/pre-v272656-1111b-entrada-nf-js"
BACKUP_BR="producao-backup-pre-v272656-1111b-entrada-nf-js"
git fetch origin producao deploy/prep-checklist-1111b-entrada-nf-js "$ROLLBACK_BR" "$BACKUP_BR" 2>/dev/null \
  || git fetch origin producao deploy/prep-checklist-1111b-entrada-nf-js
PROD_SHA="$(git rev-parse "$PROD_REF")"
echo "produção=$(git rev-parse --short "$PROD_REF") PREP=$(git rev-parse --short "$PREP_REF" 2>/dev/null || echo missing)"
_ensure() {
  local ref_name="$1"
  local remote="origin/${ref_name}"
  if git show-ref --verify --quiet "refs/remotes/${remote}"; then
    [[ "$(git rev-parse "${remote}")" == "$PROD_SHA" ]] || git push origin "$PROD_REF:refs/heads/${ref_name}"
  else
    git push origin "$PROD_REF:refs/heads/${ref_name}"
  fi
}
_ensure "$ROLLBACK_BR"
_ensure "$BACKUP_BR"
./scripts/cutover_loja_checklist_1111b_entrada_nf_js.sh
echo "PREP_CHECKPOINT_OK · cutover --exec só com frase+senha · sem migrate"
