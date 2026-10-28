#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
PROD_REF="origin/producao"
PREP_REF="origin/deploy/prep-checklist-2810b-qtd-bip"
ROLLBACK_BR="rollback/pre-v272641-2810b-qtd-bip"
BACKUP_BR="producao-backup-pre-v272641-2810b-qtd-bip"
git fetch origin producao deploy/prep-checklist-2810b-qtd-bip "$ROLLBACK_BR" "$BACKUP_BR" 2>/dev/null \
  || git fetch origin producao deploy/prep-checklist-2810b-qtd-bip
PROD_SHA="$(git rev-parse "$PROD_REF")"
echo "produção=$(git rev-parse --short "$PROD_REF") PREP=$(git rev-parse --short "$PREP_REF") VERSION=$(git show "$PREP_REF:VERSION" | tr -d '[:space:]')"
_ensure() {
  local ref_name="$1"
  if git show-ref --verify --quiet "refs/remotes/origin/${ref_name}"; then
    [[ "$(git rev-parse "origin/${ref_name}")" == "$PROD_SHA" ]] || git push origin "$PROD_REF:refs/heads/${ref_name}"
  else
    git push origin "$PROD_REF:refs/heads/${ref_name}"
  fi
}
_ensure "$ROLLBACK_BR"
_ensure "$BACKUP_BR"
./scripts/cutover_loja_checklist_2810b_qtd_bip.sh
echo "PREP_CHECKPOINT_OK · sem migrate"
