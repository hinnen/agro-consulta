#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
PROD_REF="origin/producao"
PREP_REF="origin/deploy/prep-checklist-2010c-rel-grupo-colunas"
ROLLBACK_BR="rollback/pre-v272616-2010c-rel-grupo-colunas"
BACKUP_BR="producao-backup-pre-v272616-2010c-rel-grupo-colunas"
git fetch origin producao deploy/prep-checklist-2010c-rel-grupo-colunas "$ROLLBACK_BR" "$BACKUP_BR" 2>/dev/null \
  || git fetch origin producao deploy/prep-checklist-2010c-rel-grupo-colunas
PROD_SHA="$(git rev-parse "$PROD_REF")"
PROD_SHORT="$(git rev-parse --short "$PROD_REF")"
PREP_SHORT="$(git rev-parse --short "$PREP_REF" 2>/dev/null || echo missing)"
VER="$(git show "$PREP_REF:VERSION" 2>/dev/null | tr -d '[:space:]' || echo ?)"
echo "produção=$PROD_SHORT PREP=$PREP_SHORT VERSION=$VER"
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
export AGRO_PIN_TESTE="${AGRO_PIN_TESTE:-9973}"
./scripts/cutover_loja_checklist_2010c_rel_grupo_colunas.sh
echo "PREP_CHECKPOINT_OK · cutover --exec na senha"
