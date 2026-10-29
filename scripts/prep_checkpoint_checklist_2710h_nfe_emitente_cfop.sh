#!/usr/bin/env bash
# Checkpoint pré-cutover — CHECKLIST 27/10h · NFE-EMITENTE-CFOP · v27.26.39
# Rollback = loja ANTES deste pacote (97d4b22c / v27.26.38). Não sobrescreve com produção já hotfix.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
PREP_REF="origin/deploy/prep-checklist-2710h-nfe-emitente-cfop"
ROLLBACK_BR="rollback/pre-v272639-nfe-emitente-cfop"
BACKUP_BR="producao-backup-pre-v272639-nfe-emitente-cfop"
BASE_LOJA="${NFE_EMITENTE_BASE:-97d4b22c}"
git fetch origin producao deploy/prep-checklist-2710h-nfe-emitente-cfop "$ROLLBACK_BR" "$BACKUP_BR" \
  2>/dev/null || git fetch origin producao
BASE_SHA="$(git rev-parse "${BASE_LOJA}^{commit}")"
echo "BASE_LOJA=$(git rev-parse --short "$BASE_SHA") producao=$(git rev-parse --short origin/producao) PREP=$(git rev-parse --short "$PREP_REF" 2>/dev/null || echo missing)"
_ensure_at() {
  local ref_name="$1"
  local sha="$2"
  if git show-ref --verify --quiet "refs/remotes/origin/${ref_name}"; then
    [[ "$(git rev-parse "origin/${ref_name}")" == "$sha" ]] || git push origin "${sha}:refs/heads/${ref_name}"
  else
    git push origin "${sha}:refs/heads/${ref_name}"
  fi
}
_ensure_at "$ROLLBACK_BR" "$BASE_SHA"
_ensure_at "$BACKUP_BR" "$BASE_SHA"
export AGRO_PIN_TESTE="${AGRO_PIN_TESTE:-9973}"
./scripts/cutover_loja_checklist_2710h_nfe_emitente_cfop.sh
echo "PREP_CHECKPOINT_OK rollback=$ROLLBACK_BR @ $(git rev-parse --short "$BASE_SHA") · sem migrate"
echo "Revert loja: git push origin $ROLLBACK_BR:producao"
echo "Cutover (só com senha no chat): ./scripts/cutover_loja_checklist_2710h_nfe_emitente_cfop.sh --exec"
