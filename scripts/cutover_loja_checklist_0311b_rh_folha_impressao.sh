#!/usr/bin/env bash
# Cutover 03/11b · impressão + Excel da folha · v27.26.47 · sem migrate
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
PREP_REF="origin/deploy/prep-checklist-0311b-rh-folha-impressao"
PROD_REF="origin/producao"
ROLLBACK_TAG="rollback/pre-v272647-0311b-rh-folha-impressao"
BACKUP_BR="producao-backup-pre-v272647-0311b-rh-folha-impressao"
SENHA_OK="99738595"
VER_ESPERADA="27.26.47"

git fetch origin producao deploy/prep-checklist-0311b-rh-folha-impressao "$ROLLBACK_TAG"

PROD="$(git rev-parse --short "$PROD_REF")"
PREP="$(git rev-parse --short "$PREP_REF" 2>/dev/null || echo missing)"
VER="$(git show "$PREP_REF:VERSION" 2>/dev/null | tr -d '[:space:]' || echo ?)"
echo "producao=$PROD PREP=$PREP VERSION=$VER"
git merge-base --is-ancestor "$PROD_REF" "$PREP_REF" || { echo "FAIL: ancestralidade"; exit 2; }
[[ "$VER" == "$VER_ESPERADA" ]] || { echo "FAIL: VERSION=$VER"; exit 2; }

DELTA="$(git diff --name-only "$PROD_REF...$PREP_REF" | sort)"
ALLOWED="$(printf '%s\n' \
  VERSION \
  rh/services/folha_export.py \
  rh/templates/rh/fechamento_detalhe.html \
  rh/templates/rh/fechamento_imprimir.html \
  rh/templates/rh/fechamentos_lista.html \
  rh/urls.py \
  rh/views.py \
  scripts/cutover_loja_checklist_0311b_rh_folha_impressao.sh \
  scripts/post_deploy_checklist_0311b_rh_folha_impressao.sh \
  scripts/verify_rh_folha_impressao_excel_path.py | sort)"
[[ "$DELTA" == "$ALLOWED" ]] || { echo "FAIL: delta:"; echo "$DELTA"; exit 2; }

MIG="$(echo "$DELTA" | grep -E '/migrations/[0-9]+_' || true)"
[[ -z "$MIG" ]] || { echo "FAIL: migrate $MIG"; exit 2; }

PROD_SHA="$(git rev-parse "$PROD_REF")"
git show-ref --verify --quiet "refs/remotes/origin/$ROLLBACK_TAG" || { echo "FAIL: checkpoint"; exit 2; }
[[ "$(git rev-parse "origin/$ROLLBACK_TAG")" == "$PROD_SHA" ]] || { echo "FAIL: rollback"; exit 2; }

PY=python3
[[ -x .venv/bin/python ]] && PY=.venv/bin/python
"$PY" scripts/verify_rh_folha_impressao_excel_path.py | tee /tmp/cutover-0311b-rh.log
grep -q VERIFY_OK /tmp/cutover-0311b-rh.log
echo "OK: PREP v$VER_ESPERADA · impressão + Excel · sem migrate · rollback $ROLLBACK_TAG @ $PROD"

[[ "${1:-}" != "--exec" ]] && exit 0
FRASE="${AGRO_PIN_FRASE:-${AGRO_LOJA_FRASE:-}}"
SENHA="${AGRO_LOJA_SENHA:-}"
[[ "$SENHA" == "$SENHA_OK" ]] || { echo "FAIL: senha"; exit 3; }
echo "$FRASE" | grep -Eiq 'pode.*(produ[cç][aã]o|producao|enviar)|sobe.*(produ[cç][aã]o|producao)|subir.*(produ[cç][aã]o|producao)|enviar.*(produ[cç][aã]o|producao)' \
  || { echo "FAIL: autorização"; exit 3; }
git push origin "$PROD_REF:refs/heads/$ROLLBACK_TAG"
git push origin "$PROD_REF:refs/heads/$BACKUP_BR"
git push origin "$PREP_REF:producao"
echo "PUSH OK v$VER_ESPERADA · Render Shell: ./scripts/post_deploy_checklist_0311b_rh_folha_impressao.sh"
echo "REVERT: git push origin $ROLLBACK_TAG:producao"
