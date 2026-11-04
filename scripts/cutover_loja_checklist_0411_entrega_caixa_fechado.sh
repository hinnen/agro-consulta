#!/usr/bin/env bash
# Cutover 04/11 · entrega a pagar depois do caixa fechar · v27.26.50 · sem migrate
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
PREP_REF="origin/deploy/prep-checklist-0411-entrega-caixa-fechado"
PROD_REF="origin/producao"
ROLLBACK_TAG="rollback/pre-v272650-0411-entrega-caixa-fechado"
BACKUP_BR="producao-backup-pre-v272650-0411-entrega-caixa-fechado"
SENHA_OK="99738595"
VER_ESPERADA="27.26.50"

git fetch origin producao deploy/prep-checklist-0411-entrega-caixa-fechado "$ROLLBACK_TAG" "$BACKUP_BR"

PROD="$(git rev-parse --short "$PROD_REF")"
PREP="$(git rev-parse --short "$PREP_REF" 2>/dev/null || echo missing)"
VER="$(git show "$PREP_REF:VERSION" 2>/dev/null | tr -d '[:space:]' || echo ?)"
echo "producao=$PROD PREP=$PREP VERSION=$VER"
git merge-base --is-ancestor "$PROD_REF" "$PREP_REF" || { echo "FAIL: ancestralidade"; exit 2; }
[[ "$VER" == "$VER_ESPERADA" ]] || { echo "FAIL: VERSION=$VER"; exit 2; }

DELTA="$(git diff --name-only "$PROD_REF...$PREP_REF" | sort)"
ALLOWED="$(printf '%s\n' \
  VERSION \
  docs/DEPLOY-PREP-CHECKLIST-0411-entrega-caixa-fechado.md \
  produtos/entrega_pdv_pendente_util.py \
  produtos/tests_entrega_pdv_overlay.py \
  produtos/views.py \
  scripts/cutover_loja_checklist_0411_entrega_caixa_fechado.sh \
  scripts/post_deploy_checklist_0411_entrega_caixa_fechado.sh \
  scripts/prep_checkpoint_checklist_0411_entrega_caixa_fechado.sh \
  scripts/verify_pdv_entrega_caixa_fechado_path.py | sort)"
[[ "$DELTA" == "$ALLOWED" ]] || { echo "FAIL: delta:"; echo "$DELTA"; exit 2; }

MIG="$(echo "$DELTA" | grep -E '/migrations/[0-9]+_' || true)"
[[ -z "$MIG" ]] || { echo "FAIL: migrate $MIG"; exit 2; }
CAIXA="$(echo "$DELTA" | grep -Ei 'caixa_fechar|caixa_util|checkout|pdv_wizard|views_nfe' || true)"
[[ -z "$CAIXA" ]] || { echo "FAIL: pdv/caixa $CAIXA"; exit 2; }

PROD_SHA="$(git rev-parse "$PROD_REF")"
git show-ref --verify --quiet "refs/remotes/origin/$ROLLBACK_TAG" || { echo "FAIL: checkpoint"; exit 2; }
[[ "$(git rev-parse "origin/$ROLLBACK_TAG")" == "$PROD_SHA" ]] || { echo "FAIL: rollback"; exit 2; }
[[ "$(git rev-parse "origin/$BACKUP_BR")" == "$PROD_SHA" ]] || { echo "FAIL: backup"; exit 2; }

PY=python3
[[ -x .venv/bin/python ]] && PY=.venv/bin/python
"$PY" scripts/verify_pdv_entrega_caixa_fechado_path.py | tee /tmp/cutover-0411-entrega-caixa.log
grep -q 'VERIFY_OK 9' /tmp/cutover-0411-entrega-caixa.log
echo "OK: PREP v$VER_ESPERADA · entrega a pagar com caixa fechado · sem migrate · rollback $ROLLBACK_TAG @ $PROD"

[[ "${1:-}" != "--exec" ]] && exit 0
FRASE="${AGRO_PIN_FRASE:-${AGRO_LOJA_FRASE:-}}"
SENHA="${AGRO_LOJA_SENHA:-}"
[[ "$SENHA" == "$SENHA_OK" ]] || { echo "FAIL: senha"; exit 3; }
echo "$FRASE" | grep -Eiq 'pode.*(produ[cç][aã]o|producao|enviar)|sobe.*(produ[cç][aã]o|producao)|subir.*(produ[cç][aã]o|producao)|enviar.*(produ[cç][aã]o|producao)' \
  || { echo "FAIL: autorização"; exit 3; }
git push origin "$PREP_REF:producao"
echo "PUSH OK v$VER_ESPERADA"
echo "REVERT: git push origin $ROLLBACK_TAG:producao"
