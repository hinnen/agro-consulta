#!/usr/bin/env bash
# Cutover loja — CHECKLIST 23/10b · PDV-RAPIDOS Home · v27.26.23 · sem migrate nova
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
PREP_REF="origin/deploy/prep-checklist-2310b-pdv-rapidos-home"
PROD_REF="origin/producao"
ROLLBACK_TAG="rollback/pre-v272623-2310b-pdv-rapidos-home"
BACKUP_BR="producao-backup-pre-v272623-2310b-pdv-rapidos-home"
SENHA_OK="99738595"
VER_ESPERADA="27.26.23"

git fetch origin producao deploy/prep-checklist-2310b-pdv-rapidos-home "$ROLLBACK_TAG" 2>/dev/null \
  || git fetch origin producao deploy/prep-checklist-2310b-pdv-rapidos-home

PROD="$(git rev-parse --short "$PROD_REF")"
PREP="$(git rev-parse --short "$PREP_REF" 2>/dev/null || echo missing)"
VER="$(git show "$PREP_REF:VERSION" 2>/dev/null | tr -d '[:space:]' || echo ?)"
echo "producao=$PROD PREP=$PREP VERSION=$VER"

git merge-base --is-ancestor "$PROD_REF" "$PREP_REF" || { echo "FAIL: PREP não é descendente de produção"; exit 2; }
[[ "$VER" == "$VER_ESPERADA" ]] || { echo "FAIL: VERSION=$VER esperado $VER_ESPERADA"; exit 2; }

DELTA="$(git diff --name-only "$PROD_REF...$PREP_REF" | sort)"
ALLOWED="$(printf '%s\n' \
  VERSION \
  docs/DEPLOY-PREP-CHECKLIST-2310b-pdv-rapidos-home.md \
  produtos/pdv_topbar_layout_util.py \
  produtos/static/produtos/js/pdv_rapidos.js \
  produtos/templates/produtos/partials/pdv/step_produtos.html \
  produtos/templates/produtos/pdv_wizard.html \
  produtos/tests_pdv_rapidos.py \
  scripts/cutover_loja_checklist_2310b_pdv_rapidos_home.sh \
  scripts/post_deploy_checklist_2310b_pdv_rapidos_home.sh \
  scripts/prep_checkpoint_checklist_2310b_pdv_rapidos_home.sh \
  scripts/verify_pdv_rapidos_path.py | sort)"
[[ "$DELTA" == "$ALLOWED" ]] || { echo "FAIL: delta:"; echo "$DELTA"; exit 2; }

MIG="$(echo "$DELTA" | grep -E '/migrations/[0-9]+_' || true)"
[[ -z "$MIG" ]] || { echo "FAIL: migrate inesperada: $MIG"; exit 2; }

CAIXA="$(echo "$DELTA" | grep -Ei 'caixa_fechar|caixa_util|checkout' || true)"
[[ -z "$CAIXA" ]] || { echo "FAIL: path caixa/checkout: $CAIXA"; exit 2; }

PROD_SHA="$(git rev-parse "$PROD_REF")"
git show-ref --verify --quiet "refs/remotes/origin/$ROLLBACK_TAG" || {
  echo "FAIL: rode ./scripts/prep_checkpoint_checklist_2310b_pdv_rapidos_home.sh"; exit 2;
}
[[ "$(git rev-parse origin/$ROLLBACK_TAG)" == "$PROD_SHA" ]] || {
  echo "FAIL: rollback desatualizado"; exit 2;
}

PY=python3
[[ -x .venv/bin/python ]] && PY=.venv/bin/python
"$PY" scripts/verify_pdv_rapidos_path.py | tee /tmp/cutover-2310b-pdv-rapidos.log
grep -q VERIFY_OK /tmp/cutover-2310b-pdv-rapidos.log

echo "OK: PREP v$VER_ESPERADA · 23/10b PDV-RAPIDOS Home · sem migrate · rollback $ROLLBACK_TAG"

[[ "${1:-}" != "--exec" ]] && exit 0
FRASE="${AGRO_PIN_FRASE:-${AGRO_LOJA_FRASE:-}}"
SENHA="${AGRO_LOJA_SENHA:-}"
[[ "$SENHA" == "$SENHA_OK" ]] || { echo "FAIL: senha"; exit 3; }
echo "$FRASE" | grep -Eiq 'pode.*(produ[cç][aã]o|producao|enviar)|sobe.*(produ[cç][aã]o|producao)|subir.*(produ[cç][aã]o|producao)|enviar.*(produ[cç][aã]o|producao)' \
  || { echo "FAIL: autorização"; exit 3; }
git push origin "$PROD_REF:refs/heads/$ROLLBACK_TAG" || true
git push origin "$PROD_REF:refs/heads/$BACKUP_BR" || true
git push origin "$PREP_REF:producao"
echo "PUSH OK v$VER_ESPERADA"
