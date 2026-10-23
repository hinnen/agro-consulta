#!/usr/bin/env bash
# Cutover loja — CHECKLIST 23/10 · PDV-RAPIDOS · v27.26.22 · migrate 0150+0151
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
PREP_REF="origin/deploy/prep-checklist-2310-pdv-rapidos"
PROD_REF="origin/producao"
ROLLBACK_TAG="rollback/pre-v272622-2310-pdv-rapidos"
BACKUP_BR="producao-backup-pre-v272622-2310-pdv-rapidos"
SENHA_OK="99738595"
VER_ESPERADA="27.26.22"

git fetch origin producao deploy/prep-checklist-2310-pdv-rapidos "$ROLLBACK_TAG" 2>/dev/null \
  || git fetch origin producao deploy/prep-checklist-2310-pdv-rapidos

PROD="$(git rev-parse --short "$PROD_REF")"
PREP="$(git rev-parse --short "$PREP_REF" 2>/dev/null || echo missing)"
VER="$(git show "$PREP_REF:VERSION" 2>/dev/null | tr -d '[:space:]' || echo ?)"
echo "producao=$PROD PREP=$PREP VERSION=$VER"

git merge-base --is-ancestor "$PROD_REF" "$PREP_REF" || { echo "FAIL: PREP não é descendente de produção"; exit 2; }
[[ "$VER" == "$VER_ESPERADA" ]] || { echo "FAIL: VERSION=$VER esperado $VER_ESPERADA"; exit 2; }

DELTA="$(git diff --name-only "$PROD_REF...$PREP_REF" | sort)"
ALLOWED="$(printf '%s\n' \
  VERSION \
  docs/DEPLOY-PREP-CHECKLIST-2310-pdv-rapidos.md \
  produtos/migrations/0150_pdv_rapido_slot.py \
  produtos/migrations/0151_pdv_rapido_layout.py \
  produtos/models.py \
  produtos/pdv_rapidos_util.py \
  produtos/pdv_topbar_layout_util.py \
  produtos/static/produtos/js/pdv_rapidos.js \
  produtos/static/produtos/js/pdv_wizard.js \
  produtos/templates/produtos/pdv_wizard.html \
  produtos/tests_pdv_rapidos.py \
  produtos/urls.py \
  produtos/views_pdv_rapidos.py \
  scripts/cutover_loja_checklist_2310_pdv_rapidos.sh \
  scripts/post_deploy_checklist_2310_pdv_rapidos.sh \
  scripts/prep_checkpoint_checklist_2310_pdv_rapidos.sh \
  scripts/verify_pdv_rapidos_path.py | sort)"
[[ "$DELTA" == "$ALLOWED" ]] || { echo "FAIL: delta:"; echo "$DELTA"; exit 2; }

MIG="$(echo "$DELTA" | grep -E '/migrations/[0-9]+_' | sort || true)"
[[ "$MIG" == $'produtos/migrations/0150_pdv_rapido_slot.py\nprodutos/migrations/0151_pdv_rapido_layout.py' ]] \
  || { echo "FAIL: migrations: $MIG"; exit 2; }

CAIXA="$(echo "$DELTA" | grep -Ei 'caixa_fechar|caixa_util|checkout' || true)"
[[ -z "$CAIXA" ]] || { echo "FAIL: path caixa/checkout: $CAIXA"; exit 2; }

PROD_SHA="$(git rev-parse "$PROD_REF")"
git show-ref --verify --quiet "refs/remotes/origin/$ROLLBACK_TAG" || {
  echo "FAIL: rode ./scripts/prep_checkpoint_checklist_2310_pdv_rapidos.sh"; exit 2;
}
[[ "$(git rev-parse origin/$ROLLBACK_TAG)" == "$PROD_SHA" ]] || {
  echo "FAIL: rollback desatualizado"; exit 2;
}

PY=python3
[[ -x .venv/bin/python ]] && PY=.venv/bin/python

"$PY" scripts/verify_pdv_rapidos_path.py | tee /tmp/cutover-2310-pdv-rapidos.log
grep -q VERIFY_OK /tmp/cutover-2310-pdv-rapidos.log

echo "OK: PREP v$VER_ESPERADA · 23/10 PDV-RAPIDOS · migrate 0150+0151 · rollback $ROLLBACK_TAG"
echo "Pós-deploy Render: ./scripts/post_deploy_checklist_2310_pdv_rapidos.sh"

[[ "${1:-}" != "--exec" ]] && exit 0
FRASE="${AGRO_PIN_FRASE:-${AGRO_LOJA_FRASE:-}}"
SENHA="${AGRO_LOJA_SENHA:-}"
[[ "$SENHA" == "$SENHA_OK" ]] || { echo "FAIL: senha"; exit 3; }
echo "$FRASE" | grep -Eiq 'pode.*(produ[cç][aã]o|producao|enviar)|sobe.*(produ[cç][aã]o|producao)|subir.*(produ[cç][aã]o|producao)|enviar.*(produ[cç][aã]o|producao)' \
  || { echo "FAIL: autorização"; exit 3; }
git push origin "$PROD_REF:refs/heads/$ROLLBACK_TAG" || true
git push origin "$PROD_REF:refs/heads/$BACKUP_BR" || true
git push origin "$PREP_REF:producao"
echo "PUSH OK v$VER_ESPERADA · Render Shell: ./scripts/post_deploy_checklist_2310_pdv_rapidos.sh"
