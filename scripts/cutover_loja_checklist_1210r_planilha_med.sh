#!/usr/bin/env bash
# Cutover loja — PLANILHA-MED-EXCEL · CHECKLIST 12/10r · v27.13 · SEM migrate
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

PREP_REF="origin/deploy/prep-checklist-1210r-planilha-med"
PROD_REF="origin/producao"
ROLLBACK_TAG="rollback/pre-planilha-med-v2712"
BACKUP_BR="producao-backup-pre-v2713-planilha-med-excel"
SENHA_OK="99738595"
VER_ESPERADA="27.13"

git fetch origin producao deploy/prep-checklist-1210r-planilha-med "$ROLLBACK_TAG" 2>/dev/null \
  || git fetch origin producao deploy/prep-checklist-1210r-planilha-med

PROD="$(git rev-parse --short "$PROD_REF")"
PREP="$(git rev-parse --short "$PREP_REF")"
VER="$(git show "$PREP_REF:VERSION" | tr -d '[:space:]')"
echo "producao=$PROD PREP=$PREP VERSION=$VER"

git merge-base --is-ancestor "$PROD_REF" "$PREP_REF" || {
  echo "FAIL: produção não é ancestral do PREP"; exit 2;
}
[[ "$VER" == "$VER_ESPERADA" ]] || {
  echo "FAIL: VERSION=$VER (esperado $VER_ESPERADA)"; exit 2;
}

DELTA="$(git diff --name-only "$PROD_REF...$PREP_REF" | sort)"
ALLOWED="$(printf '%s\n' \
  VERSION \
  docs/DEPLOY-PREP-CHECKLIST-1210r-planilha-med.md \
  produtos/cadastro_planilha_util.py \
  produtos/static/produtos/js/cadastro_erp_panel.js \
  produtos/tests_cadastro_planilha_med.py \
  scripts/cutover_loja_checklist_1210r_planilha_med.sh \
  scripts/post_deploy_checklist_1210r_planilha_med.sh \
  scripts/verify_classificacao_medicamentos_path.py \
  scripts/verify_planilha_med_path.py | sort)"
[[ "$DELTA" == "$ALLOWED" ]] || {
  echo "FAIL: delta inesperado:"; echo "$DELTA"; exit 2;
}

MIG="$(echo "$DELTA" | grep -E '/migrations/[0-9]+_' || true)"
[[ -z "$MIG" ]] || { echo "FAIL: migration: $MIG"; exit 2; }

FORBIDDEN="$(echo "$DELTA" | grep -E \
  'pdv_wizard\.js|busca_filtro_pdv|motor_busca_unificado|caixa_|mongo_financeiro|meta_vendas_util|pdv_state\.js|views\.py' || true)"
[[ -z "$FORBIDDEN" ]] || { echo "FAIL: sensível: $FORBIDDEN"; exit 2; }

PREP_FULL="$(git rev-parse "$PREP_REF")"
export AGRO_VERIFY_DIFF_TIP="$PREP_FULL"
git grep -q 'MED_IMPORT_KEYS' "$PREP_FULL" -- produtos/cadastro_planilha_util.py || {
  echo "FAIL: sem MED_IMPORT_KEYS"; exit 2;
}
git grep -q "_aplicar_patch_med" "$PREP_FULL" -- produtos/cadastro_planilha_util.py || {
  echo "FAIL: sem _aplicar_patch_med"; exit 2;
}

PY=python3
[[ -x .venv/bin/python ]] && PY=.venv/bin/python

AGRO_PIN_TESTE="${AGRO_PIN_TESTE:-9973}" "$PY" scripts/verify_planilha_med_path.py | tee /tmp/cutover-1210r-planilha-med.log
grep -q '^VERIFY_MED_FAILS=0$' /tmp/cutover-1210r-planilha-med.log

AGRO_PIN_TESTE="${AGRO_PIN_TESTE:-9973}" "$PY" scripts/verify_classificacao_medicamentos_path.py | tee /tmp/cutover-1210r-class-med.log
grep -q '^PREP_FAILS=0$' /tmp/cutover-1210r-class-med.log

if [[ -f scripts/verify_planilha_delivery_path.py ]]; then
  "$PY" scripts/verify_planilha_delivery_path.py | tee /tmp/cutover-1210r-delivery.log
  grep -q 'VERIFY_OK' /tmp/cutover-1210r-delivery.log
fi

echo "OK: PREP v$VER_ESPERADA · PLANILHA-MED-EXCEL · rollback $ROLLBACK_TAG @ $PROD"
echo "Pós-deploy: ./scripts/post_deploy_checklist_1210r_planilha_med.sh"

if [[ "${1:-}" != "--exec" ]]; then exit 0; fi

FRASE="${AGRO_PIN_FRASE:-${AGRO_LOJA_FRASE:-}}"
SENHA="${AGRO_LOJA_SENHA:-}"
[[ "$SENHA" == "$SENHA_OK" ]] || { echo "FAIL: senha"; exit 3; }
echo "$FRASE" | grep -Eiq 'pode.*(produ[cç][aã]o|producao)|sobe.*(produ[cç][aã]o|producao)|subir.*(produ[cç][aã]o|producao)' \
  || { echo "FAIL: autorização"; exit 3; }

git push origin "$PROD_REF:refs/heads/$BACKUP_BR" || true
git push origin "$PROD_REF:refs/heads/$ROLLBACK_TAG" || true
git push origin "$PREP_REF:producao"
echo "PUSH OK · badge v$VER_ESPERADA · Ctrl+F5 cadastro Excel"
