#!/usr/bin/env bash
# Cutover loja — hotfix GESTAO-MED-MODAL-GV · CHECKLIST 12/10q · v27.12 · SEM migrate
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

PREP_REF="origin/deploy/prep-checklist-1210q-gv"
PROD_REF="origin/producao"
ROLLBACK_TAG="rollback/pre-hf-gestao-med-modal-gv-v2711"
BACKUP_BR="producao-backup-pre-v2712-hf-gestao-med-modal-gv"
SENHA_OK="99738595"
VER_ESPERADA="27.12"

git fetch origin producao deploy/prep-checklist-1210q-gv "$ROLLBACK_TAG" 2>/dev/null \
  || git fetch origin producao deploy/prep-checklist-1210q-gv

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
ALLOWED="VERSION
docs/DEPLOY-PREP-CHECKLIST-1210q-gv.md
produtos/templates/produtos/_modal_editar_produto_cadastro_erp.inc.html
produtos/templates/produtos/produtos_gestao.html
produtos/tests/test_med_classificacao_regressao.py
scripts/cutover_loja_checklist_1210q_gv.sh
scripts/post_deploy_checklist_1210q_gv.sh
scripts/verify_classificacao_medicamentos_path.py"
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
git grep -q 'function gv(id)' "$PREP_FULL" -- produtos/templates/produtos/_modal_editar_produto_cadastro_erp.inc.html || {
  echo "FAIL: sem gv() no modal"; exit 2;
}
git grep -q 'abrirDrawerSeguro' "$PREP_FULL" -- produtos/templates/produtos/produtos_gestao.html || {
  echo "FAIL: sem try/catch gestão"; exit 2;
}

PY=python3
[[ -x .venv/bin/python ]] && PY=.venv/bin/python

AGRO_PIN_TESTE="${AGRO_PIN_TESTE:-9973}" "$PY" scripts/verify_classificacao_medicamentos_path.py | tee /tmp/cutover-1210q-gv-med.log
grep -q '^PREP_FAILS=0$' /tmp/cutover-1210q-gv-med.log
grep -q 'OK=38 FAIL=0' /tmp/cutover-1210q-gv-med.log

if [[ -f scripts/verify_cb_loja_pdv_busca_path.py ]]; then
  "$PY" scripts/verify_cb_loja_pdv_busca_path.py | tee /tmp/cutover-1210q-gv-pdv.log
  grep -qE 'PREP_FAILS=0|FAIL=0' /tmp/cutover-1210q-gv-pdv.log
fi

echo "OK: PREP v$VER_ESPERADA · GESTAO-MED-MODAL-GV · rollback $ROLLBACK_TAG"
echo "Pós-deploy: ./scripts/post_deploy_checklist_1210q_gv.sh"

if [[ "${1:-}" != "--exec" ]]; then exit 0; fi

FRASE="${AGRO_PIN_FRASE:-${AGRO_LOJA_FRASE:-}}"
SENHA="${AGRO_LOJA_SENHA:-}"
[[ "$SENHA" == "$SENHA_OK" ]] || { echo "FAIL: senha"; exit 3; }
echo "$FRASE" | grep -Eiq 'pode.*(produ[cç][aã]o|producao)|sobe.*(produ[cç][aã]o|producao)|subir.*(produ[cç][aã]o|producao)' \
  || { echo "FAIL: autorização"; exit 3; }

git push origin "$PROD_REF:refs/heads/$BACKUP_BR" || true
git push origin "$PREP_REF:producao"
echo "PUSH OK · badge v$VER_ESPERADA · Ctrl+F5 gestão + cadastro ERP modal"
