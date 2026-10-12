#!/usr/bin/env bash
# Cutover loja — CHECKLIST 12/10n · ETQ mobile + crédito revisão + med classificação · v27.08 · SEM migrate
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

PREP_REF="origin/deploy/prep-checklist-1210n"
PROD_REF="origin/producao"
ROLLBACK_TAG="rollback/pre-checklist-1210n-v27.06-b539"
BACKUP_BR="producao-backup-pre-v2708-checklist-1210n"
SENHA_OK="99738595"
VER_ESPERADA="27.08"

git fetch origin producao deploy/prep-checklist-1210n "$ROLLBACK_TAG" 2>/dev/null \
  || git fetch origin producao deploy/prep-checklist-1210n

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

MIG="$(git diff --name-only "$PROD_REF...$PREP_REF" | grep -E '/migrations/[0-9]+_' || true)"
[[ -z "$MIG" ]] || { echo "FAIL: migration no pacote: $MIG"; exit 2; }

FORBIDDEN="$(git diff --name-only "$PROD_REF...$PREP_REF" | grep -E \
  'pdv_wizard\.js|busca_filtro_pdv|motor_busca_unificado|caixa_|mongo_financeiro|meta_vendas_util' || true)"
[[ -z "$FORBIDDEN" ]] || { echo "FAIL: sensível: $FORBIDDEN"; exit 2; }

PREP_FULL="$(git rev-parse "$PREP_REF")"
git grep -q 'visualViewport' "$PREP_FULL" -- produtos/templates/produtos/etiquetas_solicitacao/app.html || {
  echo "FAIL: delta sem ETQ mobile polish"; exit 2;
}
git grep -q 'credito_limite_revisao_export_xlsx' "$PREP_FULL" -- produtos/urls.py || {
  echo "FAIL: delta sem export revisão limite"; exit 2;
}
git cat-file -e "$PREP_FULL:produtos/data/classificacao_medicamento_vet.json" 2>/dev/null || {
  echo "FAIL: delta sem JSON medicamentos"; exit 2;
}

PY=python3
[[ -x .venv/bin/python ]] && PY=.venv/bin/python

"$PY" scripts/verify_etq_solicitacao_vl_pin_path.py | tee /tmp/cutover-1210n-etq.log
grep -q 'TOTAL OK=35 FAIL=0' /tmp/cutover-1210n-etq.log

"$PY" scripts/verify_etiquetas_mobile_flow_path.py | tee /tmp/cutover-1210n-etqmob.log
grep -q 'TOTAL OK=13 FAIL=0' /tmp/cutover-1210n-etqmob.log

"$PY" scripts/verify_credito_limite_revisao_path.py | tee /tmp/cutover-1210n-cred.log
grep -qE '0 FAIL ===|· 0 FAIL ===' /tmp/cutover-1210n-cred.log

"$PY" scripts/verify_classificacao_medicamentos_path.py | tee /tmp/cutover-1210n-med.log
grep -qE '0 FAIL ===|PREP_FAILS=0|TOTAL OK=29 FAIL=0' /tmp/cutover-1210n-med.log

if [[ -f scripts/verify_cb_loja_pdv_busca_path.py ]]; then
  "$PY" scripts/verify_cb_loja_pdv_busca_path.py | tee /tmp/cutover-1210n-pdv.log
  grep -qE 'PREP_FAILS=0|FAIL=0' /tmp/cutover-1210n-pdv.log
fi

if [[ -f scripts/verify_gm_lojas_pin_runtime.py ]]; then
  AGRO_PIN_TESTE=9973 "$PY" scripts/verify_gm_lojas_pin_runtime.py | tee /tmp/cutover-1210n-vl.log
  grep -q 'FAIL=0' /tmp/cutover-1210n-vl.log
fi

echo "OK: PREP v$VER_ESPERADA · 12/10n · rollback $ROLLBACK_TAG"
echo "Pós-deploy: ./scripts/post_deploy_checklist_1210n.sh"

if [[ "${1:-}" != "--exec" ]]; then exit 0; fi

FRASE="${AGRO_PIN_FRASE:-${AGRO_LOJA_FRASE:-}}"
SENHA="${AGRO_LOJA_SENHA:-}"
[[ "$SENHA" == "$SENHA_OK" ]] || { echo "FAIL: senha"; exit 3; }
echo "$FRASE" | grep -Eiq 'pode.*(produ[cç][aã]o|producao)|sobe.*(produ[cç][aã]o|producao)|subir.*(produ[cç][aã]o|producao)' \
  || { echo "FAIL: autorização"; exit 3; }

git push origin "$PROD_REF:refs/heads/$BACKUP_BR" || true
git push origin "$PREP_REF:producao"
echo "PUSH OK · badge v$VER_ESPERADA · Ctrl+F5 etiquetas mobile + revisão limite + cadastro ERP"
