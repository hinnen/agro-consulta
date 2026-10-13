#!/usr/bin/env bash
# Cutover loja — MED-SUBCATEGORIA-DICAS · CHECKLIST 13/10b · v27.15 · SEM migrate
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

PREP_REF="origin/deploy/prep-checklist-1310b-med-sub-dicas"
PROD_REF="origin/producao"
ROLLBACK_TAG="rollback/pre-med-sub-v27144"
BACKUP_BR="producao-backup-pre-v2715-med-sub-dicas"
SENHA_OK="99738595"
VER_ESPERADA="27.15"

git fetch origin producao deploy/prep-checklist-1310b-med-sub-dicas "$ROLLBACK_TAG" 2>/dev/null \
  || git fetch origin producao deploy/prep-checklist-1310b-med-sub-dicas

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
  docs/DEPLOY-PREP-CHECKLIST-1310b-med-sub-dicas.md \
  produtos/templates/produtos/_modal_editar_produto_cadastro_erp.inc.html \
  produtos/tests/test_med_classificacao_regressao.py \
  scripts/cutover_loja_checklist_1310b_med_sub_dicas.sh \
  scripts/post_deploy_checklist_1310b_med_sub_dicas.sh \
  scripts/verify_classificacao_medicamentos_path.py \
  scripts/verify_med_subcategoria_dicas_path.py | sort)"
[[ "$DELTA" == "$ALLOWED" ]] || {
  echo "FAIL: delta inesperado:"; echo "$DELTA"; exit 2;
}

MIG="$(echo "$DELTA" | grep -E '/migrations/[0-9]+_' || true)"
[[ -z "$MIG" ]] || { echo "FAIL: migration: $MIG"; exit 2; }

FORBIDDEN="$(echo "$DELTA" | grep -E \
  'pdv_wizard\.js|busca_filtro_pdv|motor_busca_unificado|caixa_|mongo_financeiro|meta_vendas_util|pdv_state\.js|0145_|clientes_lista|views\.py' || true)"
[[ -z "$FORBIDDEN" ]] || { echo "FAIL: sensível/fora do pacote: $FORBIDDEN"; exit 2; }

PY=python3
[[ -x .venv/bin/python ]] && PY=.venv/bin/python

export AGRO_PIN_TESTE="${AGRO_PIN_TESTE:-9973}"
"$PY" scripts/verify_med_subcategoria_dicas_path.py | tee /tmp/cutover-1310b-med-sub.log
grep -q 'VERIFY_SUB_DICAS_FAILS=0' /tmp/cutover-1310b-med-sub.log

"$PY" scripts/verify_classificacao_medicamentos_path.py | tee /tmp/cutover-1310b-med-class.log
grep -q 'PREP_FAILS=0' /tmp/cutover-1310b-med-class.log
grep -q 'unit_regressao — 9' /tmp/cutover-1310b-med-class.log

echo "OK: PREP v$VER_ESPERADA · MED-SUBCATEGORIA-DICAS · rollback $ROLLBACK_TAG"
echo "Pós-deploy: ./scripts/post_deploy_checklist_1310b_med_sub_dicas.sh"

if [[ "${1:-}" != "--exec" ]]; then exit 0; fi

FRASE="${AGRO_PIN_FRASE:-${AGRO_LOJA_FRASE:-}}"
SENHA="${AGRO_LOJA_SENHA:-}"
[[ "$SENHA" == "$SENHA_OK" ]] || { echo "FAIL: senha"; exit 3; }
echo "$FRASE" | grep -Eiq 'pode.*(produ[cç][aã]o|producao)|sobe.*(produ[cç][aã]o|producao)|subir.*(produ[cç][aã]o|producao)' \
  || { echo "FAIL: autorização"; exit 3; }

git push origin "$PROD_REF:refs/heads/$BACKUP_BR" || true
git push origin "$PREP_REF:producao"
echo "PUSH OK · badge v$VER_ESPERADA · Ctrl+F5 cadastro ERP medicamentos"
