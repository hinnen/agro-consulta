#!/usr/bin/env bash
# Cutover loja — CHECKLIST 14/10d itens 1+2 · PLANILHA-MED-ID + ETQ-FILA-MODAL-QTD · v27.17 · SEM migrate
# (ETQ-MOBILE-BIP já Live v27.16 @ 78a867a0)
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

PREP_REF="origin/deploy/prep-checklist-1410d-planilha-etq-pc"
PROD_REF="origin/producao"
ROLLBACK_TAG="rollback/pre-1410d-v2716"
BACKUP_BR="producao-backup-pre-v2717-checklist-1410d"
SENHA_OK="99738595"
VER_ESPERADA="27.17"

git fetch origin producao deploy/prep-checklist-1410d-planilha-etq-pc "$ROLLBACK_TAG" 2>/dev/null \
  || git fetch origin producao deploy/prep-checklist-1410d-planilha-etq-pc

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
  docs/DEPLOY-PREP-CHECKLIST-1410d-planilha-etq-pc.md \
  produtos/cadastro_planilha_util.py \
  produtos/mongo_produto_id_util.py \
  produtos/static/produtos/js/produtos_etiquetas.js \
  produtos/templates/produtos/produtos_cadastro_erp.html \
  produtos/templates/produtos/produtos_etiquetas.html \
  produtos/tests_cadastro_planilha_id_erp.py \
  produtos/tests_mongo_produto_id_util.py \
  produtos/views.py \
  scripts/cutover_loja_checklist_1410d_planilha_etq_pc.sh \
  scripts/post_deploy_checklist_1410d_planilha_etq_pc.sh \
  scripts/verify_etq_solicitacao_vl_pin_path.py | sort)"
[[ "$DELTA" == "$ALLOWED" ]] || {
  echo "FAIL: delta inesperado:"; echo "$DELTA"; exit 2;
}

MIG="$(echo "$DELTA" | grep -E '/migrations/[0-9]+_' || true)"
[[ -z "$MIG" ]] || { echo "FAIL: migration: $MIG"; exit 2; }

FORBIDDEN="$(echo "$DELTA" | grep -E \
  'pdv_wizard\.js|busca_filtro_pdv|motor_busca_unificado|caixa_|mongo_financeiro|meta_vendas_util|pdv_state\.js|0145_|clientes_lista|cliente_form\.html' || true)"
[[ -z "$FORBIDDEN" ]] || { echo "FAIL: sensível/fora do pacote: $FORBIDDEN"; exit 2; }

PREP_FULL="$(git rev-parse "$PREP_REF")"
git grep -q 'mongo_produto_id_util' "$PREP_FULL" -- produtos/views.py || {
  echo "FAIL: falta _pid_doc_produto_gestao"; exit 2;
}
git grep -q 'desconto_pdv_percentual' "$PREP_FULL" -- produtos/views.py && {
  echo "FAIL: PREP inclui desconto PDV (0145)"; exit 2;
} || true
git grep -q 'fecharModal: false' "$PREP_FULL" -- produtos/static/produtos/js/produtos_etiquetas.js || {
  echo "FAIL: falta ETQ modal aberto"; exit 2;
}

PY=python3
[[ -x .venv/bin/python ]] && PY=.venv/bin/python
export AGRO_PIN_TESTE="${AGRO_PIN_TESTE:-9973}"

"$PY" scripts/verify_planilha_med_path.py | tee /tmp/cutover-1410d-planilha.log
grep -q 'VERIFY_MED_FAILS=0' /tmp/cutover-1410d-planilha.log

"$PY" scripts/verify_etiquetas_mobile_flow_path.py | tee /tmp/cutover-1410d-etq-mobile.log
grep -q 'FAIL=0' /tmp/cutover-1410d-etq-mobile.log

"$PY" scripts/verify_etq_solicitacao_vl_pin_path.py | tee /tmp/cutover-1410d-etq-pc.log
grep -q 'FAIL=0' /tmp/cutover-1410d-etq-pc.log
grep -q 'js_sol_import_mantem_modal' /tmp/cutover-1410d-etq-pc.log || grep -q 'js_fila_qtd_todos' /tmp/cutover-1410d-etq-pc.log

"$PY" manage.py test produtos.tests_cadastro_planilha_id_erp produtos.tests_mongo_produto_id_util produtos.tests_cadastro_planilha_med -v0 2>&1 | tee /tmp/cutover-1410d-tests.log
grep -qE '^OK$|OK \(' /tmp/cutover-1410d-tests.log

echo "OK: PREP v$VER_ESPERADA · 14/10d (planilha+etq PC) · rollback $ROLLBACK_TAG"
echo "Pós-deploy: ./scripts/post_deploy_checklist_1410d_planilha_etq_pc.sh"

if [[ "${1:-}" != "--exec" ]]; then exit 0; fi

FRASE="${AGRO_PIN_FRASE:-${AGRO_LOJA_FRASE:-}}"
SENHA="${AGRO_LOJA_SENHA:-}"
[[ "$SENHA" == "$SENHA_OK" ]] || { echo "FAIL: senha"; exit 3; }
echo "$FRASE" | grep -Eiq 'pode.*(produ[cç][aã]o|producao|enviar)|sobe.*(produ[cç][aã]o|producao)|subir.*(produ[cç][aã]o|producao)|enviar.*(produ[cç][aã]o|producao)' \
  || { echo "FAIL: autorização"; exit 3; }

git push origin "$PROD_REF:refs/heads/$BACKUP_BR" || true
git push origin "$PREP_REF:producao"
echo "PUSH OK · badge v$VER_ESPERADA · Ctrl+F5 cadastro + etiquetas PC"
