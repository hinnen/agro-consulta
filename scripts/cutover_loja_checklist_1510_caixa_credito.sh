#!/usr/bin/env bash
# Cutover loja — CHECKLIST 15/10 + 15/10b · CAIXA-FECHAR-PERF + CREDITO-DESBLOQUEIO-LAB · v27.19 · SEM migrate
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

PREP_REF="origin/deploy/prep-checklist-1510-caixa-credito"
PROD_REF="origin/producao"
ROLLBACK_TAG="rollback/pre-v2718-caixa-credito"
BACKUP_BR="producao-backup-pre-v2719-caixa-credito"
SENHA_OK="99738595"
VER_ESPERADA="27.19"

git fetch origin producao deploy/prep-checklist-1510-caixa-credito "$ROLLBACK_TAG" 2>/dev/null \
  || git fetch origin producao deploy/prep-checklist-1510-caixa-credito

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
  docs/DEPLOY-PREP-CHECKLIST-1510-caixa-credito.md \
  docs/CREDITO_SCORE_SHADOW.md \
  produtos/caixa_util.py \
  produtos/credito_limite_revisao.py \
  produtos/credito_score_views.py \
  produtos/templates/produtos/credito_limite_revisao.html \
  produtos/templates/produtos/credito_score_cliente_detalhe.html \
  produtos/templates/produtos/credito_score_laboratorio.html \
  produtos/tests_credito_limite_revisao.py \
  produtos/views.py \
  scripts/cutover_loja_checklist_1510_caixa_credito.sh \
  scripts/post_deploy_checklist_1510_caixa_credito.sh \
  scripts/verify_caixa_devolucao_dinheiro_mp_path.py \
  scripts/verify_caixa_fechar_loja_path.py \
  scripts/verify_caixa_fechar_repasse_path.py \
  scripts/verify_credito_limite_revisao_path.py | sort)"
[[ "$DELTA" == "$ALLOWED" ]] || {
  echo "FAIL: delta inesperado:"; echo "$DELTA"; exit 2;
}

MIG="$(echo "$DELTA" | grep -E '/migrations/[0-9]+_' || true)"
[[ -z "$MIG" ]] || { echo "FAIL: migration: $MIG"; exit 2; }

FORBIDDEN="$(echo "$DELTA" | grep -E \
  'pdv_wizard\.js|busca_filtro_pdv|motor_busca_unificado|pdv_state\.js|0145_|clientes_lista|cliente_form\.html' || true)"
[[ -z "$FORBIDDEN" ]] || { echo "FAIL: sensível/fora do pacote: $FORBIDDEN"; exit 2; }

PREP_FULL="$(git rev-parse "$PREP_REF")"
git grep -q 'class CaixaFecharPrep' "$PREP_FULL" -- produtos/caixa_util.py || {
  echo "FAIL: falta CaixaFecharPrep"; exit 2;
}
git grep -q 'desconto_pdv_percentual' "$PREP_FULL" -- produtos/views.py && {
  echo "FAIL: PREP inclui desconto PDV (15/10a fora)"; exit 2;
} || true

PY=python3
[[ -x .venv/bin/python ]] && PY=.venv/bin/python
export AGRO_PIN_TESTE="${AGRO_PIN_TESTE:-9973}"

"$PY" scripts/verify_caixa_fechar_loja_path.py | tee /tmp/cutover-1510-caixa-loja.log
grep -q 'VERIFY_FECHAR_LOJA_OK' /tmp/cutover-1510-caixa-loja.log

"$PY" scripts/verify_caixa_fechar_repasse_path.py | tee /tmp/cutover-1510-caixa-rep.log
grep -q 'VERIFY_FECHAR_REPASSE_OK' /tmp/cutover-1510-caixa-rep.log

"$PY" scripts/verify_caixa_devolucao_dinheiro_mp_path.py | tee /tmp/cutover-1510-caixa-dev.log
grep -qE '171/171|VERIFY.*OK|FAIL=0' /tmp/cutover-1510-caixa-dev.log

"$PY" scripts/verify_credito_limite_revisao_path.py | tee /tmp/cutover-1510-cred.log
grep -q '0 FAIL' /tmp/cutover-1510-cred.log

"$PY" manage.py test produtos.tests_credito_limite_revisao -v0 2>&1 | tee /tmp/cutover-1510-cred-tests.log
grep -qE '^OK$|OK \(' /tmp/cutover-1510-cred-tests.log

echo "OK: PREP v$VER_ESPERADA · CAIXA-FECHAR-PERF + CREDITO-DESBLOQUEIO · rollback $ROLLBACK_TAG"
echo "Pós-deploy: ./scripts/post_deploy_checklist_1510_caixa_credito.sh"
echo "NOTA: 15/10a CLIENTE-LISTA-DESC-PDV (0145) = cutover separado"

if [[ "${1:-}" != "--exec" ]]; then exit 0; fi

FRASE="${AGRO_PIN_FRASE:-${AGRO_LOJA_FRASE:-}}"
SENHA="${AGRO_LOJA_SENHA:-}"
[[ "$SENHA" == "$SENHA_OK" ]] || { echo "FAIL: senha"; exit 3; }
echo "$FRASE" | grep -Eiq 'pode.*(produ[cç][aã]o|producao|enviar)|sobe.*(produ[cç][aã]o|producao)|subir.*(produ[cç][aã]o|producao)|enviar.*(produ[cç][aã]o|producao)' \
  || { echo "FAIL: autorização"; exit 3; }

git push origin "$PROD_REF:refs/heads/$BACKUP_BR" || true
git push origin "$PREP_REF:producao"
echo "PUSH OK · badge v$VER_ESPERADA · Ctrl+F5 caixa fechar + análise crédito"
