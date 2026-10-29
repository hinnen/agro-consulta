#!/usr/bin/env bash
# Cutover loja — CHECKLIST 27/10h · NFE-EMITENTE-CFOP · v27.26.39 · sem migrate
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

PREP_REF="origin/deploy/prep-checklist-2710h-nfe-emitente-cfop"
PROD_REF="origin/producao"
ROLLBACK_TAG="rollback/pre-v272639-nfe-emitente-cfop"
BACKUP_BR="producao-backup-pre-v272639-nfe-emitente-cfop"
SENHA_OK="99738595"
VER_ESPERADA="27.26.39"
BASE_LOJA="${NFE_EMITENTE_BASE:-97d4b22c}"

_assert_version_clean() {
  local raw="$1"
  [[ "$raw" != *"<<<<<<"* ]] && [[ "$raw" != *"======="* ]] && [[ "$raw" != *">>>>>>"* ]] || {
    echo "FAIL: VERSION inválido (conflito merge): $raw"; exit 2;
  }
}

git fetch origin producao deploy/prep-checklist-2710h-nfe-emitente-cfop "$ROLLBACK_TAG" 2>/dev/null \
  || git fetch origin producao deploy/prep-checklist-2710h-nfe-emitente-cfop

PROD="$(git rev-parse --short "$PROD_REF")"
PREP="$(git rev-parse --short "$PREP_REF" 2>/dev/null || echo missing)"
VER="$(git show "$PREP_REF:VERSION" 2>/dev/null | tr -d '[:space:]' || echo ?)"
echo "producao=$PROD PREP=$PREP VERSION=$VER"
_assert_version_clean "$VER"
[[ "$VER" == "$VER_ESPERADA" ]] || { echo "FAIL: VERSION=$VER (esperado $VER_ESPERADA)"; exit 2; }

git merge-base --is-ancestor "$BASE_LOJA" "$PREP_REF" || {
  echo "FAIL: BASE $BASE_LOJA não é ancestral do PREP"; exit 2;
}

DELTA="$(git diff --name-only "${BASE_LOJA}...${PREP_REF}" | sort)"
ALLOWED="$(printf '%s\n' \
  VERSION \
  banana.md \
  docs/DEPLOY-PREP-CHECKLIST-2710h-nfe-emitente-cfop.md \
  produtos/nfe_saida_emissao_util.py \
  produtos/nfe_saida_regras_validas_util.py \
  produtos/nfe_saida_sp_emissao_util.py \
  produtos/nfe_saida_util.py \
  produtos/static/produtos/js/nfe_saida.js \
  produtos/templates/produtos/nfe_saida.html \
  produtos/tests_nfe_saida_util.py \
  produtos/views_nfe_saida.py \
  scripts/cutover_loja_checklist_2710h_nfe_emitente_cfop.sh \
  scripts/post_deploy_checklist_2710h_nfe_emitente_cfop.sh \
  scripts/prep_checkpoint_checklist_2710h_nfe_emitente_cfop.sh \
  scripts/verify_nfe_saida_path.py | sort)"
[[ "$DELTA" == "$ALLOWED" ]] || {
  echo "FAIL: delta inesperado:"; echo "$DELTA"; echo "--- permitido ---"; echo "$ALLOWED"; exit 2;
}

MIG="$(echo "$DELTA" | grep -E '/migrations/[0-9]+_' || true)"
[[ -z "$MIG" ]] || { echo "FAIL: migration inesperada: $MIG"; exit 2; }

if git show-ref --verify --quiet "refs/remotes/origin/$ROLLBACK_TAG"; then
  RB_SHA="$(git rev-parse "origin/$ROLLBACK_TAG")"
  BASE_SHA="$(git rev-parse "${BASE_LOJA}^{commit}")"
  [[ "$RB_SHA" == "$BASE_SHA" ]] || {
    echo "FAIL: $ROLLBACK_TAG != BASE $BASE_LOJA"; echo "Rode: ./scripts/prep_checkpoint_checklist_2710h_nfe_emitente_cfop.sh"; exit 2;
  }
  echo "OK rollback $ROLLBACK_TAG @ $(git rev-parse --short "$BASE_SHA")"
else
  echo "FAIL: branch $ROLLBACK_TAG ausente — rode o prep"; exit 2
fi

PY=python3
[[ -x .venv/bin/python ]] && PY=.venv/bin/python
export AGRO_PIN_TESTE="${AGRO_PIN_TESTE:-9973}"

"$PY" scripts/verify_nfe_saida_path.py | tee /tmp/cutover-2710h-nfe.log
grep -q "PREP_FAILS=0" /tmp/cutover-2710h-nfe.log

"$PY" manage.py test \
  produtos.tests_nfe_saida_util \
  produtos.tests_nfe_saida_motor_etapa3 \
  produtos.tests_nfe_saida_evento \
  produtos.tests_nfe_saida_compra_prefeitura \
  produtos.tests_nfe_fiscal_padrao \
  produtos.tests_nfe_fiscal_config \
  produtos.tests_nfce_loja \
  -v 0 2>&1 | tee /tmp/cutover-2710h-tests.log
grep -qE 'OK \(|Ran [0-9]+ test' /tmp/cutover-2710h-tests.log

echo "OK: PREP v$VER_ESPERADA · 27/10h NFE-EMITENTE-CFOP · rollback $ROLLBACK_TAG"
echo "Pós-deploy: ./scripts/post_deploy_checklist_2710h_nfe_emitente_cfop.sh"
echo "NÃO transmite NF-e automático."

if [[ "${1:-}" != "--exec" ]]; then exit 0; fi

FRASE="${AGRO_PIN_FRASE:-${AGRO_LOJA_FRASE:-}}"
SENHA="${AGRO_LOJA_SENHA:-}"
[[ "$SENHA" == "$SENHA_OK" ]] || { echo "FAIL: senha"; exit 3; }
echo "$FRASE" | grep -Eiq 'pode.*(produ[cç][aã]o|producao|enviar)|sobe.*(produ[cç][aã]o|producao)|subir.*(produ[cç][aã]o|producao)|enviar.*(produ[cç][aã]o|producao)' \
  || { echo "FAIL: autorização"; exit 3; }

git push origin "${BASE_LOJA}:refs/heads/${ROLLBACK_TAG}" || true
git push origin "${BASE_LOJA}:refs/heads/${BACKUP_BR}" || true
git push origin "$PREP_REF:producao"
echo "PUSH OK · badge v$VER_ESPERADA · Shell: ./scripts/post_deploy_checklist_2710h_nfe_emitente_cfop.sh"
