#!/usr/bin/env bash
# Cutover loja — CHECKLIST 20/10d · COMPRAS-FOLHA-MULTI-FORN · v27.26.17 · sem migrate
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
PREP_REF="origin/deploy/prep-checklist-2010d-compras-folha-multi-forn"
PROD_REF="origin/producao"
ROLLBACK_TAG="rollback/pre-v272617-2010d-compras-folha-multi-forn"
BACKUP_BR="producao-backup-pre-v272617-2010d-compras-folha-multi-forn"
SENHA_OK="99738595"
VER_ESPERADA="27.26.17"

git fetch origin producao deploy/prep-checklist-2010d-compras-folha-multi-forn "$ROLLBACK_TAG" 2>/dev/null \
  || git fetch origin producao deploy/prep-checklist-2010d-compras-folha-multi-forn

PROD="$(git rev-parse --short "$PROD_REF")"
PREP="$(git rev-parse --short "$PREP_REF" 2>/dev/null || echo missing)"
VER="$(git show "$PREP_REF:VERSION" 2>/dev/null | tr -d '[:space:]' || echo ?)"
echo "producao=$PROD PREP=$PREP VERSION=$VER"

git merge-base --is-ancestor "$PROD_REF" "$PREP_REF" || { echo "FAIL: PREP não é descendente de produção"; exit 2; }
[[ "$VER" == "$VER_ESPERADA" ]] || { echo "FAIL: VERSION=$VER esperado $VER_ESPERADA"; exit 2; }

DELTA="$(git diff --name-only "$PROD_REF...$PREP_REF" | sort)"
ALLOWED="$(printf '%s\n' \
  VERSION \
  docs/DEPLOY-PREP-CHECKLIST-2010d-compras-folha-multi-forn.md \
  produtos/compras_ultimas_compras_util.py \
  produtos/views.py \
  produtos/templates/produtos/compras_relatorio_planilha.html \
  produtos/templates/produtos/includes/_compras_planilha_print_js_snippet.html \
  produtos/tests_compras_relatorio_fornecedor_multi.py \
  scripts/cutover_loja_checklist_2010d_compras_folha_multi_forn.sh \
  scripts/post_deploy_checklist_2010d_compras_folha_multi_forn.sh \
  scripts/prep_checkpoint_checklist_2010d_compras_folha_multi_forn.sh \
  scripts/verify_compras_folha_multi_forn_path.py | sort)"
[[ "$DELTA" == "$ALLOWED" ]] || { echo "FAIL: delta:"; echo "$DELTA"; exit 2; }

MIG="$(echo "$DELTA" | grep -E '/migrations/[0-9]+_' || true)"
[[ -z "$MIG" ]] || { echo "FAIL: migration: $MIG"; exit 2; }

PDV_TOUCH="$(echo "$DELTA" | grep -Ei 'pdv_wizard|checkout|consulta_produtos|caixa_' || true)"
[[ -z "$PDV_TOUCH" ]] || { echo "FAIL: path PDV no delta: $PDV_TOUCH"; exit 2; }

PROD_SHA="$(git rev-parse "$PROD_REF")"
git show-ref --verify --quiet "refs/remotes/origin/$ROLLBACK_TAG" || {
  echo "FAIL: rode ./scripts/prep_checkpoint_checklist_2010d_compras_folha_multi_forn.sh"; exit 2;
}
[[ "$(git rev-parse origin/$ROLLBACK_TAG)" == "$PROD_SHA" ]] || {
  echo "FAIL: rollback desatualizado"; exit 2;
}

PY=python3
[[ -x .venv/bin/python ]] && PY=.venv/bin/python
export AGRO_PIN_TESTE="${AGRO_PIN_TESTE:-9973}"
"$PY" scripts/verify_compras_folha_multi_forn_path.py | tee /tmp/cutover-2010d-compras.log
grep -qE 'VERIFY_OK [0-9]+' /tmp/cutover-2010d-compras.log
"$PY" manage.py test produtos.tests_compras_relatorio_fornecedor_multi -v 0 2>&1 | tee /tmp/cutover-2010d-tests.log
grep -qE 'OK \(|Ran [0-9]+ test' /tmp/cutover-2010d-tests.log

echo "OK: PREP v$VER_ESPERADA · 20/10d COMPRAS-FOLHA-MULTI-FORN · rollback $ROLLBACK_TAG"
echo "Pós: ./scripts/post_deploy_checklist_2010d_compras_folha_multi_forn.sh"

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
