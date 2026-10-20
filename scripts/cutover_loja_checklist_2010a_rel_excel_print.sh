#!/usr/bin/env bash
# Cutover loja — CHECKLIST 20/10a · Excel download + impressão paisagem · v27.26.14 · sem migrate
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
PREP_REF="origin/deploy/prep-checklist-2010a"
PROD_REF="origin/producao"
ROLLBACK_TAG="rollback/pre-v272614-2010a-rel-excel-print"
BACKUP_BR="producao-backup-pre-v272614-2010a"
SENHA_OK="99738595"
VER_ESPERADA="27.26.14"
COMMIT_PACOTE="32e7c517"

git fetch origin producao deploy/prep-checklist-2010a "$ROLLBACK_TAG" 2>/dev/null \
  || git fetch origin producao deploy/prep-checklist-2010a

PROD="$(git rev-parse --short "$PROD_REF")"
PREP="$(git rev-parse --short "$PREP_REF" 2>/dev/null || echo missing)"
VER="$(git show "$PREP_REF:VERSION" 2>/dev/null | tr -d '[:space:]' || echo ?)"
echo "producao=$PROD PREP=$PREP VERSION=$VER"

git merge-base --is-ancestor "$PROD_REF" "$PREP_REF" || { echo "FAIL: PREP não é descendente de produção"; exit 2; }
[[ "$VER" == "$VER_ESPERADA" ]] || { echo "FAIL: VERSION=$VER esperado $VER_ESPERADA"; exit 2; }

DELTA="$(git diff --name-only "$PROD_REF...$PREP_REF" | sort)"
ALLOWED="$(printf '%s\n' \
  VERSION \
  produtos/templates/produtos/_relatorios_loading.html \
  produtos/templates/produtos/relatorios_generico.html \
  produtos/tests_relatorios_central_filtros.py \
  scripts/verify_rel_cat_sub_excel_path.py \
  scripts/cutover_loja_checklist_2010a_rel_excel_print.sh \
  scripts/post_deploy_checklist_2010a_rel_excel_print.sh \
  scripts/prep_checkpoint_checklist_2010a_rel_excel_print.sh | sort)"
[[ "$DELTA" == "$ALLOWED" ]] || { echo "FAIL: delta:"; echo "$DELTA"; exit 2; }

PROD_SHA="$(git rev-parse "$PROD_REF")"
git show-ref --verify --quiet "refs/remotes/origin/$ROLLBACK_TAG" || {
  echo "FAIL: rode ./scripts/prep_checkpoint_checklist_2010a_rel_excel_print.sh"; exit 2;
}
[[ "$(git rev-parse origin/$ROLLBACK_TAG)" == "$PROD_SHA" ]] || {
  echo "FAIL: rollback desatualizado"; exit 2;
}

PY=python3
[[ -x .venv/bin/python ]] && PY=.venv/bin/python
export AGRO_PIN_TESTE="${AGRO_PIN_TESTE:-9973}"
"$PY" scripts/verify_rel_grupo_margem_path.py | tee /tmp/cutover-2010a.log
grep -qE '=== [0-9]+ OK, 0 FAIL ===' /tmp/cutover-2010a.log
"$PY" scripts/verify_rel_cat_sub_excel_path.py | tee /tmp/cutover-2010a-cat.log
grep -qE 'TOTAL OK=[0-9]+ FAIL=0' /tmp/cutover-2010a-cat.log

echo "OK: PREP v$VER_ESPERADA · 20/10a · commit $COMMIT_PACOTE · rollback $ROLLBACK_TAG"
echo "Pós: ./scripts/post_deploy_checklist_2010a_rel_excel_print.sh"

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
