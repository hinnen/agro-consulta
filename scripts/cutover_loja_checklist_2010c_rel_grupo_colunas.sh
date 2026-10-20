#!/usr/bin/env bash
# Cutover loja — REL-GRUPO-COLUNAS · CHECKLIST 20/10c · v27.26.16 · sem migrate
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
PREP_REF="origin/deploy/prep-checklist-2010c-rel-grupo-colunas"
PROD_REF="origin/producao"
ROLLBACK_TAG="rollback/pre-v272616-2010c-rel-grupo-colunas"
BACKUP_BR="producao-backup-pre-v272616-2010c-rel-grupo-colunas"
SENHA_OK="99738595"
VER_ESPERADA="27.26.16"

git fetch origin producao deploy/prep-checklist-2010c-rel-grupo-colunas "$ROLLBACK_TAG" 2>/dev/null \
  || git fetch origin producao deploy/prep-checklist-2010c-rel-grupo-colunas

PROD="$(git rev-parse --short "$PROD_REF")"
PREP="$(git rev-parse --short "$PREP_REF" 2>/dev/null || echo missing)"
VER="$(git show "$PREP_REF:VERSION" 2>/dev/null | tr -d '[:space:]' || echo ?)"
echo "producao=$PROD PREP=$PREP VERSION=$VER"

git merge-base --is-ancestor "$PROD_REF" "$PREP_REF" || { echo "FAIL: PREP não é descendente de produção"; exit 2; }
[[ "$VER" == "$VER_ESPERADA" ]] || { echo "FAIL: VERSION=$VER esperado $VER_ESPERADA"; exit 2; }

DELTA="$(git diff --name-only "$PROD_REF...$PREP_REF" | sort)"
ALLOWED="$(printf '%s\n' \
  VERSION \
  banana.md \
  docs/DEPLOY-PREP-CHECKLIST-2010c-rel-grupo-colunas.md \
  produtos/relatorios_central_views.py \
  produtos/relatorios_vendas_util.py \
  produtos/static/produtos/js/vendas_grupo_table.js \
  produtos/templates/produtos/includes/relatorios_help_agents.html \
  produtos/templates/produtos/includes/vendas_grupo_tabela.html \
  produtos/templates/produtos/relatorios_generico.html \
  produtos/templatetags/agro_exibir.py \
  produtos/tests_relatorios_central_filtros.py \
  produtos/vendas_grupo_table.py \
  scripts/cutover_loja_checklist_2010c_rel_grupo_colunas.sh \
  scripts/post_deploy_checklist_2010c_rel_grupo_colunas.sh \
  scripts/prep_checkpoint_checklist_2010c_rel_grupo_colunas.sh \
  scripts/verify_rel_grupo_colunas_path.py \
  scripts/verify_rel_grupo_margem_path.py | sort)"
[[ "$DELTA" == "$ALLOWED" ]] || { echo "FAIL: delta:"; echo "$DELTA"; exit 2; }

MIG="$(echo "$DELTA" | grep -E '/migrations/[0-9]+_' || true)"
[[ -z "$MIG" ]] || { echo "FAIL: migration: $MIG"; exit 2; }

PDV_TOUCH="$(echo "$DELTA" | grep -Ei 'pdv|checkout|caixa|consulta_produtos|wizard' || true)"
[[ -z "$PDV_TOUCH" ]] || { echo "FAIL: path PDV no delta: $PDV_TOUCH"; exit 2; }

PROD_SHA="$(git rev-parse "$PROD_REF")"
git show-ref --verify --quiet "refs/remotes/origin/$ROLLBACK_TAG" || {
  echo "FAIL: rode ./scripts/prep_checkpoint_checklist_2010c_rel_grupo_colunas.sh"; exit 2;
}
[[ "$(git rev-parse origin/$ROLLBACK_TAG)" == "$PROD_SHA" ]] || {
  echo "FAIL: rollback desatualizado"; exit 2;
}

PY=python3
[[ -x .venv/bin/python ]] && PY=.venv/bin/python
export AGRO_PIN_TESTE="${AGRO_PIN_TESTE:-9973}"
"$PY" scripts/verify_rel_grupo_colunas_path.py | tee /tmp/cutover-2010c-colunas.log
grep -qE '=== [0-9]+ OK, 0 FAIL ===' /tmp/cutover-2010c-colunas.log
"$PY" scripts/verify_rel_grupo_margem_path.py | tee /tmp/cutover-2010c-margem.log
grep -qE '=== [0-9]+ OK, 0 FAIL ===' /tmp/cutover-2010c-margem.log
"$PY" scripts/verify_rel_cat_sub_excel_path.py | tee /tmp/cutover-2010c-cat.log
grep -qE 'TOTAL OK=[0-9]+ FAIL=0' /tmp/cutover-2010c-cat.log

echo "OK: PREP v$VER_ESPERADA · 20/10c REL-GRUPO-COLUNAS · rollback $ROLLBACK_TAG"
echo "Pós: ./scripts/post_deploy_checklist_2010c_rel_grupo_colunas.sh"

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
