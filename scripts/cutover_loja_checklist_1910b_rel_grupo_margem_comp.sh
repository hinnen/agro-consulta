#!/usr/bin/env bash
# Cutover loja — CHECKLIST 19/10b · REL-GRUPO-MARGEM-COMP · v27.26.13 · sem migrate · só leitura /relatorios/
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

PREP_REF="origin/deploy/prep-checklist-1910b"
PROD_REF="origin/producao"
ROLLBACK_TAG="rollback/pre-v272613-1910b-rel-grupo"
BACKUP_BR="producao-backup-pre-v272613-1910b"
SENHA_OK="99738595"
VER_ESPERADA="27.26.13"
COMMIT_PACOTE="dfabcf9f"

_assert_version_clean() {
  local raw="$1"
  [[ "$raw" != *"<<<<<<"* ]] && [[ "$raw" != *"======="* ]] && [[ "$raw" != *">>>>>>"* ]] || {
    echo "FAIL: VERSION inválido (conflito merge): $raw"; exit 2;
  }
}

git fetch origin producao deploy/prep-checklist-1910b "$ROLLBACK_TAG" 2>/dev/null \
  || git fetch origin producao deploy/prep-checklist-1910b

PROD="$(git rev-parse --short "$PROD_REF")"
PREP="$(git rev-parse --short "$PREP_REF" 2>/dev/null || echo missing)"
VER="$(git show "$PREP_REF:VERSION" 2>/dev/null | tr -d '[:space:]' || echo ?)"
echo "producao=$PROD PREP=$PREP VERSION=$VER"
_assert_version_clean "$VER"

git merge-base --is-ancestor "$PROD_REF" "$PREP_REF" || {
  echo "FAIL: produção não é ancestral do PREP"; exit 2;
}
[[ "$VER" == "$VER_ESPERADA" ]] || {
  echo "FAIL: VERSION=$VER (esperado $VER_ESPERADA)"; exit 2;
}

DELTA="$(git diff --name-only "$PROD_REF...$PREP_REF" | sort)"
ALLOWED="$(printf '%s\n' \
  VERSION \
  banana.md \
  docs/DEPLOY-PREP-CHECKLIST-1910b-rel-grupo-margem-comp.md \
  produtos/relatorios_central_views.py \
  produtos/relatorios_vendas_util.py \
  produtos/templates/produtos/includes/relatorios_help_agents.html \
  produtos/templates/produtos/relatorios_generico.html \
  produtos/templates/produtos/relatorios_hub.html \
  produtos/tests_relatorios_central_filtros.py \
  scripts/cutover_loja_checklist_1910b_rel_grupo_margem_comp.sh \
  scripts/post_deploy_checklist_1910b_rel_grupo_margem_comp.sh \
  scripts/prep_checkpoint_checklist_1910b_rel_grupo_margem_comp.sh \
  scripts/verify_rel_grupo_margem_path.py | sort)"
[[ "$DELTA" == "$ALLOWED" ]] || {
  echo "FAIL: delta inesperado (não merge teste inteiro — só este pacote):"; echo "$DELTA"; exit 2;
}

MIG="$(echo "$DELTA" | grep -E '/migrations/[0-9]+_' || true)"
[[ -z "$MIG" ]] || { echo "FAIL: migration inesperada: $MIG"; exit 2; }

PDV_TOUCH="$(echo "$DELTA" | grep -Ei 'pdv|checkout|caixa|venda_agro|consulta_produtos' || true)"
[[ -z "$PDV_TOUCH" ]] || { echo "FAIL: path PDV/vendas no delta: $PDV_TOUCH"; exit 2; }

git merge-base --is-ancestor "$COMMIT_PACOTE" "$PREP_REF" 2>/dev/null || git cat-file -e "$COMMIT_PACOTE^{commit}" 2>/dev/null || true
if git merge-base --is-ancestor "$COMMIT_PACOTE" "$PREP_REF" 2>/dev/null; then
  echo "OK pacote commit $COMMIT_PACOTE no PREP"
else
  echo "WARN: confira cherry-pick $COMMIT_PACOTE no PREP"
fi

PROD_SHA="$(git rev-parse "$PROD_REF")"
if git show-ref --verify --quiet "refs/remotes/origin/$ROLLBACK_TAG"; then
  RB_SHA="$(git rev-parse "origin/$ROLLBACK_TAG")"
  [[ "$RB_SHA" == "$PROD_SHA" ]] || {
    echo "FAIL: $ROLLBACK_TAG ($(git rev-parse --short origin/$ROLLBACK_TAG)) != produção ($PROD)";
    echo "Rode: ./scripts/prep_checkpoint_checklist_1910b_rel_grupo_margem_comp.sh";
    exit 2;
  }
  echo "OK rollback $ROLLBACK_TAG @ $PROD"
else
  echo "FAIL: branch $ROLLBACK_TAG ausente — rode ./scripts/prep_checkpoint_checklist_1910b_rel_grupo_margem_comp.sh"
  exit 2
fi

PY=python3
[[ -x .venv/bin/python ]] && PY=.venv/bin/python
export AGRO_PIN_TESTE="${AGRO_PIN_TESTE:-9973}"

"$PY" scripts/verify_rel_grupo_margem_path.py | tee /tmp/cutover-1910b-rel-grupo.log
grep -qE '=== [0-9]+ OK, 0 FAIL ===' /tmp/cutover-1910b-rel-grupo.log

"$PY" scripts/verify_rel_cat_sub_excel_path.py | tee /tmp/cutover-1910b-rel-cat.log
grep -qE 'TOTAL OK=[0-9]+ FAIL=0' /tmp/cutover-1910b-rel-cat.log

"$PY" manage.py test produtos.tests_relatorios_central_filtros -v 0 2>&1 | tee /tmp/cutover-1910b-tests.log
grep -qE 'OK \(|Ran [0-9]+ test' /tmp/cutover-1910b-tests.log

echo "OK: PREP v$VER_ESPERADA · 19/10b REL-GRUPO-MARGEM-COMP · rollback $ROLLBACK_TAG"
echo "Pós-deploy loja: ./scripts/post_deploy_checklist_1910b_rel_grupo_margem_comp.sh"

if [[ "${1:-}" != "--exec" ]]; then exit 0; fi

FRASE="${AGRO_PIN_FRASE:-${AGRO_LOJA_FRASE:-}}"
SENHA="${AGRO_LOJA_SENHA:-}"
[[ "$SENHA" == "$SENHA_OK" ]] || { echo "FAIL: senha"; exit 3; }
echo "$FRASE" | grep -Eiq 'pode.*(produ[cç][aã]o|producao|enviar)|sobe.*(produ[cç][aã]o|producao)|subir.*(produ[cç][aã]o|producao)|enviar.*(produ[cç][aã]o|producao)' \
  || { echo "FAIL: autorização"; exit 3; }

git push origin "$PROD_REF:refs/heads/$ROLLBACK_TAG" || true
git push origin "$PROD_REF:refs/heads/$BACKUP_BR" || true
git push origin "$PREP_REF:producao"
echo "PUSH OK · badge v$VER_ESPERADA · Render Shell: ./scripts/post_deploy_checklist_1910b_rel_grupo_margem_comp.sh"
