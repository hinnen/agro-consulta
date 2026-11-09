#!/usr/bin/env bash
# Cutover 09/11 · NF-FORN-UNIF-AGRO · v27.26.54 · sem migrate
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
PREP_REF="origin/deploy/prep-checklist-0911-nf-forn-unif-agro"
PROD_REF="origin/producao"
ROLLBACK_TAG="rollback/pre-v272654-0911-nf-forn-unif-agro"
BACKUP_BR="producao-backup-pre-v272654-0911-nf-forn-unif-agro"
SENHA_OK="99738595"
VER_ESPERADA="27.26.54"

git fetch origin producao deploy/prep-checklist-0911-nf-forn-unif-agro "$ROLLBACK_TAG" 2>/dev/null \
  || git fetch origin producao deploy/prep-checklist-0911-nf-forn-unif-agro

PROD="$(git rev-parse --short "$PROD_REF")"
PREP="$(git rev-parse --short "$PREP_REF" 2>/dev/null || echo missing)"
VER="$(git show "$PREP_REF:VERSION" 2>/dev/null | tr -d '[:space:]' || echo ?)"
echo "producao=$PROD PREP=$PREP VERSION=$VER"

git merge-base --is-ancestor "$PROD_REF" "$PREP_REF" || { echo "FAIL: PREP não é descendente de produção"; exit 2; }
[[ "$VER" == "$VER_ESPERADA" ]] || { echo "FAIL: VERSION=$VER esperado $VER_ESPERADA"; exit 2; }

DELTA="$(git diff --name-only "$PROD_REF...$PREP_REF" | sort)"
ALLOWED="$(printf '%s\n' \
  VERSION \
  docs/DEPLOY-PREP-CHECKLIST-0911-nf-forn-unif-agro.md \
  produtos/fornecedor_entrada_nfe_util.py \
  produtos/models.py \
  produtos/templates/produtos/entrada_nota.html \
  produtos/views.py \
  scripts/cutover_loja_checklist_0911_nf_forn_unif_agro.sh \
  scripts/post_deploy_checklist_0911_nf_forn_unif_agro.sh \
  scripts/prep_checkpoint_checklist_0911_nf_forn_unif_agro.sh \
  scripts/verify_fornecedor_entrada_nfe_alias_path.py \
  scripts/verify_nf_forn_unif_ajuste_path.py | sort)"
[[ "$DELTA" == "$ALLOWED" ]] || { echo "FAIL: delta:"; echo "$DELTA"; exit 2; }

MIG="$(echo "$DELTA" | grep -E '/migrations/[0-9]+_' || true)"
[[ -z "$MIG" ]] || { echo "FAIL: migrate inesperada $MIG"; exit 2; }

PDV_TOUCH="$(echo "$DELTA" | grep -Ei 'pdv_wizard|checkout|consulta_produtos|pdv_repasse|caixa_fechar|caixa_util' || true)"
[[ -z "$PDV_TOUCH" ]] || { echo "FAIL: path PDV/caixa no delta: $PDV_TOUCH"; exit 2; }

PROD_SHA="$(git rev-parse "$PROD_REF")"
git show-ref --verify --quiet "refs/remotes/origin/$ROLLBACK_TAG" || {
  echo "FAIL: rode ./scripts/prep_checkpoint_checklist_0911_nf_forn_unif_agro.sh"; exit 2;
}
[[ "$(git rev-parse origin/$ROLLBACK_TAG)" == "$PROD_SHA" ]] || {
  echo "FAIL: rollback desatualizado"; exit 2;
}

PY=python3
[[ -x .venv/bin/python ]] && PY=.venv/bin/python

"$PY" scripts/verify_nf_forn_unif_ajuste_path.py | tee /tmp/cutover-0911-path.log
grep -q VERIFY_OK /tmp/cutover-0911-path.log

"$PY" scripts/verify_fornecedor_entrada_nfe_alias_path.py | tee /tmp/cutover-0911-alias.log
grep -qE 'VERIFY_OK fornecedor_entrada_nfe_alias' /tmp/cutover-0911-alias.log

echo "OK: PREP v$VER_ESPERADA · 09/11 NF-FORN-UNIF-AGRO · sem migrate · rollback $ROLLBACK_TAG"
echo "Pós-deploy Render: ./scripts/post_deploy_checklist_0911_nf_forn_unif_agro.sh"

[[ "${1:-}" != "--exec" ]] && exit 0
FRASE="${AGRO_PIN_FRASE:-${AGRO_LOJA_FRASE:-}}"
SENHA="${AGRO_LOJA_SENHA:-}"
[[ "$SENHA" == "$SENHA_OK" ]] || { echo "FAIL: senha"; exit 3; }
echo "$FRASE" | grep -Eiq 'pode.*(produ[cç][aã]o|producao|enviar)|sobe.*(produ[cç][aã]o|producao)|subir.*(produ[cç][aã]o|producao)|enviar.*(produ[cç][aã]o|producao)' \
  || { echo "FAIL: autorização"; exit 3; }
git push origin "$PROD_REF:refs/heads/$ROLLBACK_TAG" || true
git push origin "$PROD_REF:refs/heads/$BACKUP_BR" || true
git push origin "$PREP_REF:producao"
echo "PUSH OK v$VER_ESPERADA · Render Shell: ./scripts/post_deploy_checklist_0911_nf_forn_unif_agro.sh"
echo "REVERT: git push origin $ROLLBACK_TAG:producao"
