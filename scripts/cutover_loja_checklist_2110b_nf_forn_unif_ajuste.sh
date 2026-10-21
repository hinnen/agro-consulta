#!/usr/bin/env bash
# Cutover loja — CHECKLIST 21/10b · NF forn unif + ajuste manual · v27.26.19 · migrate 0149
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
PREP_REF="origin/deploy/prep-checklist-2110b-nf-forn-unif-ajuste"
PROD_REF="origin/producao"
ROLLBACK_TAG="rollback/pre-v272619-2110b-nf-forn-unif-ajuste"
BACKUP_BR="producao-backup-pre-v272619-2110b-nf-forn-unif-ajuste"
SENHA_OK="99738595"
VER_ESPERADA="27.26.19"
MIG_ESPERADA="produtos/migrations/0149_fornecedor_entrada_nfe_alias_agro.py"

git fetch origin producao deploy/prep-checklist-2110b-nf-forn-unif-ajuste "$ROLLBACK_TAG" 2>/dev/null \
  || git fetch origin producao deploy/prep-checklist-2110b-nf-forn-unif-ajuste

PROD="$(git rev-parse --short "$PROD_REF")"
PREP="$(git rev-parse --short "$PREP_REF" 2>/dev/null || echo missing)"
VER="$(git show "$PREP_REF:VERSION" 2>/dev/null | tr -d '[:space:]' || echo ?)"
echo "producao=$PROD PREP=$PREP VERSION=$VER"

git merge-base --is-ancestor "$PROD_REF" "$PREP_REF" || { echo "FAIL: PREP não é descendente de produção"; exit 2; }
[[ "$VER" == "$VER_ESPERADA" ]] || { echo "FAIL: VERSION=$VER esperado $VER_ESPERADA"; exit 2; }

DELTA="$(git diff --name-only "$PROD_REF...$PREP_REF" | sort)"
ALLOWED="$(printf '%s\n' \
  VERSION \
  docs/DEPLOY-PREP-CHECKLIST-2110b-nf-forn-unif-ajuste.md \
  produtos/fornecedor_entrada_nfe_util.py \
  produtos/migrations/0149_fornecedor_entrada_nfe_alias_agro.py \
  produtos/models.py \
  produtos/nfe_entrada_util.py \
  produtos/pg_backup_registry.py \
  produtos/templates/produtos/entrada_nota.html \
  produtos/urls.py \
  produtos/views.py \
  scripts/cutover_loja_checklist_2110b_nf_forn_unif_ajuste.sh \
  scripts/post_deploy_checklist_2110b_nf_forn_unif_ajuste.sh \
  scripts/prep_checkpoint_checklist_2110b_nf_forn_unif_ajuste.sh \
  scripts/verify_fornecedor_entrada_nfe_alias_path.py \
  scripts/verify_nf_ajuste_manual_rateio_path.py \
  scripts/verify_nf_forn_unif_ajuste_path.py | sort)"
[[ "$DELTA" == "$ALLOWED" ]] || { echo "FAIL: delta:"; echo "$DELTA"; exit 2; }

MIG="$(echo "$DELTA" | grep -E '/migrations/[0-9]+_' || true)"
[[ "$MIG" == "$MIG_ESPERADA" ]] || { echo "FAIL: migration esperada $MIG_ESPERADA; got: $MIG"; exit 2; }

PDV_TOUCH="$(echo "$DELTA" | grep -Ei 'pdv_wizard|checkout|consulta_produtos|pdv_repasse|caixa_fechar|caixa_util' || true)"
[[ -z "$PDV_TOUCH" ]] || { echo "FAIL: path PDV/caixa no delta: $PDV_TOUCH"; exit 2; }

PROD_SHA="$(git rev-parse "$PROD_REF")"
git show-ref --verify --quiet "refs/remotes/origin/$ROLLBACK_TAG" || {
  echo "FAIL: rode ./scripts/prep_checkpoint_checklist_2110b_nf_forn_unif_ajuste.sh"; exit 2;
}
[[ "$(git rev-parse origin/$ROLLBACK_TAG)" == "$PROD_SHA" ]] || {
  echo "FAIL: rollback desatualizado"; exit 2;
}

PY=python3
[[ -x .venv/bin/python ]] && PY=.venv/bin/python

"$PY" scripts/verify_nf_forn_unif_ajuste_path.py | tee /tmp/cutover-2110b-path.log
grep -q VERIFY_OK /tmp/cutover-2110b-path.log

"$PY" scripts/verify_nf_ajuste_manual_rateio_path.py | tee /tmp/cutover-2110b-rateio.log
grep -q VERIFY_OK /tmp/cutover-2110b-rateio.log

"$PY" scripts/verify_fornecedor_entrada_nfe_alias_path.py | tee /tmp/cutover-2110b-alias.log
grep -qE 'VERIFY_OK fornecedor_entrada_nfe_alias|SKIP alias DB' /tmp/cutover-2110b-alias.log

echo "OK: PREP v$VER_ESPERADA · 21/10b NF-FORN-UNIF+AJUSTE · migrate 0149 · rollback $ROLLBACK_TAG"
echo "Pós-deploy Render: ./scripts/post_deploy_checklist_2110b_nf_forn_unif_ajuste.sh"

[[ "${1:-}" != "--exec" ]] && exit 0
FRASE="${AGRO_PIN_FRASE:-${AGRO_LOJA_FRASE:-}}"
SENHA="${AGRO_LOJA_SENHA:-}"
[[ "$SENHA" == "$SENHA_OK" ]] || { echo "FAIL: senha"; exit 3; }
echo "$FRASE" | grep -Eiq 'pode.*(produ[cç][aã]o|producao|enviar)|sobe.*(produ[cç][aã]o|producao)|subir.*(produ[cç][aã]o|producao)|enviar.*(produ[cç][aã]o|producao)' \
  || { echo "FAIL: autorização"; exit 3; }
git push origin "$PROD_REF:refs/heads/$ROLLBACK_TAG" || true
git push origin "$PROD_REF:refs/heads/$BACKUP_BR" || true
git push origin "$PREP_REF:producao"
echo "PUSH OK v$VER_ESPERADA · Render Shell: ./scripts/post_deploy_checklist_2110b_nf_forn_unif_ajuste.sh"
