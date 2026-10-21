#!/usr/bin/env bash
# Cutover loja — CHECKLIST 21/10 · REPASSE-CENTRO-VILA-PDV · v27.26.18 · migrate 0148
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
PREP_REF="origin/deploy/prep-checklist-2110-repasse-centro-vila-pdv"
PROD_REF="origin/producao"
ROLLBACK_TAG="rollback/pre-v272618-2110-repasse-centro-vila-pdv"
BACKUP_BR="producao-backup-pre-v272618-2110-repasse-centro-vila-pdv"
SENHA_OK="99738595"
VER_ESPERADA="27.26.18"
MIG_ESPERADA="produtos/migrations/0148_repasse_centro_vila.py"

git fetch origin producao deploy/prep-checklist-2110-repasse-centro-vila-pdv "$ROLLBACK_TAG" 2>/dev/null \
  || git fetch origin producao deploy/prep-checklist-2110-repasse-centro-vila-pdv

PROD="$(git rev-parse --short "$PROD_REF")"
PREP="$(git rev-parse --short "$PREP_REF" 2>/dev/null || echo missing)"
VER="$(git show "$PREP_REF:VERSION" 2>/dev/null | tr -d '[:space:]' || echo ?)"
echo "producao=$PROD PREP=$PREP VERSION=$VER"

git merge-base --is-ancestor "$PROD_REF" "$PREP_REF" || { echo "FAIL: PREP não é descendente de produção"; exit 2; }
[[ "$VER" == "$VER_ESPERADA" ]] || { echo "FAIL: VERSION=$VER esperado $VER_ESPERADA"; exit 2; }

DELTA="$(git diff --name-only "$PROD_REF...$PREP_REF" | sort)"
ALLOWED="$(printf '%s\n' \
  VERSION \
  docs/DEPLOY-PREP-CHECKLIST-2110-repasse-centro-vila-pdv.md \
  produtos/migrations/0148_repasse_centro_vila.py \
  produtos/models.py \
  produtos/pg_backup_registry.py \
  produtos/repasse_vila_util.py \
  produtos/static/produtos/js/pdv_repasse_vila.js \
  produtos/templates/produtos/includes/repasse_aviso_abertura.html \
  produtos/templates/produtos/includes/repasse_help_agents.html \
  produtos/templates/produtos/partials/pdv/repasse_vila_overlay.html \
  produtos/urls.py \
  produtos/views.py \
  produtos/views_repasse_vila.py \
  scripts/cutover_loja_checklist_2110_repasse_centro_vila_pdv.sh \
  scripts/post_deploy_checklist_2110_repasse_centro_vila_pdv.sh \
  scripts/prep_checkpoint_checklist_2110_repasse_centro_vila_pdv.sh \
  scripts/verify_repasse_centro_vila_deep.py \
  scripts/verify_repasse_centro_vila_path.py | sort)"
[[ "$DELTA" == "$ALLOWED" ]] || { echo "FAIL: delta:"; echo "$DELTA"; exit 2; }

MIG="$(echo "$DELTA" | grep -E '/migrations/[0-9]+_' || true)"
[[ "$MIG" == "$MIG_ESPERADA" ]] || { echo "FAIL: migration esperada $MIG_ESPERADA; got: $MIG"; exit 2; }

FORBIDDEN="$(echo "$DELTA" | grep -Ei 'pdv_wizard\.js|consulta_produtos|compras_relatorio|lancamentos_financeiro|nfe_saida' || true)"
[[ -z "$FORBIDDEN" ]] || { echo "FAIL: path proibido no delta: $FORBIDDEN"; exit 2; }

PROD_SHA="$(git rev-parse "$PROD_REF")"
git show-ref --verify --quiet "refs/remotes/origin/$ROLLBACK_TAG" || {
  echo "FAIL: rode ./scripts/prep_checkpoint_checklist_2110_repasse_centro_vila_pdv.sh"; exit 2;
}
[[ "$(git rev-parse origin/$ROLLBACK_TAG)" == "$PROD_SHA" ]] || {
  echo "FAIL: rollback desatualizado"; exit 2;
}

PY=python3
[[ -x .venv/bin/python ]] && PY=.venv/bin/python
export AGRO_PIN_TESTE="${AGRO_PIN_TESTE:-9973}"
git fetch origin teste 2>/dev/null || true

"$PY" scripts/verify_repasse_centro_vila_path.py | tee /tmp/cutover-2110-cv-path.log
grep -q VERIFY_REPASSE_CENTRO_VILA_PATH_OK /tmp/cutover-2110-cv-path.log

"$PY" scripts/verify_repasse_vila_path.py | tee /tmp/cutover-2110-vila.log
grep -q '^VERIFY_OK$' /tmp/cutover-2110-vila.log

"$PY" scripts/verify_repasse_pdv_overlay_path.py | tee /tmp/cutover-2110-overlay.log
grep -q VERIFY_REPASSE_PDV_OVERLAY_OK /tmp/cutover-2110-overlay.log

"$PY" scripts/verify_repasse_centro_vila_deep.py | tee /tmp/cutover-2110-cv-deep.log
grep -qE 'VERIFY_REPASSE_CENTRO_VILA_DEEP_OK|SKIP deep' /tmp/cutover-2110-cv-deep.log

echo "OK: PREP v$VER_ESPERADA · 21/10 REPASSE-CENTRO-VILA-PDV · migrate 0148 · rollback $ROLLBACK_TAG"
echo "Pós-deploy Render: ./scripts/post_deploy_checklist_2110_repasse_centro_vila_pdv.sh"

[[ "${1:-}" != "--exec" ]] && exit 0
FRASE="${AGRO_PIN_FRASE:-${AGRO_LOJA_FRASE:-}}"
SENHA="${AGRO_LOJA_SENHA:-}"
[[ "$SENHA" == "$SENHA_OK" ]] || { echo "FAIL: senha"; exit 3; }
echo "$FRASE" | grep -Eiq 'pode.*(produ[cç][aã]o|producao|enviar)|sobe.*(produ[cç][aã]o|producao)|subir.*(produ[cç][aã]o|producao)|enviar.*(produ[cç][aã]o|producao)' \
  || { echo "FAIL: autorização"; exit 3; }
git push origin "$PROD_REF:refs/heads/$ROLLBACK_TAG" || true
git push origin "$PROD_REF:refs/heads/$BACKUP_BR" || true
git push origin "$PREP_REF:producao"
echo "PUSH OK v$VER_ESPERADA · Render Shell: ./scripts/post_deploy_checklist_2110_repasse_centro_vila_pdv.sh"
