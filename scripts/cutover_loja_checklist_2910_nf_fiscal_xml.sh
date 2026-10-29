#!/usr/bin/env bash
# Cutover 29/10 · fiscal do XML na entrada de nota · v27.26.44 · sem migrate
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
PREP_REF="origin/deploy/prep-checklist-2910-nf-fiscal-xml"
PROD_REF="origin/producao"
ROLLBACK_TAG="rollback/pre-v272644-2910-nf-fiscal-xml"
BACKUP_BR="producao-backup-pre-v272644-2910-nf-fiscal-xml"
SENHA_OK="99738595"
VER_ESPERADA="27.26.44"

git fetch origin producao deploy/prep-checklist-2910-nf-fiscal-xml "$ROLLBACK_TAG" 2>/dev/null \
  || git fetch origin producao deploy/prep-checklist-2910-nf-fiscal-xml

PROD="$(git rev-parse --short "$PROD_REF")"
PREP="$(git rev-parse --short "$PREP_REF" 2>/dev/null || echo missing)"
VER="$(git show "$PREP_REF:VERSION" 2>/dev/null | tr -d '[:space:]' || echo ?)"
echo "producao=$PROD PREP=$PREP VERSION=$VER"
git merge-base --is-ancestor "$PROD_REF" "$PREP_REF" || { echo "FAIL: ancestralidade"; exit 2; }
[[ "$VER" == "$VER_ESPERADA" ]] || { echo "FAIL: VERSION=$VER"; exit 2; }

DELTA="$(git diff --name-only "$PROD_REF...$PREP_REF" | sort)"
ALLOWED="$(printf '%s\n' \
  VERSION \
  docs/DEPLOY-PREP-CHECKLIST-2910-nf-fiscal-xml.md \
  produtos/agro_produto_fiscal_defaults.py \
  produtos/nfe_entrada_util.py \
  produtos/templates/produtos/entrada_nota.html \
  produtos/tests_entrada_nf_fiscal_xml.py \
  produtos/views.py \
  scripts/cutover_loja_checklist_2910_nf_fiscal_xml.sh \
  scripts/post_deploy_checklist_2910_nf_fiscal_xml.sh \
  scripts/prep_checkpoint_checklist_2910_nf_fiscal_xml.sh \
  scripts/verify_nf_fiscal_xml_path.py | sort)"
[[ "$DELTA" == "$ALLOWED" ]] || { echo "FAIL: delta:"; echo "$DELTA"; exit 2; }

MIG="$(echo "$DELTA" | grep -E '/migrations/[0-9]+_' || true)"
[[ -z "$MIG" ]] || { echo "FAIL: migrate $MIG"; exit 2; }
CAIXA="$(echo "$DELTA" | grep -Ei 'caixa_fechar|caixa_util|checkout|pdv_wizard' || true)"
[[ -z "$CAIXA" ]] || { echo "FAIL: pdv/caixa $CAIXA"; exit 2; }

PROD_SHA="$(git rev-parse "$PROD_REF")"
git show-ref --verify --quiet "refs/remotes/origin/$ROLLBACK_TAG" || { echo "FAIL: checkpoint"; exit 2; }
[[ "$(git rev-parse origin/$ROLLBACK_TAG)" == "$PROD_SHA" ]] || { echo "FAIL: rollback"; exit 2; }

PY=python3
[[ -x .venv/bin/python ]] && PY=.venv/bin/python
"$PY" scripts/verify_nf_fiscal_xml_path.py | tee /tmp/cutover-2910.log
grep -q 'VERIFY_OK 14' /tmp/cutover-2910.log
echo "OK: PREP v$VER_ESPERADA · 29/10 fiscal XML · sem migrate · rollback $ROLLBACK_TAG"

[[ "${1:-}" != "--exec" ]] && exit 0
FRASE="${AGRO_PIN_FRASE:-${AGRO_LOJA_FRASE:-}}"
SENHA="${AGRO_LOJA_SENHA:-}"
[[ "$SENHA" == "$SENHA_OK" ]] || { echo "FAIL: senha"; exit 3; }
echo "$FRASE" | grep -Eiq 'pode.*(produ[cç][aã]o|producao|enviar)|sobe.*(produ[cç][aã]o|producao)|subir.*(produ[cç][aã]o|producao)|enviar.*(produ[cç][aã]o|producao)' \
  || { echo "FAIL: autorização"; exit 3; }
git push origin "$PROD_REF:refs/heads/$ROLLBACK_TAG" || true
git push origin "$PROD_REF:refs/heads/$BACKUP_BR" || true
git push origin "$PREP_REF:producao"
echo "PUSH OK v$VER_ESPERADA · Render Shell: ./scripts/post_deploy_checklist_2910_nf_fiscal_xml.sh"
