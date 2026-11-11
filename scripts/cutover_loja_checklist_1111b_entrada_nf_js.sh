#!/usr/bin/env bash
# Hotfix 11/11b · tela Entrada NF travada · v27.26.56
# Só sobe com --exec e frase+senha. Delta: script da página + versão.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
PREP_REF="origin/deploy/prep-checklist-1111b-entrada-nf-js"
PROD_REF="origin/producao"
ROLLBACK_TAG="rollback/pre-v272656-1111b-entrada-nf-js"
BACKUP_BR="producao-backup-pre-v272656-1111b-entrada-nf-js"
SENHA_OK="99738595"
VER_ESPERADA="27.26.56"

git fetch origin producao deploy/prep-checklist-1111b-entrada-nf-js "$ROLLBACK_TAG" 2>/dev/null \
  || git fetch origin producao deploy/prep-checklist-1111b-entrada-nf-js

PROD="$(git rev-parse --short "$PROD_REF")"
PREP="$(git rev-parse --short "$PREP_REF" 2>/dev/null || echo missing)"
VER="$(git show "$PREP_REF:VERSION" 2>/dev/null | tr -d '[:space:]' || echo ?)"
echo "producao=$PROD PREP=$PREP VERSION=$VER"

git merge-base --is-ancestor "$PROD_REF" "$PREP_REF" || { echo "FAIL: PREP não desce da produção"; exit 2; }
[[ "$VER" == "$VER_ESPERADA" ]] || { echo "FAIL: VERSION=$VER esperado $VER_ESPERADA"; exit 2; }

DELTA="$(git diff --name-only "$PROD_REF...$PREP_REF" | sort)"
ALLOWED="$(printf '%s\n' \
  VERSION \
  docs/DEPLOY-PREP-CHECKLIST-1111b-entrada-nf-js.md \
  produtos/templates/produtos/entrada_nota.html \
  scripts/cutover_loja_checklist_1111b_entrada_nf_js.sh \
  scripts/post_deploy_checklist_1111b_entrada_nf_js.sh \
  scripts/prep_checkpoint_checklist_1111b_entrada_nf_js.sh \
  scripts/verify_entrada_nfe_tela_js.py | sort)"
[[ "$DELTA" == "$ALLOWED" ]] || { echo "FAIL: delta:"; echo "$DELTA"; exit 2; }

MIG="$(echo "$DELTA" | grep -E '/migrations/' || true)"
[[ -z "$MIG" ]] || { echo "FAIL: migrate $MIG"; exit 2; }

PROD_SHA="$(git rev-parse "$PROD_REF")"
git show-ref --verify --quiet "refs/remotes/origin/$ROLLBACK_TAG" || {
  echo "FAIL: rode ./scripts/prep_checkpoint_checklist_1111b_entrada_nf_js.sh"; exit 2;
}
[[ "$(git rev-parse origin/$ROLLBACK_TAG)" == "$PROD_SHA" ]] || {
  echo "FAIL: rollback desatualizado"; exit 2;
}

PY=python3
[[ -x .venv/bin/python ]] && PY=.venv/bin/python
"$PY" scripts/verify_entrada_nfe_tela_js.py | tee /tmp/cutover-1111b-js.log
grep -q '^VERIFY_OK' /tmp/cutover-1111b-js.log

echo "OK: PREP v$VER_ESPERADA · só o script da Entrada NF · sem migrate · rollback $ROLLBACK_TAG"

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
echo "REVERT: git push origin $ROLLBACK_TAG:producao"
