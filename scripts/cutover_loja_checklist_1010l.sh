#!/usr/bin/env bash
# Cutover loja — CHECKLIST 10/10l · PDV-CACHE-EAN230 · v26.90
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

PREP_REF="origin/deploy/prep-checklist-1010l"
PROD_REF="origin/producao"
ROLLBACK_TAG="rollback/pre-checklist-1010l-v26.89"
BACKUP_BR="producao-backup-pre-v2690-checklist-1010l"
SENHA_OK="99738595"
VER_ESPERADA="26.90"

git fetch origin producao deploy/prep-checklist-1010l "$ROLLBACK_TAG" 2>/dev/null \
  || git fetch origin producao deploy/prep-checklist-1010l

PROD="$(git rev-parse --short "$PROD_REF")"
PREP="$(git rev-parse --short "$PREP_REF")"
VER="$(git show "$PREP_REF:VERSION" | tr -d '[:space:]')"
echo "producao=$PROD PREP=$PREP VERSION=$VER"

git merge-base --is-ancestor "$PROD_REF" "$PREP_REF" || {
  echo "FAIL: produção não é ancestral do PREP"; exit 2;
}
[[ "$VER" == "$VER_ESPERADA" ]] || {
  echo "FAIL: VERSION=$VER (esperado $VER_ESPERADA)"; exit 2;
}
MIG="$(git diff --name-only "$PROD_REF...$PREP_REF" | grep -E '/migrations/[0-9]+_' || true)"
[[ -z "$MIG" ]] || { echo "FAIL: migrations: $MIG"; exit 2; }

git rev-parse "$ROLLBACK_TAG" >/dev/null 2>&1 \
  || git ls-remote --exit-code --tags origin "refs/tags/$ROLLBACK_TAG" >/dev/null \
  || { echo "FAIL: tag rollback ausente $ROLLBACK_TAG"; exit 2; }

PREP_FULL="$(git rev-parse "$PREP_REF")"
git grep -q 'normalizarScanEanLojaParaBusca' "$PREP_FULL" -- produtos/static/produtos/js/pdv_wizard.js || {
  echo "FAIL: delta sem normalização scan PDV"; exit 2;
}

PY=python3
[[ -x .venv/bin/python ]] && PY=.venv/bin/python

"$PY" scripts/verify_cb_loja_legado_migrate_path.py | tee /tmp/cutover-1010l-legado.log
grep -q 'OK 15/15' /tmp/cutover-1010l-legado.log

"$PY" scripts/verify_cb_loja_bip_variantes_path.py | tee /tmp/cutover-1010l-bip.log
grep -q 'OK 10/10' /tmp/cutover-1010l-bip.log

"$PY" scripts/verify_cb_loja_pdv_busca_path.py | tee /tmp/cutover-1010l-pdv.log
grep -q 'OK 13/13' /tmp/cutover-1010l-pdv.log

"$PY" manage.py test produtos.tests_cb_loja_bip_busca produtos.tests_agro_codigo_barras_loja_util \
  --settings=config.settings 2>&1 | tee /tmp/cutover-1010l-django.log
grep -q 'OK$' /tmp/cutover-1010l-django.log || grep -q 'OK (' /tmp/cutover-1010l-django.log

echo "OK: PREP · migrate NÃO · rollback $ROLLBACK_TAG"
echo "Pós-deploy: Ctrl+F5 PDV (limpa cache v4) · bip 2300000001556 ou 200000001556"

if [[ "${1:-}" != "--exec" ]]; then exit 0; fi

FRASE="${AGRO_LOJA_FRASE:-}"
SENHA="${AGRO_LOJA_SENHA:-}"
[[ "$SENHA" == "$SENHA_OK" ]] || { echo "FAIL: senha"; exit 3; }
echo "$FRASE" | grep -Eiq 'pode.*(produ[cç][aã]o|producao)|sobe.*(produ[cç][aã]o|producao)' \
  || { echo "FAIL: autorização"; exit 3; }

git push origin "$PROD_REF:refs/heads/$BACKUP_BR" || true
git push origin "$PREP_REF:producao"
echo "PUSH OK · badge v$VER_ESPERADA"
