#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

PREP_REF="origin/deploy/prep-checklist-1010"
PROD_REF="origin/producao"
ROLLBACK_TAG="rollback/pre-checklist-1010-v26.78.1"
BACKUP_BR="producao-backup-pre-v2679-checklist-1010"
SENHA_OK="99738595"

git fetch origin producao deploy/prep-checklist-1010 "$ROLLBACK_TAG" 2>/dev/null \
  || git fetch origin producao deploy/prep-checklist-1010

PROD="$(git rev-parse --short "$PROD_REF")"
PREP="$(git rev-parse --short "$PREP_REF")"
VER="$(git show "$PREP_REF:VERSION" | tr -d '[:space:]')"
echo "producao=$PROD PREP=$PREP VERSION=$VER"

git merge-base --is-ancestor "$PROD_REF" "$PREP_REF" || {
  echo "FAIL: produção não é ancestral do PREP"; exit 2;
}
[[ "$VER" == "26.79" ]] || {
  echo "FAIL: VERSION=$VER (esperado 26.79)"; exit 2;
}
MIG="$(git diff --name-only "$PROD_REF...$PREP_REF" | grep -E '/migrations/[0-9]+_' || true)"
[[ -z "$MIG" ]] || {
  echo "FAIL: migrations inesperadas: $MIG"; exit 2;
}
git rev-parse "$ROLLBACK_TAG" >/dev/null 2>&1 \
  || git ls-remote --exit-code --tags origin "refs/tags/$ROLLBACK_TAG" >/dev/null

PY=python3
[[ -x .venv/bin/python ]] && PY=.venv/bin/python

"$PY" scripts/verify_cb_loja_bip_variantes_path.py | tee /tmp/cutover-1010-bip.log
grep -q 'OK 10/10' /tmp/cutover-1010-bip.log
grep -q 'PREP_FAILS=0' /tmp/cutover-1010-bip.log

"$PY" scripts/verify_cadastro_cb_loja_gerador_path.py | tee /tmp/cutover-1010-gerador.log
grep -q 'OK 22/22' /tmp/cutover-1010-gerador.log
grep -q 'PREP_FAILS=0' /tmp/cutover-1010-gerador.log

"$PY" scripts/verify_pdv_balanca_etq_plu_path.py | tee /tmp/cutover-1010-balanca.log
grep -q 'OK=41 FAIL=0' /tmp/cutover-1010-balanca.log
grep -q 'PREP_FAILS=0' /tmp/cutover-1010-balanca.log

"$PY" scripts/verify_pdv_valor_rs_modal_path.py | tee /tmp/cutover-1010-kg.log
grep -q 'OK=46 FAIL=0' /tmp/cutover-1010-kg.log
grep -q 'PREP_FAILS=0' /tmp/cutover-1010-kg.log

echo "OK: PREP pronto · migrate NÃO · rollback $ROLLBACK_TAG"

if [[ "${1:-}" != "--exec" ]]; then
  echo "Dry-run OK. Aguarda frase + senha."
  exit 0
fi

FRASE="${AGRO_LOJA_FRASE:-}"
SENHA="${AGRO_LOJA_SENHA:-}"
[[ "$SENHA" == "$SENHA_OK" ]] || { echo "FAIL: senha"; exit 3; }
echo "$FRASE" | grep -Eiq 'pode.*(produ[cç][aã]o|producao)|sobe.*(produ[cç][aã]o|producao)' \
  || { echo "FAIL: autorização"; exit 3; }

git push origin "$PROD_REF:refs/heads/$BACKUP_BR" || true
git push origin "$PREP_REF:producao"
echo "PUSH OK · aguardar Render · Ctrl+F5 · v26.79"
