#!/usr/bin/env bash
# Cutover loja — CHECKLIST 10/10b · NF-EAN-OPCIONAL · v26.80
# Dry-run (padrão): prova + gates. Exec: só com frase + senha no env.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

PREP_REF="origin/deploy/prep-nf-ean-opcional"
PROD_REF="origin/producao"
ROLLBACK_TAG="rollback/pre-checklist-1010b-v26.79"
BACKUP_BR="producao-backup-pre-v2680-checklist-1010b"
SENHA_OK="99738595"
VER_ESPERADA="26.80"

git fetch origin producao deploy/prep-nf-ean-opcional "$ROLLBACK_TAG" 2>/dev/null \
  || git fetch origin producao deploy/prep-nf-ean-opcional

PROD="$(git rev-parse --short "$PROD_REF")"
PREP="$(git rev-parse --short "$PREP_REF")"
VER="$(git show "$PREP_REF:VERSION" | tr -d '[:space:]')"
PROD_FULL="$(git rev-parse "$PROD_REF")"
echo "producao=$PROD ($PROD_FULL) PREP=$PREP VERSION=$VER"

git merge-base --is-ancestor "$PROD_REF" "$PREP_REF" || {
  echo "FAIL: produção não é ancestral do PREP"; exit 2;
}
[[ "$VER" == "$VER_ESPERADA" ]] || {
  echo "FAIL: VERSION=$VER (esperado $VER_ESPERADA)"; exit 2;
}
MIG="$(git diff --name-only "$PROD_REF...$PREP_REF" | grep -E '/migrations/[0-9]+_' || true)"
[[ -z "$MIG" ]] || {
  echo "FAIL: migrations inesperadas: $MIG"; exit 2;
}
git rev-parse "$ROLLBACK_TAG" >/dev/null 2>&1 \
  || git ls-remote --exit-code --tags origin "refs/tags/$ROLLBACK_TAG" >/dev/null \
  || { echo "FAIL: tag rollback ausente $ROLLBACK_TAG"; exit 2; }

# Delta só o pacote NF-EAN
DELTA="$(git diff --name-only "$PROD_REF...$PREP_REF")"
echo "$DELTA" | grep -q 'produtos/nfe_entrada_util.py' || {
  echo "FAIL: delta sem nfe_entrada_util.py"; exit 2;
}
echo "$DELTA" | grep -q 'produtos/tests_nfe_ean_opcional.py' || {
  echo "FAIL: delta sem tests_nfe_ean_opcional.py"; exit 2;
}

PY=python3
[[ -x .venv/bin/python ]] && PY=.venv/bin/python

"$PY" scripts/verify_nfe_ean_opcional_path.py | tee /tmp/cutover-1010b-nfe-ean.log
grep -q 'OK=26 FAIL=0' /tmp/cutover-1010b-nfe-ean.log
grep -q 'PREP_FAILS=0' /tmp/cutover-1010b-nfe-ean.log
grep -q 'Ran 20 tests' /tmp/cutover-1010b-nfe-ean.log

echo "OK: PREP pronto · migrate NÃO · rollback $ROLLBACK_TAG · backup $BACKUP_BR"
echo "Checkpoint Live atual: $PROD_FULL"

if [[ "${1:-}" != "--exec" ]]; then
  echo "Dry-run OK. Aguarda frase + senha no próximo chat."
  exit 0
fi

FRASE="${AGRO_LOJA_FRASE:-}"
SENHA="${AGRO_LOJA_SENHA:-}"
[[ "$SENHA" == "$SENHA_OK" ]] || { echo "FAIL: senha"; exit 3; }
echo "$FRASE" | grep -Eiq 'pode.*(produ[cç][aã]o|producao)|sobe.*(produ[cç][aã]o|producao)' \
  || { echo "FAIL: autorização"; exit 3; }

git push origin "$PROD_REF:refs/heads/$BACKUP_BR" || true
git push origin "$PREP_REF:producao"
echo "PUSH OK · aguardar Render · Ctrl+F5 · badge v$VER_ESPERADA"
echo "Rollback: git push origin $ROLLBACK_TAG:producao --force-with-lease"
