#!/usr/bin/env bash
# Cutover loja — CHECKLIST 09/10h (v26.78).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
PREP_REF="origin/deploy/prep-checklist-0910h"
PROD_REF="origin/producao"
ROLLBACK_TAG="rollback/pre-checklist-0910h-v26.76"
BACKUP_BR="producao-backup-pre-v2678-checklist-0910h"
SENHA_OK="99738595"

echo "== fetch =="
git fetch origin producao deploy/prep-checklist-0910h "$ROLLBACK_TAG" 2>/dev/null \
  || git fetch origin producao deploy/prep-checklist-0910h

PROD="$(git rev-parse --short "$PROD_REF")"
PREP="$(git rev-parse --short "$PREP_REF")"
VER="$(git show "$PREP_REF:VERSION" | tr -d '[:space:]')"
echo "producao=$PROD  PREP=$PREP  VERSION=$VER"

if ! git merge-base --is-ancestor "$PROD_REF" "$PREP_REF"; then
  echo "FAIL: producao não é ancestral do PREP"
  exit 2
fi
if [[ "$VER" != "26.78" ]]; then
  echo "FAIL: VERSION PREP=$VER (esperado 26.78)"
  exit 2
fi
MIG="$(git diff --name-only "$PROD_REF...$PREP_REF" | grep -E '/migrations/[0-9]+_' || true)"
if [[ -n "$MIG" ]]; then
  echo "FAIL: migrations no delta:"
  echo "$MIG"
  exit 2
fi
if ! git rev-parse "$ROLLBACK_TAG" >/dev/null 2>&1 \
  && ! git ls-remote --exit-code --tags origin "refs/tags/$ROLLBACK_TAG" >/dev/null 2>&1; then
  echo "FAIL: falta tag $ROLLBACK_TAG"
  exit 2
fi

if [[ -x .venv/bin/python ]]; then PY=.venv/bin/python; else PY=python3; fi

echo "== provas path =="
"$PY" scripts/verify_cb_loja_bip_variantes_path.py | tee /tmp/cutover-0910h-bip.log | tail -5
grep -q 'PREP_FAILS=0' /tmp/cutover-0910h-bip.log
grep -q '10/10' /tmp/cutover-0910h-bip.log

"$PY" scripts/verify_cadastro_cb_loja_gerador_path.py | tee /tmp/cutover-0910h-gerador.log | tail -5
grep -q 'PREP_FAILS=0' /tmp/cutover-0910h-gerador.log

"$PY" scripts/verify_etq_ean_loja_path.py | tee /tmp/cutover-0910h-etq.log | tail -5
grep -q 'PREP_FAILS=0' /tmp/cutover-0910h-etq.log

"$PY" scripts/verify_pdv_valor_rs_modal_path.py | tee /tmp/cutover-0910h-valor.log | tail -8
grep -q 'OK=46 FAIL=0' /tmp/cutover-0910h-valor.log
grep -q 'PREP_FAILS=0' /tmp/cutover-0910h-valor.log

echo "OK tip PREP · migrate NÃO · rollback $ROLLBACK_TAG · backup $BACKUP_BR"

if [[ "${1:-}" != "--exec" ]]; then
  echo
  echo "Dry-run OK. Na senha:"
  echo "  AGRO_LOJA_FRASE='pode subir para produção' AGRO_LOJA_SENHA='$SENHA_OK' $0 --exec"
  exit 0
fi

FRASE="${AGRO_LOJA_FRASE:-}"
SENHA="${AGRO_LOJA_SENHA:-}"
if [[ "$SENHA" != "$SENHA_OK" ]]; then
  echo "FAIL: senha ausente/incorreta"
  exit 3
fi
if ! echo "$FRASE" | grep -Eiq 'pode.*(produ[cç][aã]o|producao)|sobe.*(produ[cç][aã]o|producao)|produ[cç][aã]o'; then
  echo "FAIL: frase de autorização"
  exit 3
fi

echo "== cutover producao → $PREP =="
git push origin "$PROD_REF:refs/heads/$BACKUP_BR" || true
if git show-ref --verify --quiet refs/heads/producao; then
  git checkout -f producao
  git reset --hard "$PREP_REF"
  git push origin producao
else
  git push origin "$PREP_REF:producao"
fi
echo "PUSH OK · Render · Ctrl+F5 · badge v26.78"
