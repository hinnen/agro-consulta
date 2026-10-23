#!/usr/bin/env bash
# Cutover loja — CHECKLIST 09/10f (ETQ-EAN-LOJA-DV + PDV-VALOR-RS-MODAL · v26.76).
# SEM senha: dry-run + provas path.
# COM senha: sobe producao = tip PREP (mínima pausa).
#
# Uso:
#   ./scripts/cutover_loja_checklist_0910f.sh
#   AGRO_LOJA_FRASE='pode subir para produção' AGRO_LOJA_SENHA='99738595' \
#     ./scripts/cutover_loja_checklist_0910f.sh --exec
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
PREP_REF="origin/deploy/prep-checklist-0910f"
PROD_REF="origin/producao"
ROLLBACK_TAG="rollback/pre-checklist-0910f-v26.74"
BACKUP_BR="producao-backup-pre-v2676-checklist-0910f"
SENHA_OK="99738595"

echo "== fetch =="
git fetch origin producao deploy/prep-checklist-0910f "$ROLLBACK_TAG" 2>/dev/null \
  || git fetch origin producao deploy/prep-checklist-0910f

PROD="$(git rev-parse --short "$PROD_REF")"
PREP="$(git rev-parse --short "$PREP_REF")"
VER="$(git show "$PREP_REF:VERSION" | tr -d '[:space:]')"
echo "producao=$PROD  PREP=$PREP  VERSION=$VER"

if ! git merge-base --is-ancestor "$PROD_REF" "$PREP_REF"; then
  echo "FAIL: producao não é ancestral do PREP — abortar cutover"
  exit 2
fi
if [[ "$VER" != "26.76" ]]; then
  echo "FAIL: VERSION PREP=$VER (esperado 26.76)"
  exit 2
fi
MIG="$(git diff --name-only "$PROD_REF...$PREP_REF" | grep -ci migration || true)"
if [[ "${MIG:-0}" != "0" ]]; then
  echo "FAIL: há migrations no delta — não é cutover rápido"
  exit 2
fi
if ! git rev-parse "$ROLLBACK_TAG" >/dev/null 2>&1 \
  && ! git rev-parse "origin/$ROLLBACK_TAG" >/dev/null 2>&1 \
  && ! git ls-remote --exit-code --tags origin "refs/tags/$ROLLBACK_TAG" >/dev/null 2>&1; then
  echo "FAIL: falta tag $ROLLBACK_TAG"
  exit 2
fi

if [[ -x .venv/bin/python ]]; then
  PY=.venv/bin/python
else
  PY=python3
fi

echo "== provas path =="
"$PY" scripts/verify_etq_ean_loja_path.py | tee /tmp/cutover-0910f-etq.log | tail -8
grep -q 'PREP_FAILS=0' /tmp/cutover-0910f-etq.log

"$PY" scripts/verify_cadastro_cb_loja_gerador_path.py | tee /tmp/cutover-0910f-gerador.log | tail -5
grep -q 'PREP_FAILS=0' /tmp/cutover-0910f-gerador.log

"$PY" scripts/verify_pdv_valor_rs_modal_path.py | tee /tmp/cutover-0910f-valor.log | tail -8
grep -q 'OK=39 FAIL=0' /tmp/cutover-0910f-valor.log
grep -q 'PREP_FAILS=0' /tmp/cutover-0910f-valor.log

echo "OK tip PREP pronto · migrate NÃO · rollback $ROLLBACK_TAG · backup $BACKUP_BR"

if [[ "${1:-}" != "--exec" ]]; then
  echo
  echo "Dry-run OK. Na senha (frase + $SENHA_OK):"
  echo "  AGRO_LOJA_FRASE='pode subir para produção' AGRO_LOJA_SENHA='$SENHA_OK' $0 --exec"
  exit 0
fi

FRASE="${AGRO_LOJA_FRASE:-}"
SENHA="${AGRO_LOJA_SENHA:-}"
if [[ "$SENHA" != "$SENHA_OK" ]]; then
  echo "FAIL: AGRO_LOJA_SENHA incorreta ou ausente — não sobe"
  exit 3
fi
if ! echo "$FRASE" | grep -Eiq 'pode.*(produ[cç][aã]o|producao)|sobe.*(produ[cç][aã]o|producao)|produ[cç][aã]o'; then
  echo "FAIL: AGRO_LOJA_FRASE sem autorização clara de produção — não sobe"
  exit 3
fi

echo "== cutover producao → $PREP =="
git push origin "$PROD_REF:refs/heads/$BACKUP_BR" || true
git checkout producao 2>/dev/null || git checkout -b producao "$PROD_REF"
git reset --hard "$PREP_REF"
git push origin producao
echo "PUSH OK · Render Live · Ctrl+F5 · badge v26.76"
echo "Smoke: botão 230 cadastro · etiqueta 230… · wizard Enter → R\$"
