#!/usr/bin/env bash
# Cutover loja — PDV-BALANCA-QTY-VALOR (v26.74).
# SEM senha: só verifica se o tip PREP está pronto (rápido).
# COM senha: sobe producao = tip PREP (mínima pausa).
#
# Uso:
#   ./scripts/cutover_loja_qty_valor.sh              # dry-run / prova
#   AGRO_LOJA_FRASE='pode subir para produção' \
#   AGRO_LOJA_SENHA='99738595' \
#   ./scripts/cutover_loja_qty_valor.sh --exec
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
PREP_REF="origin/deploy/prep-pdv-balanca-agro-pg"
PROD_REF="origin/producao"
ROLLBACK_TAG="rollback/pre-pdv-balanca-qty-valor-v26.71"
BACKUP_BR="producao-backup-pre-v2674-qty-valor"
SENHA_OK="99738595"

echo "== fetch =="
git fetch origin producao deploy/prep-pdv-balanca-agro-pg "$ROLLBACK_TAG" 2>/dev/null || git fetch origin producao deploy/prep-pdv-balanca-agro-pg

PROD="$(git rev-parse --short "$PROD_REF")"
PREP="$(git rev-parse --short "$PREP_REF")"
VER="$(git show "$PREP_REF:VERSION" | tr -d '[:space:]')"
echo "producao=$PROD  PREP=$PREP  VERSION=$VER"

if ! git merge-base --is-ancestor "$PROD_REF" "$PREP_REF"; then
  echo "FAIL: producao não é ancestral do PREP — abortar cutover"
  exit 2
fi
if [[ "$VER" != "26.74" ]]; then
  echo "FAIL: VERSION PREP=$VER (esperado 26.74)"
  exit 2
fi
MIG="$(git diff --name-only "$PROD_REF...$PREP_REF" | grep -ci migration || true)"
if [[ "${MIG:-0}" != "0" ]]; then
  echo "FAIL: há migrations no delta — não é cutover rápido"
  exit 2
fi
if ! git rev-parse "$ROLLBACK_TAG" >/dev/null 2>&1 && ! git rev-parse "origin/$ROLLBACK_TAG" >/dev/null 2>&1; then
  # tag pode existir só no remoto
  if ! git ls-remote --exit-code --tags origin "refs/tags/$ROLLBACK_TAG" >/dev/null 2>&1; then
    echo "FAIL: falta tag $ROLLBACK_TAG"
    exit 2
  fi
fi

echo "== provas path =="
if [[ -x .venv/bin/python ]]; then
  PY=.venv/bin/python
else
  PY=python3
fi
"$PY" scripts/verify_pdv_balanca_qty_valor_path.py | tee /tmp/cutover-qty-valor.log | tail -5
grep -q 'OK=63 FAIL=0' /tmp/cutover-qty-valor.log
grep -q 'PREP_FAILS=0' /tmp/cutover-qty-valor.log

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
# Frase: qualquer texto com 'produ' (produção/producao) + 'pode'/'sobe'
if ! echo "$FRASE" | grep -Eiq 'pode.*(produ[cç][aã]o|producao)|sobe.*(produ[cç][aã]o|producao)|produ[cç][aã]o'; then
  echo "FAIL: AGRO_LOJA_FRASE sem autorização clara de produção — não sobe"
  exit 3
fi

echo "== cutover producao → $PREP =="
# backup tip atual (idempotente)
git push origin "$PROD_REF:refs/heads/$BACKUP_BR" || true
git checkout producao
git reset --hard "$PREP_REF"
git push origin producao
echo "PUSH OK · acompanhar Render Live · Ctrl+F5 · badge v26.74"
echo "Smoke: /pdv/checkout/ bip 2001000004812 → qty≈0,512 · nome R\$10 Enter"
