#!/usr/bin/env bash
# Cutover loja — CHECKLIST 10/10e · CB-230-GM4045-HOTFIX · v26.83
# Dry-run (padrão): prova + gates. Exec: só com frase + senha no env.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

PREP_REF="origin/deploy/prep-checklist-1010e"
PROD_REF="origin/producao"
ROLLBACK_TAG="rollback/pre-checklist-1010e-v26.82"
BACKUP_BR="producao-backup-pre-v2683-checklist-1010e"
SENHA_OK="99738595"
VER_ESPERADA="26.83"

git fetch origin producao deploy/prep-checklist-1010e "$ROLLBACK_TAG" 2>/dev/null \
  || git fetch origin producao deploy/prep-checklist-1010e

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

DELTA="$(git diff --name-only "$PROD_REF...$PREP_REF")"
echo "$DELTA" | grep -q 'produtos/agro_codigo_barras_loja_util.py' || {
  echo "FAIL: delta sem agro_codigo_barras_loja_util.py"; exit 2;
}
NFILES=$(echo "$DELTA" | wc -l | tr -d ' ')
[[ "$NFILES" -le 8 ]] || {
  echo "FAIL: delta grande demais ($NFILES arquivos) — esperado hotfix mínimo"; exit 2;
}

PY=python3
[[ -x .venv/bin/python ]] && PY=.venv/bin/python

"$PY" scripts/verify_cadastro_cb_loja_gerador_path.py | tee /tmp/cutover-1010e-gerador.log
grep -q 'OK 26/26' /tmp/cutover-1010e-gerador.log
grep -q 'PREP_FAILS=0' /tmp/cutover-1010e-gerador.log

"$PY" scripts/verify_cb_loja_bip_variantes_path.py | tee /tmp/cutover-1010e-bip.log
grep -q 'OK 10/10' /tmp/cutover-1010e-bip.log
grep -q 'PREP_FAILS=0' /tmp/cutover-1010e-bip.log

"$PY" scripts/verify_etq_ean_loja_path.py | tee /tmp/cutover-1010e-etq.log
grep -q 'OK 74/74' /tmp/cutover-1010e-etq.log
grep -q 'PREP_FAILS=0' /tmp/cutover-1010e-etq.log

"$PY" manage.py test produtos.tests_cb_loja_colisao_canonica produtos.tests_cb_loja_gerador_save \
  produtos.tests_agro_codigo_barras_loja_util produtos.tests_cb_loja_bip_busca \
  --settings=config.settings 2>&1 | tee /tmp/cutover-1010e-django.log
grep -q 'OK$' /tmp/cutover-1010e-django.log || grep -q 'OK (' /tmp/cutover-1010e-django.log

echo "OK: PREP pronto · migrate NÃO · rollback $ROLLBACK_TAG · backup $BACKUP_BR"
echo "Checkpoint Live atual: $PROD_FULL"
echo "Pós-deploy loja: GM4045 — reatribuir_cb_loja_exclusivo ou botão 230 + nova etiqueta"

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
