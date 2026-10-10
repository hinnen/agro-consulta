#!/usr/bin/env bash
# Cutover loja — CHECKLIST 10/10i · CB-230-LEGADO-POS-GRUPO · v26.87
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

PREP_REF="origin/deploy/prep-checklist-1010i"
PROD_REF="origin/producao"
ROLLBACK_TAG="rollback/pre-checklist-1010i-v26.86"
BACKUP_BR="producao-backup-pre-v2687-checklist-1010i"
SENHA_OK="99738595"
VER_ESPERADA="26.87"

git fetch origin producao deploy/prep-checklist-1010i "$ROLLBACK_TAG" 2>/dev/null \
  || git fetch origin producao deploy/prep-checklist-1010i

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
echo "$DELTA" | grep -q 'validar_codigo_barras_loja_pos_grupo_migracao' || {
  echo "FAIL: delta sem validação pós-grupo"; exit 2;
}
NFILES=$(echo "$DELTA" | wc -l | tr -d ' ')
[[ "$NFILES" -le 14 ]] || {
  echo "FAIL: delta grande demais ($NFILES arquivos)"; exit 2;
}

PY=python3
[[ -x .venv/bin/python ]] && PY=.venv/bin/python

"$PY" scripts/verify_cb_loja_legado_migrate_path.py | tee /tmp/cutover-1010i-legado.log
grep -q 'OK 14/14' /tmp/cutover-1010i-legado.log
grep -q 'PREP_FAILS=0' /tmp/cutover-1010i-legado.log

"$PY" scripts/verify_cb_loja_bip_variantes_path.py | tee /tmp/cutover-1010i-bip.log
grep -q 'OK 10/10' /tmp/cutover-1010i-bip.log
grep -q 'PREP_FAILS=0' /tmp/cutover-1010i-bip.log

"$PY" manage.py test produtos.tests_cb_loja_legado_gm0024 produtos.tests_cb_loja_legado_lote \
  produtos.tests_cb_loja_legado_grupo_migracao produtos.tests_cb_loja_legado_liberar_intruso \
  produtos.tests_cb_loja_colisao_canonica produtos.tests_cb_loja_gerador_save \
  produtos.tests_agro_codigo_barras_loja_util produtos.tests_cb_loja_bip_busca \
  --settings=config.settings 2>&1 | tee /tmp/cutover-1010i-django.log
grep -q 'OK$' /tmp/cutover-1010i-django.log || grep -q 'OK (' /tmp/cutover-1010i-django.log

echo "OK: PREP pronto · migrate NÃO · rollback $ROLLBACK_TAG · backup $BACKUP_BR"
echo "Checkpoint Live atual: $PROD_FULL"
echo "Pós-deploy Shell: python manage.py migrar_cb_loja_legado --liberar-intruso"

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
