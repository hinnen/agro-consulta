#!/usr/bin/env bash
# Cutover loja — CHECKLIST 10/10k · PDV-BUSCA-EAN230 · v26.89
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

PREP_REF="origin/deploy/prep-checklist-1010k"
PROD_REF="origin/producao"
ROLLBACK_TAG="rollback/pre-checklist-1010k-v26.88"
BACKUP_BR="producao-backup-pre-v2689-checklist-1010k"
SENHA_OK="99738595"
VER_ESPERADA="26.89"

git fetch origin producao deploy/prep-checklist-1010k "$ROLLBACK_TAG" 2>/dev/null \
  || git fetch origin producao deploy/prep-checklist-1010k

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

git show "$PREP_REF:produtos/busca_filtro_pdv_util.py" | grep -q 'termo_eh_ean_loja_bip_valido' || {
  echo "FAIL: delta sem filtro EAN loja PDV"; exit 2;
}

PY=python3
[[ -x .venv/bin/python ]] && PY=.venv/bin/python

"$PY" scripts/verify_cb_loja_legado_migrate_path.py | tee /tmp/cutover-1010k-legado.log
grep -q 'OK 15/15' /tmp/cutover-1010k-legado.log
grep -q 'PREP_FAILS=0' /tmp/cutover-1010k-legado.log

"$PY" scripts/verify_cb_loja_bip_variantes_path.py | tee /tmp/cutover-1010k-bip.log
grep -q 'OK 10/10' /tmp/cutover-1010k-bip.log
grep -q 'PREP_FAILS=0' /tmp/cutover-1010k-bip.log

"$PY" scripts/verify_cb_loja_pdv_busca_path.py | tee /tmp/cutover-1010k-pdv.log
grep -q 'OK 10/10' /tmp/cutover-1010k-pdv.log
grep -q 'PREP_FAILS=0' /tmp/cutover-1010k-pdv.log

"$PY" manage.py test produtos.tests_cb_loja_legado_gm0024 produtos.tests_cb_loja_legado_lote \
  produtos.tests_cb_loja_legado_grupo_migracao produtos.tests_cb_loja_legado_liberar_intruso \
  produtos.tests_cb_loja_colisao_canonica produtos.tests_cb_loja_gerador_save \
  produtos.tests_agro_codigo_barras_loja_util produtos.tests_cb_loja_bip_busca \
  --settings=config.settings 2>&1 | tee /tmp/cutover-1010k-django.log
grep -q 'OK$' /tmp/cutover-1010k-django.log || grep -q 'OK (' /tmp/cutover-1010k-django.log

echo "OK: PREP pronto · migrate NÃO · rollback $ROLLBACK_TAG · backup $BACKUP_BR"
echo "Pós-deploy: Ctrl+F5 PDV · bip 2300000001556 → GM0024-P (migração já rodou na v26.88)"

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
echo "PUSH OK · aguardar Render · Ctrl+F5 · badge v$VER_ESPERADA"
