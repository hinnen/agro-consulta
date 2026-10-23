#!/usr/bin/env bash
# Cutover loja — ETQ-FILTRO-NF-ENTRADA · CHECKLIST 16/10c · v27.22 · SEM migrate
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

PREP_REF="origin/deploy/prep-checklist-1610c-etq-filtro-nf"
PROD_REF="origin/producao"
ROLLBACK_TAG="rollback/pre-v2721-etq-filtro-nf"
BACKUP_BR="producao-backup-pre-v2722-etq-filtro-nf"
SENHA_OK="99738595"
VER_ESPERADA="27.22"

_assert_version_clean() {
  local raw="$1"
  [[ "$raw" != *"<<<<<<"* ]] && [[ "$raw" != *"======="* ]] && [[ "$raw" != *">>>>>>"* ]] || {
    echo "FAIL: VERSION inválido (conflito merge): $raw"; exit 2;
  }
}

git fetch origin producao deploy/prep-checklist-1610c-etq-filtro-nf "$ROLLBACK_TAG" 2>/dev/null \
  || git fetch origin producao deploy/prep-checklist-1610c-etq-filtro-nf

PROD="$(git rev-parse --short "$PROD_REF")"
PREP="$(git rev-parse --short "$PREP_REF")"
VER="$(git show "$PREP_REF:VERSION" | tr -d '[:space:]')"
echo "producao=$PROD PREP=$PREP VERSION=$VER"
_assert_version_clean "$VER"

git merge-base --is-ancestor "$PROD_REF" "$PREP_REF" || {
  echo "FAIL: produção não é ancestral do PREP"; exit 2;
}
[[ "$VER" == "$VER_ESPERADA" ]] || {
  echo "FAIL: VERSION=$VER (esperado $VER_ESPERADA)"; exit 2;
}

DELTA="$(git diff --name-only "$PROD_REF...$PREP_REF" | sort)"
ALLOWED="$(printf '%s\n' \
  VERSION \
  banana.md \
  docs/DEPLOY-PREP-CHECKLIST-1610c-etq-filtro-nf.md \
  produtos/cadastro_filtros_util.py \
  produtos/static/produtos/js/produtos_etiquetas.js \
  produtos/templates/produtos/produtos_etiquetas.html \
  produtos/tests_pdv_cadastro_rapido.py \
  produtos/views.py \
  scripts/cutover_loja_checklist_1610c_etq_filtro_nf.sh \
  scripts/post_deploy_checklist_1610c_etq_filtro_nf.sh \
  scripts/verify_etq_filtro_nf_entrada_path.py | sort)"
[[ "$DELTA" == "$ALLOWED" ]] || {
  echo "FAIL: delta inesperado:"; echo "$DELTA"; exit 2;
}

MIG="$(echo "$DELTA" | grep -E '/migrations/[0-9]+_' || true)"
[[ -z "$MIG" ]] || { echo "FAIL: migration: $MIG"; exit 2; }

PY=python3
[[ -x .venv/bin/python ]] && PY=.venv/bin/python
export AGRO_PIN_TESTE="${AGRO_PIN_TESTE:-9973}"

"$PY" scripts/verify_etq_filtro_nf_entrada_path.py | tee /tmp/cutover-1610c-etq-nf.log
grep -q VERIFY_OK /tmp/cutover-1610c-etq-nf.log

echo "OK: PREP v$VER_ESPERADA · ETQ-FILTRO-NF-ENTRADA · rollback $ROLLBACK_TAG"

if [[ "${1:-}" != "--exec" ]]; then exit 0; fi

FRASE="${AGRO_PIN_FRASE:-${AGRO_LOJA_FRASE:-}}"
SENHA="${AGRO_LOJA_SENHA:-}"
[[ "$SENHA" == "$SENHA_OK" ]] || { echo "FAIL: senha"; exit 3; }
echo "$FRASE" | grep -Eiq 'pode.*(produ[cç][aã]o|producao|enviar)|sobe.*(produ[cç][aã]o|producao)|subir.*(produ[cç][aã]o|producao)|enviar.*(produ[cç][aã]o|producao)' \
  || { echo "FAIL: autorização"; exit 3; }

git push origin "$PROD_REF:refs/heads/$BACKUP_BR" || true
git push origin "$PREP_REF:producao"
echo "PUSH OK · badge v$VER_ESPERADA · Ctrl+F5 Etiquetas → Nº NF entrada"
