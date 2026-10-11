#!/usr/bin/env bash
# Cutover loja — CHECKLIST 11/10j · ETQ importar todas + PWA GM Lojas · v27.00 · SEM migrate
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

PREP_REF="origin/deploy/prep-checklist-1110j"
PROD_REF="origin/producao"
ROLLBACK_TAG="rollback/pre-checklist-1110j-v26.99"
BACKUP_BR="producao-backup-pre-v2700-checklist-1110j"
SENHA_OK="99738595"
VER_ESPERADA="27.00"

git fetch origin producao deploy/prep-checklist-1110j "$ROLLBACK_TAG" 2>/dev/null \
  || git fetch origin producao deploy/prep-checklist-1110j

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
[[ -z "$MIG" ]] || {
  echo "FAIL: este pacote não deve ter migrations: $MIG"; exit 2;
}

# Não tocar PDV / caixa / busca 230
FORBIDDEN="$(git diff --name-only "$PROD_REF...$PREP_REF" | grep -E \
  'pdv_wizard\.js|busca_filtro_pdv|motor_busca_unificado|caixa_|mongo_financeiro' || true)"
[[ -z "$FORBIDDEN" ]] || {
  echo "FAIL: arquivos sensíveis no delta: $FORBIDDEN"; exit 2;
}

git rev-parse "$ROLLBACK_TAG" >/dev/null 2>&1 \
  || git ls-remote --exit-code --tags origin "refs/tags/$ROLLBACK_TAG" >/dev/null \
  || { echo "FAIL: tag rollback ausente $ROLLBACK_TAG"; exit 2; }

PREP_FULL="$(git rev-parse "$PREP_REF")"
git grep -q 'importarTodasSolicitacoesParaFila' "$PREP_FULL" -- produtos/static/produtos/js/produtos_etiquetas.js || {
  echo "FAIL: delta sem importar todas etiquetas"; exit 2;
}
git cat-file -e "$PREP_FULL:produtos/templates/produtos/includes/vendas_lojas_pwa_install.html" 2>/dev/null || {
  echo "FAIL: delta sem PWA install template"; exit 2;
}
git cat-file -e "$PREP_FULL:scripts/verify_vendas_lojas_pwa_install.py" 2>/dev/null || {
  echo "FAIL: delta sem verify PWA"; exit 2;
}

PY=python3
[[ -x .venv/bin/python ]] && PY=.venv/bin/python

"$PY" scripts/verify_etq_solicitacao_vl_pin_path.py | tee /tmp/cutover-1110j-etq.log
grep -qE 'TOTAL OK=31 FAIL=0|TOTAL OK=28 FAIL=0' /tmp/cutover-1110j-etq.log

"$PY" scripts/verify_vendas_lojas_pwa_install.py | tee /tmp/cutover-1110j-pwa.log
grep -qE '0 falha\(s\)|FAIL=0' /tmp/cutover-1110j-pwa.log

if [[ -f scripts/verify_gm_lojas_pin_runtime.py ]]; then
  "$PY" scripts/verify_gm_lojas_pin_runtime.py | tee /tmp/cutover-1110j-runtime.log
  grep -q 'FAIL=0' /tmp/cutover-1110j-runtime.log
fi

if [[ -f scripts/verify_cb_loja_pdv_busca_path.py ]]; then
  "$PY" scripts/verify_cb_loja_pdv_busca_path.py | tee /tmp/cutover-1110j-pdv.log
  grep -q 'FAIL=0' /tmp/cutover-1110j-pdv.log
fi

echo "OK: PREP v$VER_ESPERADA · rollback $ROLLBACK_TAG · SEM migrate"
echo "Pós-deploy: ./scripts/post_deploy_checklist_1110j.sh"

if [[ "${1:-}" != "--exec" ]]; then exit 0; fi

FRASE="${AGRO_LOJA_FRASE:-}"
SENHA="${AGRO_LOJA_SENHA:-}"
[[ "$SENHA" == "$SENHA_OK" ]] || { echo "FAIL: senha"; exit 3; }
echo "$FRASE" | grep -Eiq 'pode.*(produ[cç][aã]o|producao)|sobe.*(produ[cç][aã]o|producao)' \
  || { echo "FAIL: autorização"; exit 3; }

git push origin "$PROD_REF:refs/heads/$BACKUP_BR" || true
git push origin "$PREP_REF:producao"
echo "PUSH OK · badge v$VER_ESPERADA · Ctrl+F5 PDVs (cache etiquetas ?v=35)"
