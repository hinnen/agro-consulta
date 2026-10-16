#!/usr/bin/env bash
# Cutover loja — CLIENTE-LISTA-DESC-PDV · CHECKLIST 15/10a · v27.20 · COM migrate 0145
# Deploy **depois** de 15/10 v27.19 se possível (mesmo pause: dois push + migrate).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

PREP_REF="origin/deploy/prep-checklist-1510a-cliente-desc-pdv"
PROD_REF="origin/producao"
ROLLBACK_TAG="rollback/pre-v2719-cliente-desc-pdv"
BACKUP_BR="producao-backup-pre-v2720-cliente-desc-pdv"
SENHA_OK="99738595"
VER_ESPERADA="27.20"
MIG_ESPERADA="0145_clienteagro_desconto_pdv_percentual"

git fetch origin producao deploy/prep-checklist-1510a-cliente-desc-pdv "$ROLLBACK_TAG" 2>/dev/null \
  || git fetch origin producao deploy/prep-checklist-1510a-cliente-desc-pdv

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

DELTA="$(git diff --name-only "$PROD_REF...$PREP_REF" | sort)"
ALLOWED="$(printf '%s\n' \
  VERSION \
  docs/DEPLOY-PREP-CHECKLIST-1510a-cliente-desc-pdv.md \
  produtos/cliente_planilha_util.py \
  produtos/forms.py \
  produtos/migrations/0145_clienteagro_desconto_pdv_percentual.py \
  produtos/models.py \
  produtos/static/produtos/js/pdv_state.js \
  produtos/templates/produtos/cliente_form.html \
  produtos/templates/produtos/clientes_lista.html \
  produtos/views.py \
  scripts/cutover_loja_checklist_1510a_cliente_desc_pdv.sh \
  scripts/post_deploy_checklist_1510a_cliente_desc_pdv.sh \
  scripts/verify_cliente_lista_desc_pdv_path.py \
  scripts/verify_cliente_planilha_path.py | sort)"
[[ "$DELTA" == "$ALLOWED" ]] || {
  echo "FAIL: delta inesperado:"; echo "$DELTA"; exit 2;
}

MIG="$(echo "$DELTA" | grep -E '/migrations/[0-9]+_' || true)"
echo "$MIG" | grep -q "$MIG_ESPERADA" || {
  echo "FAIL: migration esperada $MIG_ESPERADA"; exit 2;
}

FORBIDDEN="$(echo "$DELTA" | grep -E \
  'pdv_wizard\.js|busca_filtro_pdv|caixa_util|mongo_financeiro|meta_vendas_util' || true)"
[[ -z "$FORBIDDEN" ]] || { echo "FAIL: sensível: $FORBIDDEN"; exit 2; }

PREP_FULL="$(git rev-parse "$PREP_REF")"
git grep -q 'desconto_pdv_percentual' "$PREP_FULL" -- produtos/views.py produtos/models.py || {
  echo "FAIL: falta campo desconto PDV"; exit 2;
}

PY=python3
[[ -x .venv/bin/python ]] && PY=.venv/bin/python
export AGRO_PIN_TESTE="${AGRO_PIN_TESTE:-9973}"

"$PY" scripts/verify_cliente_lista_desc_pdv_path.py | tee /tmp/cutover-1510a-cli.log
grep -qE '18/18|0 FAIL' /tmp/cutover-1510a-cli.log

"$PY" scripts/verify_cliente_planilha_path.py | tee /tmp/cutover-1510a-plan.log
grep -qE '61/61|0 FAIL' /tmp/cutover-1510a-plan.log

echo "OK: PREP v$VER_ESPERADA · CLIENTE-LISTA-DESC-PDV · rollback $ROLLBACK_TAG"
echo "Pós-deploy: ./scripts/post_deploy_checklist_1510a_cliente_desc_pdv.sh"

if [[ "${1:-}" != "--exec" ]]; then exit 0; fi

FRASE="${AGRO_PIN_FRASE:-${AGRO_LOJA_FRASE:-}}"
SENHA="${AGRO_LOJA_SENHA:-}"
[[ "$SENHA" == "$SENHA_OK" ]] || { echo "FAIL: senha"; exit 3; }
echo "$FRASE" | grep -Eiq 'pode.*(produ[cç][aã]o|producao|enviar)|sobe.*(produ[cç][aã]o|producao)|subir.*(produ[cç][aã]o|producao)|enviar.*(produ[cç][aã]o|producao)' \
  || { echo "FAIL: autorização"; exit 3; }

git push origin "$PROD_REF:refs/heads/$BACKUP_BR" || true
git push origin "$PREP_REF:producao"
echo "PUSH OK · Render Shell: python manage.py migrate --noinput"
