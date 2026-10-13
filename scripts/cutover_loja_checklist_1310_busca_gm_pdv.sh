#!/usr/bin/env bash
# Cutover loja — CADASTRO-BUSCA-GM-PDV · CHECKLIST 13/10 · v27.14.3 hotfix · SEM migrate
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

PREP_REF="origin/deploy/prep-checklist-1310-busca-gm-pdv"
PROD_REF="origin/producao"
ROLLBACK_TAG="rollback/pre-busca-gm-pdv-v2713"
BACKUP_BR="producao-backup-pre-v27142-busca-gm-pdv"
SENHA_OK="99738595"
VER_ESPERADA="27.14.4"

git fetch origin producao deploy/prep-checklist-1310-busca-gm-pdv "$ROLLBACK_TAG" 2>/dev/null \
  || git fetch origin producao deploy/prep-checklist-1310-busca-gm-pdv

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
  docs/DEPLOY-PREP-CHECKLIST-1310-busca-gm-pdv.md \
  produtos/agro_codigo_barras_loja_util.py \
  produtos/bca_busca_cache_util.py \
  produtos/busca_filtro_pdv_util.py \
  produtos/cadastro_busca_codigo_util.py \
  produtos/catalogo_agro.py \
  produtos/mongo_index_codigos.py \
  produtos/motor_busca_unificado_util.py \
  produtos/tests_cb_loja_bip_busca.py \
  produtos/tests_cadastro_busca_gm_variante.py \
  produtos/tests_cadastro_busca_pdv_alinhada.py \
  produtos/views.py \
  banana.md \
  scripts/cutover_loja_checklist_1310_busca_gm_pdv.sh \
  scripts/post_deploy_checklist_1310_busca_gm_pdv.sh \
  scripts/verify_cadastro_busca_gm_path.py | sort)"
echo "$DELTA" | grep -qx 'VERSION' || {
  echo "FAIL: delta sem VERSION"; exit 2;
}
while IFS= read -r _df; do
  [[ -n "$_df" ]] || continue
  echo "$ALLOWED" | grep -qx "$_df" || {
    echo "FAIL: arquivo fora do pacote: $_df"; echo "$DELTA"; exit 2;
  }
done <<< "$DELTA"

MIG="$(echo "$DELTA" | grep -E '/migrations/[0-9]+_' || true)"
[[ -z "$MIG" ]] || { echo "FAIL: migration: $MIG"; exit 2; }

FORBIDDEN="$(echo "$DELTA" | grep -E \
  'pdv_wizard\.js|caixa_|mongo_financeiro|meta_vendas_util|pdv_state\.js|0145_|clientes_lista|cliente_form\.html' || true)"
[[ -z "$FORBIDDEN" ]] || { echo "FAIL: sensível/fora do pacote: $FORBIDDEN"; exit 2; }

PREP_FULL="$(git rev-parse "$PREP_REF")"
git grep -q '_parse_sort_cadastro_request_explicit' "$PREP_FULL" -- produtos/views.py || {
  echo "FAIL: falta sort explícito cadastro"; exit 2;
}
git grep -q 'contexto_cadastro' "$PREP_FULL" -- produtos/motor_busca_unificado_util.py || {
  echo "FAIL: falta complemento Mongo cadastro"; exit 2;
}
git grep -q 'BCA_BUSCA_CACHE_VERSION = 4' "$PREP_FULL" -- produtos/bca_busca_cache_util.py || {
  echo "FAIL: cache BCA v4"; exit 2;
}
git grep -q 'filtrar_rows_api_estilo_pdv(res, q)' "$PREP_FULL" -- produtos/views.py || {
  echo "FAIL: falta filtro PDV na resposta api/buscar"; exit 2;
}
if echo "$DELTA" | grep -qx 'produtos/views.py'; then
  git diff "$PROD_REF...$PREP_REF" -- produtos/views.py | grep -qE '^\+.*desconto_pdv_percentual' && {
    echo "FAIL: diff views inclui desconto PDV (item 3)"; exit 2;
  } || true
fi

PY=python3
[[ -x .venv/bin/python ]] && PY=.venv/bin/python

"$PY" scripts/verify_cadastro_busca_gm_path.py | tee /tmp/cutover-1310-busca-gm.log
grep -qE '18/18|19/19|16/16' /tmp/cutover-1310-busca-gm.log

"$PY" scripts/verify_pdv_busca_coerente_path.py | tee /tmp/cutover-1310-busca-pdv.log
grep -q '30/30' /tmp/cutover-1310-busca-pdv.log

"$PY" manage.py test produtos.tests_cadastro_busca_gm_variante produtos.tests_cadastro_busca_pdv_alinhada -v0 2>&1 | tee /tmp/cutover-1310-busca-tests.log
grep -qE 'OK|passed' /tmp/cutover-1310-busca-tests.log

if [[ -f scripts/verify_cb_loja_pdv_busca_path.py ]]; then
  "$PY" scripts/verify_cb_loja_pdv_busca_path.py | tee /tmp/cutover-1310-busca-cb230.log
  grep -qE 'PREP_FAILS=0|FAIL=0' /tmp/cutover-1310-busca-cb230.log
fi

echo "OK: PREP v$VER_ESPERADA · CADASTRO-BUSCA-GM-PDV · rollback $ROLLBACK_TAG"
echo "Pós-deploy: ./scripts/post_deploy_checklist_1310_busca_gm_pdv.sh"

if [[ "${1:-}" != "--exec" ]]; then exit 0; fi

FRASE="${AGRO_PIN_FRASE:-${AGRO_LOJA_FRASE:-}}"
SENHA="${AGRO_LOJA_SENHA:-}"
[[ "$SENHA" == "$SENHA_OK" ]] || { echo "FAIL: senha"; exit 3; }
echo "$FRASE" | grep -Eiq 'pode.*(produ[cç][aã]o|producao)|sobe.*(produ[cç][aã]o|producao)|subir.*(produ[cç][aã]o|producao)' \
  || { echo "FAIL: autorização"; exit 3; }

git push origin "$PROD_REF:refs/heads/$BACKUP_BR" || true
git push origin "$PREP_REF:producao"
echo "PUSH OK · badge v$VER_ESPERADA · Ctrl+F5 cadastro ERP + gestão busca"
