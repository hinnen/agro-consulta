#!/usr/bin/env bash
# Cutover 11/11 · fornecedor da nota + Point pago + PIN da saída · v27.26.55
# Não empurra produção sem --exec e sem frase+senha.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
PREP_REF="origin/deploy/prep-checklist-1111-loja-3"
PROD_REF="origin/producao"
ROLLBACK_TAG="rollback/pre-v272655-1111-loja-3"
BACKUP_BR="producao-backup-pre-v272655-1111-loja-3"
SENHA_OK="99738595"
VER_ESPERADA="27.26.55"

git fetch origin producao deploy/prep-checklist-1111-loja-3 "$ROLLBACK_TAG" 2>/dev/null \
  || git fetch origin producao deploy/prep-checklist-1111-loja-3

PROD="$(git rev-parse --short "$PROD_REF")"
PREP="$(git rev-parse --short "$PREP_REF" 2>/dev/null || echo missing)"
VER="$(git show "$PREP_REF:VERSION" 2>/dev/null | tr -d '[:space:]' || echo ?)"
echo "producao=$PROD PREP=$PREP VERSION=$VER"

git merge-base --is-ancestor "$PROD_REF" "$PREP_REF" || { echo "FAIL: PREP não é descendente de produção"; exit 2; }
[[ "$VER" == "$VER_ESPERADA" ]] || { echo "FAIL: VERSION=$VER esperado $VER_ESPERADA"; exit 2; }

DELTA="$(git diff --name-only "$PROD_REF...$PREP_REF" | sort)"
ALLOWED="$(printf '%s\n' \
  VERSION \
  banana-roteiro.md \
  config/settings.py \
  docs/DEPLOY-PREP-CHECKLIST-0911-nf-forn-unif-agro.md \
  docs/DEPLOY-PREP-CHECKLIST-1111-loja-3.md \
  estoque/views.py \
  produtos/admin.py \
  produtos/caixa_util.py \
  produtos/fornecedor_entrada_nfe_util.py \
  produtos/gestao_logs_catalog.py \
  produtos/gestao_logs_util.py \
  produtos/middleware_pdv_operacao_log.py \
  produtos/migrations/0155_pdv_operacao_log_agro.py \
  produtos/models.py \
  produtos/pdv_operacao_log_util.py \
  produtos/static/produtos/js/pdv_wizard.js \
  produtos/templates/produtos/entrada_nota.html \
  produtos/tests_mp_point_pin_forcar.py \
  produtos/tests_pdv_operacao_log.py \
  produtos/vendas_lojas_ajuste_sessao_util.py \
  produtos/vendas_lojas_hub_acesso_util.py \
  produtos/vendas_lojas_hub_sessao_util.py \
  produtos/views.py \
  produtos/views_gestao_logs.py \
  produtos/views_mp_point.py \
  produtos/views_nfe_saida.py \
  rh/services/salario_financeiro_mongo.py \
  scripts/cutover_loja_checklist_0911_nf_forn_unif_agro.sh \
  scripts/cutover_loja_checklist_1111_loja_3.sh \
  scripts/post_deploy_checklist_0911_nf_forn_unif_agro.sh \
  scripts/post_deploy_checklist_1111_loja_3.sh \
  scripts/prep_checkpoint_checklist_0911_nf_forn_unif_agro.sh \
  scripts/prep_checkpoint_checklist_1111_loja_3.sh \
  scripts/verify_fornecedor_entrada_nfe_alias_path.py \
  scripts/verify_mp_point_orfao_pago_pdv_log_path.py \
  scripts/verify_nf_forn_unif_ajuste_path.py \
  scripts/verify_pin_operador_saida_nathan_path.py | sort)"
[[ "$DELTA" == "$ALLOWED" ]] || { echo "FAIL: delta:"; echo "$DELTA"; exit 2; }

MIG="$(echo "$DELTA" | grep -E '/migrations/[0-9]+_' || true)"
[[ "$MIG" == "produtos/migrations/0155_pdv_operacao_log_agro.py" ]] || { echo "FAIL: migrate $MIG"; exit 2; }
grep -q '0154_whatsapp_public_origin' produtos/migrations/0155_pdv_operacao_log_agro.py

PROD_SHA="$(git rev-parse "$PROD_REF")"
git show-ref --verify --quiet "refs/remotes/origin/$ROLLBACK_TAG" || {
  echo "FAIL: rode ./scripts/prep_checkpoint_checklist_1111_loja_3.sh"; exit 2;
}
[[ "$(git rev-parse origin/$ROLLBACK_TAG)" == "$PROD_SHA" ]] || {
  echo "FAIL: rollback desatualizado"; exit 2;
}

PY=python3
[[ -x .venv/bin/python ]] && PY=.venv/bin/python

"$PY" scripts/verify_nf_forn_unif_ajuste_path.py | tee /tmp/cutover-1111-nf.log
grep -q VERIFY_OK /tmp/cutover-1111-nf.log
"$PY" scripts/verify_fornecedor_entrada_nfe_alias_path.py | tee /tmp/cutover-1111-alias.log
grep -qE 'VERIFY_OK fornecedor_entrada_nfe_alias' /tmp/cutover-1111-alias.log
"$PY" scripts/verify_mp_point_orfao_pago_pdv_log_path.py | tee /tmp/cutover-1111-mp.log
grep -q VERIFY_OK /tmp/cutover-1111-mp.log
"$PY" scripts/verify_pin_operador_saida_nathan_path.py | tee /tmp/cutover-1111-pin.log
grep -q VERIFY_OK /tmp/cutover-1111-pin.log

echo "OK: PREP v$VER_ESPERADA · 11/11 três pacotes · migrate 0155 · rollback $ROLLBACK_TAG"
echo "Pós-deploy Render: ./scripts/post_deploy_checklist_1111_loja_3.sh"

[[ "${1:-}" != "--exec" ]] && exit 0
FRASE="${AGRO_PIN_FRASE:-${AGRO_LOJA_FRASE:-}}"
SENHA="${AGRO_LOJA_SENHA:-}"
[[ "$SENHA" == "$SENHA_OK" ]] || { echo "FAIL: senha"; exit 3; }
echo "$FRASE" | grep -Eiq 'pode.*(produ[cç][aã]o|producao|enviar)|sobe.*(produ[cç][aã]o|producao)|subir.*(produ[cç][aã]o|producao)|enviar.*(produ[cç][aã]o|producao)' \
  || { echo "FAIL: autorização"; exit 3; }
git push origin "$PROD_REF:refs/heads/$ROLLBACK_TAG" || true
git push origin "$PROD_REF:refs/heads/$BACKUP_BR" || true
git push origin "$PREP_REF:producao"
echo "PUSH OK v$VER_ESPERADA · Render Shell: ./scripts/post_deploy_checklist_1111_loja_3.sh"
echo "REVERT: git push origin $ROLLBACK_TAG:producao"
