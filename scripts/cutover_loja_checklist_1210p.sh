#!/usr/bin/env bash
# Cutover loja — CHECKLIST 12/10p · cadastro VN + MED + cliente % PDV · v27.10
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

PREP_REF="origin/deploy/prep-checklist-1210p"
PROD_REF="origin/producao"
ROLLBACK_TAG="rollback/pre-checklist-1210p-v27.09-f6a4"
BACKUP_BR="producao-backup-pre-v2710-checklist-1210p"
SENHA_OK="99738595"
VER_ESPERADA="27.10"

git fetch origin producao deploy/prep-checklist-1210p "$ROLLBACK_TAG" 2>/dev/null \
  || git fetch origin producao deploy/prep-checklist-1210p

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
[[ "$MIG" == "produtos/migrations/0145_clienteagro_desconto_pdv_percentual.py" ]] || {
  echo "FAIL: migrations inesperadas: $MIG"; exit 2;
}

FORBIDDEN="$(git diff --name-only "$PROD_REF...$PREP_REF" | grep -E \
  'pdv_wizard\.js|busca_filtro_pdv|motor_busca_unificado|caixa_|mongo_financeiro|meta_vendas_util' || true)"
[[ -z "$FORBIDDEN" ]] || { echo "FAIL: sensível: $FORBIDDEN"; exit 2; }

PREP_FULL="$(git rev-parse "$PREP_REF")"
export AGRO_VERIFY_DIFF_TIP="$PREP_FULL"
git cat-file -e "$PREP_FULL:produtos/cadastro_overlay_orfao_util.py" || {
  echo "FAIL: sem cadastro agro vn"; exit 2;
}
git cat-file -e "$PREP_FULL:produtos/data/classificacao_medicamento_vet.json" || {
  echo "FAIL: sem MED JSON"; exit 2;
}
git grep -q 'desconto_pdv_percentual' "$PREP_FULL" -- produtos/models.py || {
  echo "FAIL: sem desconto PDV"; exit 2;
}

PY=python3
[[ -x .venv/bin/python ]] && PY=.venv/bin/python

"$PY" scripts/verify_cadastro_agro_vn_path.py | tee /tmp/cutover-1210p-vn.log
grep -q 'VERIFY_FAIL=0' /tmp/cutover-1210p-vn.log

export AGRO_VERIFY_DIFF_TIP="$PREP_FULL"
"$PY" scripts/verify_classificacao_medicamentos_path.py | tee /tmp/cutover-1210p-med.log
grep -q '^PREP_FAILS=0$' /tmp/cutover-1210p-med.log

"$PY" scripts/verify_cliente_lista_desc_pdv_path.py | tee /tmp/cutover-1210p-cli.log
grep -qE '0 FAIL ===|· 0 FAIL ===' /tmp/cutover-1210p-cli.log

"$PY" -c "
import scripts.verify_cliente_planilha_path as v
v.test_media_mensal_calendario()
v.test_arquivos()
v.test_export_xlsx_bytes()
v.test_valores_mes()
v.test_import_roundtrip()
v.test_patch_whatsapp_contratos()
v.test_import_whatsapp_vazio_mock()
assert not v.fails, v.fails
print('planilha_static OK', len(v.oks))
" | tee /tmp/cutover-1210p-plan.log
grep -q 'planilha_static OK' /tmp/cutover-1210p-plan.log

if [[ -f scripts/verify_cb_loja_pdv_busca_path.py ]]; then
  "$PY" scripts/verify_cb_loja_pdv_busca_path.py | tee /tmp/cutover-1210p-pdv.log
  grep -qE 'PREP_FAILS=0|FAIL=0' /tmp/cutover-1210p-pdv.log
fi

echo "OK: PREP v$VER_ESPERADA · 12/10p · rollback $ROLLBACK_TAG"
echo "Pós-deploy: ./scripts/post_deploy_checklist_1210p.sh"

if [[ "${1:-}" != "--exec" ]]; then exit 0; fi

FRASE="${AGRO_PIN_FRASE:-${AGRO_LOJA_FRASE:-}}"
SENHA="${AGRO_LOJA_SENHA:-}"
[[ "$SENHA" == "$SENHA_OK" ]] || { echo "FAIL: senha"; exit 3; }
echo "$FRASE" | grep -Eiq 'pode.*(produ[cç][aã]o|producao)|sobe.*(produ[cç][aã]o|producao)|subir.*(produ[cç][aã]o|producao)' \
  || { echo "FAIL: autorização"; exit 3; }

git push origin "$PROD_REF:refs/heads/$BACKUP_BR" || true
git push origin "$PREP_REF:producao"
echo "PUSH OK · badge v$VER_ESPERADA · migrate 0145 + limpar_overlays_agro_vn_fantasma"
