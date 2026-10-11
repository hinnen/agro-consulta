#!/usr/bin/env bash
# Cutover loja — CHECKLIST 11/10m · Vila abertura 01/08 + domingo zero · v27.06 · SEM migrate
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

PREP_REF="origin/deploy/prep-checklist-1110m"
PROD_REF="origin/producao"
ROLLBACK_TAG="rollback/pre-checklist-1110m-v27.04-f590"
BACKUP_BR="producao-backup-pre-v2706-checklist-1110m"
SENHA_OK="99738595"
VER_ESPERADA="27.06"

git fetch origin producao deploy/prep-checklist-1110m "$ROLLBACK_TAG" 2>/dev/null \
  || git fetch origin producao deploy/prep-checklist-1110m

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
[[ -z "$MIG" ]] || { echo "FAIL: migration no pacote: $MIG"; exit 2; }

FORBIDDEN="$(git diff --name-only "$PROD_REF...$PREP_REF" | grep -E \
  'pdv_wizard\.js|busca_filtro_pdv|motor_busca_unificado|caixa_|mongo_financeiro' || true)"
[[ -z "$FORBIDDEN" ]] || { echo "FAIL: sensível: $FORBIDDEN"; exit 2; }

PREP_FULL="$(git rev-parse "$PREP_REF")"
git grep -q '_dashboard_meta_c_vila_domingo_fechado' "$PREP_FULL" -- produtos/views.py || {
  echo "FAIL: delta sem domingo Vila fechado"; exit 2;
}
git grep -q '2026-08-01' "$PREP_FULL" -- produtos/views.py || {
  echo "FAIL: delta sem default abertura 01/08"; exit 2;
}

PY=python3
[[ -x .venv/bin/python ]] && PY=.venv/bin/python

"$PY" -c "
import scripts.verify_meta_c_vila_abertura as v
for fn in (
    'test_static', 'test_clip_unitario', 'test_soma_e_serie', 'test_vendas_lojas_wiring',
    'test_ocorrencia_mes', 'test_ramp_vila', 'test_regressao_centro_e_rollback',
):
    getattr(v, fn)()
assert v._FAIL == 0, f'abertura static fails={v._FAIL}'
print('META VILA static OK', v._OK)
" | tee /tmp/cutover-1110m-abertura.log
grep -q 'META VILA static OK' /tmp/cutover-1110m-abertura.log

"$PY" -c "
import scripts.verify_meta_c_vila_ramp_deep as d
d.test_wiring()
d.test_unit_ramp()
assert d._FAIL == 0, f'ramp static fails={d._FAIL}'
print('META VILA ramp static OK', d._OK)
" | tee /tmp/cutover-1110m-ramp.log
grep -q 'META VILA ramp static OK' /tmp/cutover-1110m-ramp.log

"$PY" manage.py test produtos.tests_vendas_lojas_ritmo -v0 2>&1 | tee /tmp/cutover-1110m-ritmo.log
grep -qE 'OK|passed' /tmp/cutover-1110m-ritmo.log

if [[ -f scripts/verify_cb_loja_pdv_busca_path.py ]]; then
  "$PY" scripts/verify_cb_loja_pdv_busca_path.py | tee /tmp/cutover-1110m-pdv.log
  grep -qE 'PREP_FAILS=0|FAIL=0' /tmp/cutover-1110m-pdv.log
fi

echo "OK: PREP v$VER_ESPERADA · 11/10m · rollback $ROLLBACK_TAG"
echo "Render: remover AGRO_VILA_ABERTURA=2026-07-20 se existir (ver ROLLBACK-BI-META-C-VILA.md)"
echo "Pós-deploy: ./scripts/post_deploy_checklist_1110m.sh"

if [[ "${1:-}" != "--exec" ]]; then exit 0; fi

FRASE="${AGRO_PIN_FRASE:-${AGRO_LOJA_FRASE:-}}"
SENHA="${AGRO_LOJA_SENHA:-}"
[[ "$SENHA" == "$SENHA_OK" ]] || { echo "FAIL: senha"; exit 3; }
echo "$FRASE" | grep -Eiq 'pode.*(produ[cç][aã]o|producao)|sobe.*(produ[cç][aã]o|producao)|subir.*(produ[cç][aã]o|producao)' \
  || { echo "FAIL: autorização"; exit 3; }

git push origin "$PROD_REF:refs/heads/$BACKUP_BR" || true
git push origin "$PREP_REF:producao"
echo "PUSH OK · badge v$VER_ESPERADA · Ctrl+F5 BI /meta/"
