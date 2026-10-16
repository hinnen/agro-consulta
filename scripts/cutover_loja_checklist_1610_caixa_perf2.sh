#!/usr/bin/env bash
# Cutover loja — CAIXA-FECHAR-PERF-2 · CHECKLIST 16/10 · v27.21 · SEM migrate
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

PREP_REF="origin/deploy/prep-checklist-1610-caixa-perf2"
PROD_REF="origin/producao"
ROLLBACK_TAG="rollback/pre-v2720-caixa-perf2"
BACKUP_BR="producao-backup-pre-v2721-caixa-perf2"
SENHA_OK="99738595"
VER_ESPERADA="27.21"

_assert_version_clean() {
  local raw="$1"
  [[ "$raw" != *"<<<<<<"* ]] && [[ "$raw" != *"======="* ]] && [[ "$raw" != *">>>>>>"* ]] || {
    echo "FAIL: VERSION inválido (conflito merge): $raw"; exit 2;
  }
}

git fetch origin producao deploy/prep-checklist-1610-caixa-perf2 "$ROLLBACK_TAG" 2>/dev/null \
  || git fetch origin producao deploy/prep-checklist-1610-caixa-perf2

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
  docs/DEPLOY-PREP-CHECKLIST-1610-caixa-perf2.md \
  produtos/caixa_util.py \
  produtos/repasse_vila_util.py \
  produtos/views.py \
  scripts/cutover_loja_checklist_1610_caixa_perf2.sh \
  scripts/post_deploy_checklist_1610_caixa_perf2.sh \
  scripts/verify_caixa_devolucao_dinheiro_mp_path.py \
  scripts/verify_caixa_fechar_loja_path.py \
  scripts/verify_caixa_fechar_repasse_path.py | sort)"
[[ "$DELTA" == "$ALLOWED" ]] || {
  echo "FAIL: delta inesperado:"; echo "$DELTA"; exit 2;
}

MIG="$(echo "$DELTA" | grep -E '/migrations/[0-9]+_' || true)"
[[ -z "$MIG" ]] || { echo "FAIL: migration: $MIG"; exit 2; }

FORBIDDEN="$(echo "$DELTA" | grep -E \
  'pdv_wizard\.js|busca_filtro_pdv|pdv_state\.js|0145_|mongo_financeiro' || true)"
[[ -z "$FORBIDDEN" ]] || { echo "FAIL: sensível: $FORBIDDEN"; exit 2; }

PREP_FULL="$(git rev-parse "$PREP_REF")"
git grep -q 'reserva_leve_estado_caixa_fechar' "$PREP_FULL" -- produtos/caixa_util.py || {
  echo "FAIL: falta reserva_leve_estado_caixa_fechar"; exit 2;
}

PY=python3
[[ -x .venv/bin/python ]] && PY=.venv/bin/python
export AGRO_PIN_TESTE="${AGRO_PIN_TESTE:-9973}"

"$PY" scripts/verify_caixa_fechar_loja_path.py | tee /tmp/cutover-1610-caixa.log
grep -q VERIFY_FECHAR_LOJA_OK /tmp/cutover-1610-caixa.log

"$PY" scripts/verify_caixa_fechar_repasse_path.py | tee /tmp/cutover-1610-rep.log
grep -q VERIFY_FECHAR_REPASSE_OK /tmp/cutover-1610-rep.log

"$PY" scripts/verify_caixa_devolucao_dinheiro_mp_path.py | tee /tmp/cutover-1610-dev.log
grep -q VERIFY_CAIXA_DEVOL_DINHEIRO_MP_OK /tmp/cutover-1610-dev.log

echo "OK: PREP v$VER_ESPERADA · CAIXA-FECHAR-PERF-2 · rollback $ROLLBACK_TAG"

if [[ "${1:-}" != "--exec" ]]; then exit 0; fi

FRASE="${AGRO_PIN_FRASE:-${AGRO_LOJA_FRASE:-}}"
SENHA="${AGRO_LOJA_SENHA:-}"
[[ "$SENHA" == "$SENHA_OK" ]] || { echo "FAIL: senha"; exit 3; }
echo "$FRASE" | grep -Eiq 'pode.*(produ[cç][aã]o|producao|enviar)|sobe.*(produ[cç][aã]o|producao)|subir.*(produ[cç][aã]o|producao)|enviar.*(produ[cç][aã]o|producao)' \
  || { echo "FAIL: autorização"; exit 3; }

git push origin "$PROD_REF:refs/heads/$BACKUP_BR" || true
git push origin "$PREP_REF:producao"
echo "PUSH OK · badge v$VER_ESPERADA · Ctrl+F5 Fechar caixa Vila"
