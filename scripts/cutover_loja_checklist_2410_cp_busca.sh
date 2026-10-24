#!/usr/bin/env bash
# Cutover — 23/10c CP-EXPORT + 24/10 CADASTRO-BUSCA + gaveta busca · v27.26.26 · sem migrate
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
PREP_REF="origin/deploy/prep-checklist-2410-cp-busca"
PROD_REF="origin/producao"
ROLLBACK_TAG="rollback/pre-v272626-2410-cp-busca"
BACKUP_BR="producao-backup-pre-v272626-2410-cp-busca"
SENHA_OK="99738595"
VER_ESPERADA="27.26.26"

git fetch origin producao deploy/prep-checklist-2410-cp-busca "$ROLLBACK_TAG" 2>/dev/null \
  || git fetch origin producao deploy/prep-checklist-2410-cp-busca

PROD="$(git rev-parse --short "$PROD_REF")"
PREP="$(git rev-parse --short "$PREP_REF" 2>/dev/null || echo missing)"
VER="$(git show "$PREP_REF:VERSION" 2>/dev/null | tr -d '[:space:]' || echo ?)"
echo "producao=$PROD PREP=$PREP VERSION=$VER"
git merge-base --is-ancestor "$PROD_REF" "$PREP_REF" || { echo "FAIL: ancestralidade"; exit 2; }
[[ "$VER" == "$VER_ESPERADA" ]] || { echo "FAIL: VERSION=$VER"; exit 2; }

DELTA="$(git diff --name-only "$PROD_REF...$PREP_REF" | sort)"
ALLOWED="$(printf '%s\n' \
  VERSION \
  docs/DEPLOY-PREP-CHECKLIST-2410-cp-busca.md \
  produtos/bca_busca_cache_util.py \
  produtos/busca_filtro_pdv_util.py \
  produtos/cadastro_busca_codigo_util.py \
  produtos/catalogo_agro.py \
  produtos/templates/produtos/lancamentos_contas_pagar_teste.html \
  produtos/templates/produtos/partials/pdv/step_produtos.html \
  produtos/templates/produtos/pdv_wizard.html \
  produtos/tests_cadastro_busca_pdv_alinhada.py \
  produtos/views.py \
  scripts/cutover_loja_checklist_2410_cp_busca.sh \
  scripts/post_deploy_checklist_2410_cp_busca.sh \
  scripts/prep_checkpoint_checklist_2410_cp_busca.sh \
  scripts/verify_cadastro_busca_med_path.py \
  scripts/verify_cp_export_button_path.py | sort)"
[[ "$DELTA" == "$ALLOWED" ]] || { echo "FAIL: delta:"; echo "$DELTA"; exit 2; }

MIG="$(echo "$DELTA" | grep -E '/migrations/[0-9]+_' || true)"
[[ -z "$MIG" ]] || { echo "FAIL: migrate $MIG"; exit 2; }
CAIXA="$(echo "$DELTA" | grep -Ei 'caixa_fechar|caixa_util|checkout' || true)"
[[ -z "$CAIXA" ]] || { echo "FAIL: caixa/checkout $CAIXA"; exit 2; }

PROD_SHA="$(git rev-parse "$PROD_REF")"
git show-ref --verify --quiet "refs/remotes/origin/$ROLLBACK_TAG" || { echo "FAIL: checkpoint"; exit 2; }
[[ "$(git rev-parse origin/$ROLLBACK_TAG)" == "$PROD_SHA" ]] || { echo "FAIL: rollback"; exit 2; }

PY=python3
[[ -x .venv/bin/python ]] && PY=.venv/bin/python
"$PY" scripts/verify_cp_export_button_path.py | tee /tmp/cutover-2410-cp.log
grep -q VERIFY_OK /tmp/cutover-2410-cp.log
"$PY" scripts/verify_cadastro_busca_med_path.py | tee /tmp/cutover-2410-busca.log
grep -q VERIFY_OK /tmp/cutover-2410-busca.log
echo "OK: PREP v$VER_ESPERADA · CP-EXPORT + CADASTRO-BUSCA + gaveta · sem migrate · rollback $ROLLBACK_TAG"

[[ "${1:-}" != "--exec" ]] && exit 0
FRASE="${AGRO_PIN_FRASE:-${AGRO_LOJA_FRASE:-}}"
SENHA="${AGRO_LOJA_SENHA:-}"
[[ "$SENHA" == "$SENHA_OK" ]] || { echo "FAIL: senha"; exit 3; }
echo "$FRASE" | grep -Eiq 'pode.*(produ[cç][aã]o|producao|enviar)|sobe.*(produ[cç][aã]o|producao)|subir.*(produ[cç][aã]o|producao)|enviar.*(produ[cç][aã]o|producao)' \
  || { echo "FAIL: autorização"; exit 3; }
git push origin "$PROD_REF:refs/heads/$ROLLBACK_TAG" || true
git push origin "$PROD_REF:refs/heads/$BACKUP_BR" || true
git push origin "$PREP_REF:producao"
echo "PUSH OK v$VER_ESPERADA"
