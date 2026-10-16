#!/usr/bin/env bash
# Cutover loja — CADASTRO-PALAVRAS-REABRE · CHECKLIST 16/10e · v27.24 · SEM migrate
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

PREP_REF="origin/deploy/prep-checklist-1610e-cadastro-palavras-reabre"
PROD_REF="origin/producao"
ROLLBACK_TAG="rollback/pre-v2723-cadastro-palavras-reabre"
BACKUP_BR="producao-backup-pre-v2724-cadastro-palavras-reabre"
SENHA_OK="99738595"
VER_ESPERADA="27.24"

_assert_version_clean() {
  local raw="$1"
  [[ "$raw" != *"<<<<<<"* ]] && [[ "$raw" != *"======="* ]] && [[ "$raw" != *">>>>>>"* ]] || {
    echo "FAIL: VERSION inválido (conflito merge): $raw"; exit 2;
  }
}

git fetch origin producao deploy/prep-checklist-1610e-cadastro-palavras-reabre "$ROLLBACK_TAG" 2>/dev/null \
  || git fetch origin producao deploy/prep-checklist-1610e-cadastro-palavras-reabre

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
  docs/DEPLOY-PREP-CHECKLIST-1610e-cadastro-palavras-reabre.md \
  produtos/catalogo_agro.py \
  produtos/medicamento_vet_taxonomia.py \
  produtos/templates/produtos/_modal_editar_produto_cadastro_erp.inc.html \
  produtos/tests/test_medicamento_vet_taxonomia.py \
  scripts/cutover_loja_checklist_1610e_cadastro_palavras_reabre.sh \
  scripts/post_deploy_checklist_1610e_cadastro_palavras_reabre.sh \
  scripts/prep_checkpoint_checklist_1610e_cadastro_palavras_reabre.sh \
  scripts/verify_cadastro_busca_med_path.py | sort)"
[[ "$DELTA" == "$ALLOWED" ]] || {
  echo "FAIL: delta inesperado:"; echo "$DELTA"; exit 2;
}

FORBIDDEN="$(echo "$DELTA" | grep -E 'pdv_wizard|busca_filtro_pdv|/migrations/[0-9]+_' || true)"
[[ -z "$FORBIDDEN" ]] || { echo "FAIL: arquivo proibido no delta: $FORBIDDEN"; exit 2; }

PROD_SHA="$(git rev-parse "$PROD_REF")"
if git show-ref --verify --quiet "refs/remotes/origin/$ROLLBACK_TAG"; then
  RB_SHA="$(git rev-parse "origin/$ROLLBACK_TAG")"
  [[ "$RB_SHA" == "$PROD_SHA" ]] || {
    echo "FAIL: $ROLLBACK_TAG ($(git rev-parse --short origin/$ROLLBACK_TAG)) != produção ($PROD)";
    echo "Rode: ./scripts/prep_checkpoint_checklist_1610e_cadastro_palavras_reabre.sh";
    exit 2;
  }
  echo "OK rollback $ROLLBACK_TAG @ $PROD"
else
  echo "FAIL: branch $ROLLBACK_TAG ausente — rode ./scripts/prep_checkpoint_checklist_1610e_cadastro_palavras_reabre.sh"
  exit 2
fi

PY=python3
[[ -x .venv/bin/python ]] && PY=.venv/bin/python
export AGRO_PIN_TESTE="${AGRO_PIN_TESTE:-9973}"

"$PY" scripts/verify_cadastro_busca_med_path.py | tee /tmp/cutover-1610e-cad-palavras.log
grep -q "PREP_FAILS=0" /tmp/cutover-1610e-cad-palavras.log

echo "OK: PREP v$VER_ESPERADA · CADASTRO-PALAVRAS-REABRE · rollback $ROLLBACK_TAG"

if [[ "${1:-}" != "--exec" ]]; then exit 0; fi

FRASE="${AGRO_PIN_FRASE:-${AGRO_LOJA_FRASE:-}}"
SENHA="${AGRO_LOJA_SENHA:-}"
[[ "$SENHA" == "$SENHA_OK" ]] || { echo "FAIL: senha"; exit 3; }
echo "$FRASE" | grep -Eiq 'pode.*(produ[cç][aã]o|producao|enviar)|sobe.*(produ[cç][aã]o|producao)|subir.*(produ[cç][aã]o|producao)|enviar.*(produ[cç][aã]o|producao)' \
  || { echo "FAIL: autorização"; exit 3; }

git push origin "$PROD_REF:refs/heads/$ROLLBACK_TAG" || true
git push origin "$PROD_REF:refs/heads/$BACKUP_BR" || true
git push origin "$PREP_REF:producao"
echo "PUSH OK · badge v$VER_ESPERADA · Ctrl+F5 Cadastro → aba 2. Busca → reabrir modal"
