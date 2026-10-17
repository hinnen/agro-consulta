#!/usr/bin/env bash
# Cutover loja — CADASTRO-PALAVRAS-PDV · CHECKLIST 16/10f · v27.25 · SEM migrate
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

PREP_REF="origin/deploy/prep-checklist-1610f-cadastro-palavras-pdv"
PROD_REF="origin/producao"
ROLLBACK_TAG="rollback/pre-v2724-cadastro-palavras-pdv"
BACKUP_BR="producao-backup-pre-v2725-cadastro-palavras-pdv"
SENHA_OK="99738595"
VER_ESPERADA="27.25"

_assert_version_clean() {
  local raw="$1"
  [[ "$raw" != *"<<<<<<"* ]] && [[ "$raw" != *"======="* ]] && [[ "$raw" != *">>>>>>"* ]] || {
    echo "FAIL: VERSION inválido (conflito merge): $raw"; exit 2;
  }
}

git fetch origin producao deploy/prep-checklist-1610f-cadastro-palavras-pdv "$ROLLBACK_TAG" 2>/dev/null \
  || git fetch origin producao deploy/prep-checklist-1610f-cadastro-palavras-pdv

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
  docs/DEPLOY-PREP-CHECKLIST-1610f-cadastro-palavras-pdv.md \
  produtos/cadastro_busca_codigo_util.py \
  produtos/catalogo_agro.py \
  produtos/medicamento_vet_taxonomia.py \
  produtos/tests/test_medicamento_vet_taxonomia.py \
  produtos/tests_cadastro_busca_pdv_alinhada.py \
  scripts/cutover_loja_checklist_1610f_cadastro_palavras_pdv.sh \
  scripts/post_deploy_checklist_1610f_cadastro_palavras_pdv.sh \
  scripts/prep_checkpoint_checklist_1610f_cadastro_palavras_pdv.sh \
  scripts/verify_cadastro_busca_med_path.py | sort)"
[[ "$DELTA" == "$ALLOWED" ]] || {
  echo "FAIL: delta inesperado:"; echo "$DELTA"; exit 2;
}

FORBIDDEN="$(echo "$DELTA" | grep -E 'pdv_wizard|/migrations/[0-9]+_' || true)"
[[ -z "$FORBIDDEN" ]] || { echo "FAIL: arquivo proibido no delta: $FORBIDDEN"; exit 2; }

PROD_SHA="$(git rev-parse "$PROD_REF")"
if git show-ref --verify --quiet "refs/remotes/origin/$ROLLBACK_TAG"; then
  RB_SHA="$(git rev-parse "origin/$ROLLBACK_TAG")"
  [[ "$RB_SHA" == "$PROD_SHA" ]] || {
    echo "FAIL: $ROLLBACK_TAG ($(git rev-parse --short origin/$ROLLBACK_TAG)) != produção ($PROD)";
    echo "Rode: ./scripts/prep_checkpoint_checklist_1610f_cadastro_palavras_pdv.sh";
    exit 2;
  }
  echo "OK rollback $ROLLBACK_TAG @ $PROD"
else
  echo "FAIL: branch $ROLLBACK_TAG ausente — rode ./scripts/prep_checkpoint_checklist_1610f_cadastro_palavras_pdv.sh"
  exit 2
fi

PY=python3
[[ -x .venv/bin/python ]] && PY=.venv/bin/python
export AGRO_PIN_TESTE="${AGRO_PIN_TESTE:-9973}"

"$PY" scripts/verify_cadastro_busca_med_path.py | tee /tmp/cutover-1610f-cad-pdv.log
grep -q "PREP_FAILS=0" /tmp/cutover-1610f-cad-pdv.log

"$PY" -m django test produtos.tests_cadastro_busca_pdv_alinhada -v 0 2>&1 | tee /tmp/cutover-1610f-pdv-tests.log
grep -qE 'OK \(|Ran [0-9]+ test' /tmp/cutover-1610f-pdv-tests.log

echo "OK: PREP v$VER_ESPERADA · CADASTRO-PALAVRAS-PDV · rollback $ROLLBACK_TAG"

if [[ "${1:-}" != "--exec" ]]; then exit 0; fi

FRASE="${AGRO_PIN_FRASE:-${AGRO_LOJA_FRASE:-}}"
SENHA="${AGRO_LOJA_SENHA:-}"
[[ "$SENHA" == "$SENHA_OK" ]] || { echo "FAIL: senha"; exit 3; }
echo "$FRASE" | grep -Eiq 'pode.*(produ[cç][aã]o|producao|enviar)|sobe.*(produ[cç][aã]o|producao)|subir.*(produ[cç][aã]o|producao)|enviar.*(produ[cç][aã]o|producao)' \
  || { echo "FAIL: autorização"; exit 3; }

git push origin "$PROD_REF:refs/heads/$ROLLBACK_TAG" || true
git push origin "$PROD_REF:refs/heads/$BACKUP_BR" || true
git push origin "$PREP_REF:producao"
echo "PUSH OK · badge v$VER_ESPERADA · Ctrl+F5 PDV busca apelido (2 tempo)"
