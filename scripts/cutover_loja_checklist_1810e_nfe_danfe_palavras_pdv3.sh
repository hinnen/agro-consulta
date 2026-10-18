#!/usr/bin/env bash
# Cutover loja — CHECKLIST 18/10e · DANFE + PALAVRAS-PDV-3 · v27.26.5 · sem migration nova
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

PREP_REF="origin/deploy/prep-checklist-1810e-nfe-danfe-palavras3"
PROD_REF="origin/producao"
ROLLBACK_TAG="rollback/pre-v27263-1810e-danfe-palavras3"
BACKUP_BR="producao-backup-pre-v27265-1810e"
SENHA_OK="99738595"
VER_ESPERADA="27.26.5"

_assert_version_clean() {
  local raw="$1"
  [[ "$raw" != *"<<<<<<"* ]] && [[ "$raw" != *"======="* ]] && [[ "$raw" != *">>>>>>"* ]] || {
    echo "FAIL: VERSION inválido (conflito merge): $raw"; exit 2;
  }
}

git fetch origin producao deploy/prep-checklist-1810e-nfe-danfe-palavras3 "$ROLLBACK_TAG" 2>/dev/null \
  || git fetch origin producao deploy/prep-checklist-1810e-nfe-danfe-palavras3

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
  banana-roteiro.md \
  docs/DEPLOY-PREP-CHECKLIST-1810e-nfe-danfe-palavras3.md \
  produtos/bca_busca_cache_util.py \
  produtos/busca_filtro_pdv_util.py \
  produtos/cadastro_busca_codigo_util.py \
  produtos/catalogo_agro.py \
  produtos/management/commands/backfill_busca_palavras_overlay.py \
  produtos/motor_busca_unificado_util.py \
  produtos/nfe_saida_danfe_html.py \
  produtos/nfe_saida_emissao_util.py \
  produtos/static/produtos/js/nfe_saida.js \
  produtos/templates/produtos/includes/nfe_saida_help_agents.html \
  produtos/templates/produtos/nfe_saida.html \
  produtos/tests_cadastro_busca_palavras_overlay.py \
  produtos/tests_cadastro_busca_pdv_alinhada.py \
  produtos/tests_nfe_saida_util.py \
  produtos/views_nfe_saida.py \
  produtos/views.py \
  scripts/cutover_loja_checklist_1810e_nfe_danfe_palavras_pdv3.sh \
  scripts/post_deploy_checklist_1810e_cadastro_palavras_pdv3.sh \
  scripts/post_deploy_checklist_1810e_nfe_danfe_palavras_pdv3.sh \
  scripts/prep_checkpoint_checklist_1810e_cadastro_palavras_pdv3.sh \
  scripts/prep_checkpoint_checklist_1810e_nfe_danfe_palavras_pdv3.sh \
  scripts/verify_cadastro_busca_med_path.py \
  scripts/verify_nfe_saida_path.py | sort)"
[[ "$DELTA" == "$ALLOWED" ]] || {
  echo "FAIL: delta inesperado:"; echo "$DELTA"; exit 2;
}

MIG="$(echo "$DELTA" | grep -E '/migrations/[0-9]+_' || true)"
[[ -z "$MIG" ]] || { echo "FAIL: migration inesperada no pacote: $MIG"; exit 2; }

FORBIDDEN="$(echo "$DELTA" | grep -E 'pdv_wizard\.js|caixa_util\.py' || true)"
[[ -z "$FORBIDDEN" ]] || { echo "FAIL: arquivo sensível: $FORBIDDEN"; exit 2; }

PROD_SHA="$(git rev-parse "$PROD_REF")"
if git show-ref --verify --quiet "refs/remotes/origin/$ROLLBACK_TAG"; then
  RB_SHA="$(git rev-parse "origin/$ROLLBACK_TAG")"
  [[ "$RB_SHA" == "$PROD_SHA" ]] || {
    echo "FAIL: $ROLLBACK_TAG ($(git rev-parse --short origin/$ROLLBACK_TAG)) != produção ($PROD)";
    echo "Rode: ./scripts/prep_checkpoint_checklist_1810e_nfe_danfe_palavras_pdv3.sh";
    exit 2;
  }
  echo "OK rollback $ROLLBACK_TAG @ $PROD"
else
  echo "FAIL: branch $ROLLBACK_TAG ausente — rode ./scripts/prep_checkpoint_checklist_1810e_nfe_danfe_palavras_pdv3.sh"
  exit 2
fi

PY=python3
[[ -x .venv/bin/python ]] && PY=.venv/bin/python
export AGRO_PIN_TESTE="${AGRO_PIN_TESTE:-9973}"

"$PY" scripts/verify_nfe_saida_path.py | tee /tmp/cutover-1810e-nfe.log
grep -q "PREP_FAILS=0" /tmp/cutover-1810e-nfe.log

"$PY" scripts/verify_cadastro_busca_med_path.py | tee /tmp/cutover-1810e-cad.log
grep -q "PREP_FAILS=0" /tmp/cutover-1810e-cad.log

"$PY" manage.py test \
  produtos.tests_nfe_saida_util \
  produtos.tests_cadastro_busca_pdv_alinhada \
  produtos.tests_cadastro_busca_palavras_overlay \
  -v 0 2>&1 | tee /tmp/cutover-1810e-tests.log
grep -qE 'OK \(|Ran [0-9]+ test' /tmp/cutover-1810e-tests.log

echo "OK: PREP v$VER_ESPERADA · 18/10e DANFE + PALAVRAS-PDV-3 · rollback $ROLLBACK_TAG"
echo "Pós-deploy loja: ./scripts/post_deploy_checklist_1810e_nfe_danfe_palavras_pdv3.sh"

if [[ "${1:-}" != "--exec" ]]; then exit 0; fi

FRASE="${AGRO_PIN_FRASE:-${AGRO_LOJA_FRASE:-}}"
SENHA="${AGRO_LOJA_SENHA:-}"
[[ "$SENHA" == "$SENHA_OK" ]] || { echo "FAIL: senha"; exit 3; }
echo "$FRASE" | grep -Eiq 'pode.*(produ[cç][aã]o|producao|enviar)|sobe.*(produ[cç][aã]o|producao)|subir.*(produ[cç][aã]o|producao)|enviar.*(produ[cç][aã]o|producao)' \
  || { echo "FAIL: autorização"; exit 3; }

git push origin "$PROD_REF:refs/heads/$ROLLBACK_TAG" || true
git push origin "$PROD_REF:refs/heads/$BACKUP_BR" || true
git push origin "$PREP_REF:producao"
echo "PUSH OK · badge v$VER_ESPERADA · Render Shell: ./scripts/post_deploy_checklist_1810e_nfe_danfe_palavras_pdv3.sh"
