#!/usr/bin/env bash
# Cutover loja — CHECKLIST 18/10g · NF-e XSD dest + manual UX · v27.26.10 · sem migrate
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

PREP_REF="origin/deploy/prep-checklist-1810g-nfe-xsd-ux"
PROD_REF="origin/producao"
ROLLBACK_TAG="rollback/pre-v272610-1810g-nfe-xsd"
BACKUP_BR="producao-backup-pre-v272610-1810g"
SENHA_OK="99738595"
VER_ESPERADA="27.26.10"

_assert_version_clean() {
  local raw="$1"
  [[ "$raw" != *"<<<<<<"* ]] && [[ "$raw" != *"======="* ]] && [[ "$raw" != *">>>>>>"* ]] || {
    echo "FAIL: VERSION inválido (conflito merge): $raw"; exit 2;
  }
}

git fetch origin producao deploy/prep-checklist-1810g-nfe-xsd-ux "$ROLLBACK_TAG" 2>/dev/null \
  || git fetch origin producao deploy/prep-checklist-1810g-nfe-xsd-ux

PROD="$(git rev-parse --short "$PROD_REF")"
PREP="$(git rev-parse --short "$PREP_REF" 2>/dev/null || echo missing)"
VER="$(git show "$PREP_REF:VERSION" 2>/dev/null | tr -d '[:space:]' || echo ?)"
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
  docs/DEPLOY-PREP-CHECKLIST-1810g-nfe-xsd-ux.md \
  produtos/nfe_saida_sp_emissao_util.py \
  produtos/static/produtos/js/nfe_saida.js \
  produtos/templates/produtos/includes/nfe_saida_help_agents.html \
  produtos/templates/produtos/nfe_saida.html \
  produtos/tests_nfe_saida_util.py \
  scripts/cutover_loja_checklist_1810g_nfe_xsd_ux.sh \
  scripts/post_deploy_checklist_1810g_nfe_xsd_ux.sh \
  scripts/prep_checkpoint_checklist_1810g_nfe_xsd_ux.sh \
  scripts/verify_nfe_saida_path.py | sort)"
[[ "$DELTA" == "$ALLOWED" ]] || {
  echo "FAIL: delta inesperado:"; echo "$DELTA"; exit 2;
}

MIG="$(echo "$DELTA" | grep -E '/migrations/[0-9]+_' || true)"
[[ -z "$MIG" ]] || { echo "FAIL: migration inesperada: $MIG"; exit 2; }

PROD_SHA="$(git rev-parse "$PROD_REF")"
if git show-ref --verify --quiet "refs/remotes/origin/$ROLLBACK_TAG"; then
  RB_SHA="$(git rev-parse "origin/$ROLLBACK_TAG")"
  [[ "$RB_SHA" == "$PROD_SHA" ]] || {
    echo "FAIL: $ROLLBACK_TAG ($(git rev-parse --short origin/$ROLLBACK_TAG)) != produção ($PROD)";
    echo "Rode: ./scripts/prep_checkpoint_checklist_1810g_nfe_xsd_ux.sh";
    exit 2;
  }
  echo "OK rollback $ROLLBACK_TAG @ $PROD"
else
  echo "FAIL: branch $ROLLBACK_TAG ausente — rode ./scripts/prep_checkpoint_checklist_1810g_nfe_xsd_ux.sh"
  exit 2
fi

PY=python3
[[ -x .venv/bin/python ]] && PY=.venv/bin/python
export AGRO_PIN_TESTE="${AGRO_PIN_TESTE:-9973}"

"$PY" scripts/verify_nfe_saida_path.py | tee /tmp/cutover-1810g-nfe.log
grep -q "PREP_FAILS=0" /tmp/cutover-1810g-nfe.log

"$PY" manage.py test \
  produtos.tests_nfe_saida_util.NfeSaidaDanfeLayoutTests \
  produtos.tests_nfe_saida_util.NfeSaidaCfopTests \
  produtos.tests_nfe_saida_util.NfeSaidaDestinatarioTests \
  produtos.tests_nfe_saida_util.NfeSaidaPostgresStoreTests \
  produtos.tests_nfe_saida_util.NfeSaidaPagamentoTests \
  produtos.tests_nfe_saida_util.NfeSaidaXmlMontagemTests \
  -v 0 2>&1 | tee /tmp/cutover-1810g-tests.log
grep -qE 'OK \(|Ran [0-9]+ test' /tmp/cutover-1810g-tests.log

echo "OK: PREP v$VER_ESPERADA · 18/10g XSD+UX · rollback $ROLLBACK_TAG"
echo "Pós-deploy loja: ./scripts/post_deploy_checklist_1810g_nfe_xsd_ux.sh"

if [[ "${1:-}" != "--exec" ]]; then exit 0; fi

FRASE="${AGRO_PIN_FRASE:-${AGRO_LOJA_FRASE:-}}"
SENHA="${AGRO_LOJA_SENHA:-}"
[[ "$SENHA" == "$SENHA_OK" ]] || { echo "FAIL: senha"; exit 3; }
echo "$FRASE" | grep -Eiq 'pode.*(produ[cç][aã]o|producao|enviar)|sobe.*(produ[cç][aã]o|producao)|subir.*(produ[cç][aã]o|producao)|enviar.*(produ[cç][aã]o|producao)' \
  || { echo "FAIL: autorização"; exit 3; }

git push origin "$PROD_REF:refs/heads/$ROLLBACK_TAG" || true
git push origin "$PROD_REF:refs/heads/$BACKUP_BR" || true
git push origin "$PREP_REF:producao"
echo "PUSH OK · badge v$VER_ESPERADA · Render Shell: ./scripts/post_deploy_checklist_1810g_nfe_xsd_ux.sh"
