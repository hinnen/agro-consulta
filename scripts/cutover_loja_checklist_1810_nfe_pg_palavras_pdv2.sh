#!/usr/bin/env bash
# Cutover loja — CHECKLIST 18/10 · NFE-SAIDA-55-PG + CADASTRO-PALAVRAS-PDV-2 · v27.26.3 · migrate 0146
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

PREP_REF="origin/deploy/prep-checklist-1810-nfe-pg-palavras-pdv2"
PROD_REF="origin/producao"
ROLLBACK_TAG="rollback/pre-v2726-1810-nfe-pg-palavras"
BACKUP_BR="producao-backup-pre-v2726-3-1810"
SENHA_OK="99738595"
VER_ESPERADA="27.26.3"
MIG_ESPERADA="produtos/migrations/0146_nfe_saida_rascunho_agro.py"

_assert_version_clean() {
  local raw="$1"
  [[ "$raw" != *"<<<<<<"* ]] && [[ "$raw" != *"======="* ]] && [[ "$raw" != *">>>>>>"* ]] || {
    echo "FAIL: VERSION inválido (conflito merge): $raw"; exit 2;
  }
}

git fetch origin producao deploy/prep-checklist-1810-nfe-pg-palavras-pdv2 "$ROLLBACK_TAG" 2>/dev/null \
  || git fetch origin producao deploy/prep-checklist-1810-nfe-pg-palavras-pdv2

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
  AGENTS.md \
  VERSION \
  banana.md \
  banana-roteiro.md \
  docs/DEPLOY-PREP-CHECKLIST-1810-nfe-pg-palavras-pdv2.md \
  docs/DEPLOY-PREP-CHECKLIST-1810-nfe-saida-pg.md \
  produtos/agro_fonte_config.py \
  produtos/bca_busca_cache_util.py \
  produtos/cadastro_busca_codigo_util.py \
  produtos/catalogo_agro.py \
  produtos/medicamento_vet_taxonomia.py \
  produtos/migrations/0146_nfe_saida_rascunho_agro.py \
  produtos/models.py \
  produtos/nfe_saida_rascunho_pg_util.py \
  produtos/nfe_saida_util.py \
  produtos/pg_backup_registry.py \
  produtos/tests_cadastro_busca_pdv_alinhada.py \
  produtos/tests_nfe_saida_util.py \
  produtos/views_nfe_saida.py \
  scripts/cutover_loja_checklist_1810_nfe_pg_palavras_pdv2.sh \
  scripts/post_deploy_checklist_1810_nfe_pg_palavras_pdv2.sh \
  scripts/prep_checkpoint_checklist_1810_nfe_pg_palavras_pdv2.sh \
  scripts/post_deploy_checklist_1810_nfe_saida_pg.sh \
  scripts/verify_cadastro_busca_med_path.py \
  scripts/verify_nfe_saida_path.py | sort)"
[[ "$DELTA" == "$ALLOWED" ]] || {
  echo "FAIL: delta inesperado:"; echo "$DELTA"; exit 2;
}

MIG="$(echo "$DELTA" | grep -E '/migrations/[0-9]+_' || true)"
[[ "$MIG" == "$MIG_ESPERADA" ]] || { echo "FAIL: migration esperada $MIG_ESPERADA; got: $MIG"; exit 2; }

FORBIDDEN="$(echo "$DELTA" | grep -E 'pdv_wizard\.js|caixa_util\.py' || true)"
[[ -z "$FORBIDDEN" ]] || { echo "FAIL: arquivo sensível: $FORBIDDEN"; exit 2; }

PROD_SHA="$(git rev-parse "$PROD_REF")"
if git show-ref --verify --quiet "refs/remotes/origin/$ROLLBACK_TAG"; then
  RB_SHA="$(git rev-parse "origin/$ROLLBACK_TAG")"
  [[ "$RB_SHA" == "$PROD_SHA" ]] || {
    echo "FAIL: $ROLLBACK_TAG ($(git rev-parse --short origin/$ROLLBACK_TAG)) != produção ($PROD)";
    echo "Rode: ./scripts/prep_checkpoint_checklist_1810_nfe_pg_palavras_pdv2.sh";
    exit 2;
  }
  echo "OK rollback $ROLLBACK_TAG @ $PROD"
else
  echo "FAIL: branch $ROLLBACK_TAG ausente — rode ./scripts/prep_checkpoint_checklist_1810_nfe_pg_palavras_pdv2.sh"
  exit 2
fi

PY=python3
[[ -x .venv/bin/python ]] && PY=.venv/bin/python
export AGRO_PIN_TESTE="${AGRO_PIN_TESTE:-9973}"

"$PY" scripts/verify_nfe_saida_path.py | tee /tmp/cutover-1810-nfe.log
grep -q "PREP_FAILS=0" /tmp/cutover-1810-nfe.log

"$PY" scripts/verify_cadastro_busca_med_path.py | tee /tmp/cutover-1810-cad.log
grep -q "PREP_FAILS=0" /tmp/cutover-1810-cad.log

"$PY" manage.py test \
  produtos.tests_nfe_saida_util.NfeSaidaCfopTests \
  produtos.tests_nfe_saida_util.NfeSaidaDestinatarioTests \
  produtos.tests_nfe_saida_util.NfeSaidaPostgresStoreTests \
  produtos.tests_nfe_saida_util.NfeSaidaPagamentoTests \
  produtos.tests_cadastro_busca_pdv_alinhada \
  -v 0 2>&1 | tee /tmp/cutover-1810-tests.log
grep -qE 'OK \(|Ran [0-9]+ test' /tmp/cutover-1810-tests.log

echo "OK: PREP v$VER_ESPERADA · 18/10 NF-e PG + palavras PDV · migrate 0146 · rollback $ROLLBACK_TAG"
echo "Pós-deploy loja: ./scripts/post_deploy_checklist_1810_nfe_pg_palavras_pdv2.sh (migrate Render)"

if [[ "${1:-}" != "--exec" ]]; then exit 0; fi

FRASE="${AGRO_PIN_FRASE:-${AGRO_LOJA_FRASE:-}}"
SENHA="${AGRO_LOJA_SENHA:-}"
[[ "$SENHA" == "$SENHA_OK" ]] || { echo "FAIL: senha"; exit 3; }
echo "$FRASE" | grep -Eiq 'pode.*(produ[cç][aã]o|producao|enviar)|sobe.*(produ[cç][aã]o|producao)|subir.*(produ[cç][aã]o|producao)|enviar.*(produ[cç][aã]o|producao)' \
  || { echo "FAIL: autorização"; exit 3; }

git push origin "$PROD_REF:refs/heads/$ROLLBACK_TAG" || true
git push origin "$PROD_REF:refs/heads/$BACKUP_BR" || true
git push origin "$PREP_REF:producao"
echo "PUSH OK · badge v$VER_ESPERADA · Render Shell: ./scripts/post_deploy_checklist_1810_nfe_pg_palavras_pdv2.sh"
