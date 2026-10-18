#!/usr/bin/env bash
# Cutover loja — CHECKLIST 18/10f · SEFAZ NF-e + DANFE Agro + PDV busca · v27.26.9 · migrate 0147
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

PREP_REF="origin/deploy/prep-checklist-1810f-nfe-sefaz-danfe-pdv"
PROD_REF="origin/producao"
ROLLBACK_TAG="rollback/pre-v27265-1810f-nfe-sefaz"
BACKUP_BR="producao-backup-pre-v27269-1810f"
SENHA_OK="99738595"
VER_ESPERADA="27.26.9"
MIG_ESPERADA="produtos/migrations/0147_nfe_numeracao_agro.py"

_assert_version_clean() {
  local raw="$1"
  [[ "$raw" != *"<<<<<<"* ]] && [[ "$raw" != *"======="* ]] && [[ "$raw" != *">>>>>>"* ]] || {
    echo "FAIL: VERSION inválido (conflito merge): $raw"; exit 2;
  }
}

git fetch origin producao deploy/prep-checklist-1810f-nfe-sefaz-danfe-pdv "$ROLLBACK_TAG" 2>/dev/null \
  || git fetch origin producao deploy/prep-checklist-1810f-nfe-sefaz-danfe-pdv

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
  docs/DEPLOY-PREP-CHECKLIST-1810f-nfe-sefaz-danfe-pdv.md \
  docs/NFE-EM-PRODUCAO-PASSO-A-PASSO.md \
  produtos/bca_busca_cache_util.py \
  produtos/busca_filtro_pdv_util.py \
  produtos/cadastro_busca_codigo_util.py \
  produtos/catalogo_agro.py \
  produtos/migrations/0147_nfe_numeracao_agro.py \
  produtos/models.py \
  produtos/motor_busca_unificado_util.py \
  produtos/nfce_sp_emissao_util.py \
  produtos/nfe_saida_danfe_html.py \
  produtos/nfe_saida_emissao_util.py \
  produtos/nfe_saida_sp_emissao_util.py \
  produtos/pg_backup_registry.py \
  produtos/static/produtos/img/logo_agro_mais_danfe.png \
  produtos/static/produtos/js/pdv_wizard.js \
  produtos/templates/produtos/_js_busca_produto_inteligente.html \
  produtos/tests_cadastro_busca_pdv_alinhada.py \
  produtos/tests_nfe_saida_util.py \
  produtos/views_nfe_saida.py \
  produtos/views.py \
  scripts/cutover_loja_checklist_1810f_nfe_sefaz_danfe_pdv.sh \
  scripts/post_deploy_checklist_1810f_nfe_sefaz_danfe_pdv.sh \
  scripts/prep_checkpoint_checklist_1810f_nfe_sefaz_danfe_pdv.sh \
  scripts/verify_cadastro_busca_med_path.py \
  scripts/verify_nfe_saida_path.py | sort)"
[[ "$DELTA" == "$ALLOWED" ]] || {
  echo "FAIL: delta inesperado:"; echo "$DELTA"; exit 2;
}

MIG="$(echo "$DELTA" | grep -E '/migrations/[0-9]+_' || true)"
[[ "$MIG" == "$MIG_ESPERADA" ]] || { echo "FAIL: migration esperada $MIG_ESPERADA; got: $MIG"; exit 2; }

FORBIDDEN="$(echo "$DELTA" | grep -E 'caixa_util\.py' || true)"
[[ -z "$FORBIDDEN" ]] || { echo "FAIL: arquivo sensível: $FORBIDDEN"; exit 2; }

PROD_SHA="$(git rev-parse "$PROD_REF")"
if git show-ref --verify --quiet "refs/remotes/origin/$ROLLBACK_TAG"; then
  RB_SHA="$(git rev-parse "origin/$ROLLBACK_TAG")"
  [[ "$RB_SHA" == "$PROD_SHA" ]] || {
    echo "FAIL: $ROLLBACK_TAG ($(git rev-parse --short origin/$ROLLBACK_TAG)) != produção ($PROD)";
    echo "Rode: ./scripts/prep_checkpoint_checklist_1810f_nfe_sefaz_danfe_pdv.sh";
    exit 2;
  }
  echo "OK rollback $ROLLBACK_TAG @ $PROD"
else
  echo "FAIL: branch $ROLLBACK_TAG ausente — rode ./scripts/prep_checkpoint_checklist_1810f_nfe_sefaz_danfe_pdv.sh"
  exit 2
fi

PY=python3
[[ -x .venv/bin/python ]] && PY=.venv/bin/python
export AGRO_PIN_TESTE="${AGRO_PIN_TESTE:-9973}"

"$PY" scripts/verify_nfe_saida_path.py | tee /tmp/cutover-1810f-nfe.log
grep -q "PREP_FAILS=0" /tmp/cutover-1810f-nfe.log

"$PY" scripts/verify_cadastro_busca_med_path.py | tee /tmp/cutover-1810f-cad.log
grep -q "PREP_FAILS=0" /tmp/cutover-1810f-cad.log

"$PY" manage.py test \
  produtos.tests_nfe_saida_util.NfeSaidaDanfeLayoutTests \
  produtos.tests_nfe_saida_util.NfeSaidaCfopTests \
  produtos.tests_nfe_saida_util.NfeSaidaDestinatarioTests \
  produtos.tests_nfe_saida_util.NfeSaidaPostgresStoreTests \
  produtos.tests_nfe_saida_util.NfeSaidaPagamentoTests \
  produtos.tests_nfe_saida_util.NfeSaidaXmlMontagemTests \
  produtos.tests_cadastro_busca_pdv_alinhada \
  -v 0 2>&1 | tee /tmp/cutover-1810f-tests.log
grep -qE 'OK \(|Ran [0-9]+ test' /tmp/cutover-1810f-tests.log

echo "OK: PREP v$VER_ESPERADA · 18/10f SEFAZ+DANFE+PDV · migrate 0147 · rollback $ROLLBACK_TAG"
echo "Pós-deploy loja: ./scripts/post_deploy_checklist_1810f_nfe_sefaz_danfe_pdv.sh"

if [[ "${1:-}" != "--exec" ]]; then exit 0; fi

FRASE="${AGRO_PIN_FRASE:-${AGRO_LOJA_FRASE:-}}"
SENHA="${AGRO_LOJA_SENHA:-}"
[[ "$SENHA" == "$SENHA_OK" ]] || { echo "FAIL: senha"; exit 3; }
echo "$FRASE" | grep -Eiq 'pode.*(produ[cç][aã]o|producao|enviar)|sobe.*(produ[cç][aã]o|producao)|subir.*(produ[cç][aã]o|producao)|enviar.*(produ[cç][aã]o|producao)' \
  || { echo "FAIL: autorização"; exit 3; }

git push origin "$PROD_REF:refs/heads/$ROLLBACK_TAG" || true
git push origin "$PROD_REF:refs/heads/$BACKUP_BR" || true
git push origin "$PREP_REF:producao"
echo "PUSH OK · badge v$VER_ESPERADA · Render Shell: ./scripts/post_deploy_checklist_1810f_nfe_sefaz_danfe_pdv.sh"
