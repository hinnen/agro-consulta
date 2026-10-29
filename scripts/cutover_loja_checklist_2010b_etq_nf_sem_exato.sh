#!/usr/bin/env bash
# Cutover loja — ETQ-NF-SEM-EXATO · CHECKLIST 20/10b · v27.26.15 · SEM migrate
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

PREP_REF="origin/deploy/prep-checklist-2010b-etq-nf-sem-exato"
PROD_REF="origin/producao"
ROLLBACK_TAG="rollback/pre-v272615-2010b-etq-nf-sem-exato"
BACKUP_BR="producao-backup-pre-v272615-2010b-etq-nf-sem-exato"
SENHA_OK="99738595"
VER_ESPERADA="27.26.15"

_assert_version_clean() {
  local raw="$1"
  [[ "$raw" != *"<<<<<<"* ]] && [[ "$raw" != *"======="* ]] && [[ "$raw" != *">>>>>>"* ]] || {
    echo "FAIL: VERSION inválido (conflito merge): $raw"; exit 2;
  }
}

git fetch origin producao deploy/prep-checklist-2010b-etq-nf-sem-exato "$ROLLBACK_TAG" 2>/dev/null \
  || git fetch origin producao deploy/prep-checklist-2010b-etq-nf-sem-exato

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
  docs/DEPLOY-PREP-CHECKLIST-2010b-etq-nf-sem-exato.md \
  produtos/cadastro_filtros_util.py \
  produtos/tests_pdv_cadastro_rapido.py \
  scripts/cutover_loja_checklist_2010b_etq_nf_sem_exato.sh \
  scripts/post_deploy_checklist_2010b_etq_nf_sem_exato.sh \
  scripts/prep_checkpoint_checklist_2010b_etq_nf_sem_exato.sh \
  scripts/verify_etq_filtro_nf_entrada_path.py | sort)"
[[ "$DELTA" == "$ALLOWED" ]] || {
  echo "FAIL: delta inesperado:"; echo "$DELTA"; exit 2;
}

MIG="$(echo "$DELTA" | grep -E '/migrations/[0-9]+_' || true)"
[[ -z "$MIG" ]] || { echo "FAIL: migration: $MIG"; exit 2; }

PROD_SHA="$(git rev-parse "$PROD_REF")"
if git show-ref --verify --quiet "refs/remotes/origin/$ROLLBACK_TAG"; then
  RB_SHA="$(git rev-parse "origin/$ROLLBACK_TAG")"
  [[ "$RB_SHA" == "$PROD_SHA" ]] || {
    echo "FAIL: $ROLLBACK_TAG != produção ($PROD) — rode prep_checkpoint"; exit 2;
  }
  echo "OK rollback $ROLLBACK_TAG @ $PROD"
else
  echo "FAIL: $ROLLBACK_TAG ausente — rode ./scripts/prep_checkpoint_checklist_2010b_etq_nf_sem_exato.sh"
  exit 2
fi

PY=python3
[[ -x .venv/bin/python ]] && PY=.venv/bin/python
export AGRO_PIN_TESTE="${AGRO_PIN_TESTE:-9973}"

"$PY" scripts/verify_etq_filtro_nf_entrada_path.py | tee /tmp/cutover-2010b-etq-nf.log
grep -q VERIFY_OK /tmp/cutover-2010b-etq-nf.log

echo "OK: PREP v$VER_ESPERADA · ETQ-NF-SEM-EXATO · rollback $ROLLBACK_TAG"
echo "Pós: ./scripts/post_deploy_checklist_2010b_etq_nf_sem_exato.sh"

if [[ "${1:-}" != "--exec" ]]; then exit 0; fi

FRASE="${AGRO_PIN_FRASE:-${AGRO_LOJA_FRASE:-}}"
SENHA="${AGRO_LOJA_SENHA:-}"
[[ "$SENHA" == "$SENHA_OK" ]] || { echo "FAIL: senha"; exit 3; }
echo "$FRASE" | grep -Eiq 'pode.*(produ[cç][aã]o|producao|enviar)|sobe.*(produ[cç][aã]o|producao)|subir.*(produ[cç][aã]o|producao)|enviar.*(produ[cç][aã]o|producao)' \
  || { echo "FAIL: autorização"; exit 3; }

git push origin "$PROD_REF:refs/heads/$ROLLBACK_TAG" || true
git push origin "$PROD_REF:refs/heads/$BACKUP_BR" || true
git push origin "$PREP_REF:producao"
echo "PUSH OK · badge v$VER_ESPERADA · Etiquetas → Nº NF (SEM completo)"
