#!/usr/bin/env bash
# Cutover loja — CHECKLIST 11/10 · ETQ-SOLIC-MOBILE + VL-HUB-PIN · v26.91
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

PREP_REF="origin/deploy/prep-checklist-1110"
PROD_REF="origin/producao"
ROLLBACK_TAG="rollback/pre-checklist-1110-v26.90"
BACKUP_BR="producao-backup-pre-v2691-checklist-1110"
SENHA_OK="99738595"
VER_ESPERADA="26.92"
MIG_OK=(
  "produtos/migrations/0142_etiqueta_solicitacao_pacote_agro.py"
  "produtos/migrations/0143_gm_lojas_app_log_agro.py"
)

git fetch origin producao deploy/prep-checklist-1110 "$ROLLBACK_TAG" 2>/dev/null \
  || git fetch origin producao deploy/prep-checklist-1110

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
if [[ -n "$MIG" ]]; then
  while IFS= read -r f; do
    [[ -z "$f" ]] && continue
    ok_mig=0
    for allowed in "${MIG_OK[@]}"; do
      [[ "$f" == "$allowed" ]] && ok_mig=1
    done
    [[ "$ok_mig" == 1 ]] || {
      echo "FAIL: migration inesperada: $f"; exit 2;
    }
  done <<< "$MIG"
fi

git rev-parse "$ROLLBACK_TAG" >/dev/null 2>&1 \
  || git ls-remote --exit-code --tags origin "refs/tags/$ROLLBACK_TAG" >/dev/null \
  || { echo "FAIL: tag rollback ausente $ROLLBACK_TAG"; exit 2; }

PREP_FULL="$(git rev-parse "$PREP_REF")"
git grep -q 'EtiquetaSolicitacaoPacoteAgro' "$PREP_FULL" -- produtos/models.py || {
  echo "FAIL: delta sem modelo solicitação etiquetas"; exit 2;
}
git grep -q 'pin_liberado_vendas_tarefas' "$PREP_FULL" -- produtos/vendas_lojas_hub_acesso_util.py || {
  echo "FAIL: delta sem allowlist hub"; exit 2;
}

PY=python3
[[ -x .venv/bin/python ]] && PY=.venv/bin/python

"$PY" scripts/verify_etq_solicitacao_vl_pin_path.py | tee /tmp/cutover-1110-etq-vl.log
grep -q 'TOTAL OK=24 FAIL=0' /tmp/cutover-1110-etq-vl.log

echo "OK: PREP · migrate SIM (0142) · rollback $ROLLBACK_TAG"
echo "Pós-deploy: python manage.py migrate produtos 0143 · smoke hub PIN 7d + Solicitações etiquetas"

if [[ "${1:-}" != "--exec" ]]; then exit 0; fi

FRASE="${AGRO_LOJA_FRASE:-}"
SENHA="${AGRO_LOJA_SENHA:-}"
[[ "$SENHA" == "$SENHA_OK" ]] || { echo "FAIL: senha"; exit 3; }
echo "$FRASE" | grep -Eiq 'pode.*(produ[cç][aã]o|producao)|sobe.*(produ[cç][aã]o|producao)' \
  || { echo "FAIL: autorização"; exit 3; }

git push origin "$PROD_REF:refs/heads/$BACKUP_BR" || true
git push origin "$PREP_REF:producao"
echo "PUSH OK · badge v$VER_ESPERADA · migrate 0143 no Shell"
