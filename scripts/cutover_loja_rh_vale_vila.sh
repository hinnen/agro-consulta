#!/usr/bin/env bash
# Cutover loja — RH-VALE-VILA · v27.18 · SEM migrate
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

# Produção divergiu de `teste` — pacote loja = cherry sobre producao em deploy/prep-rh-vale-vila.
PREP_REF="${PREP_REF:-origin/deploy/prep-rh-vale-vila}"
PROD_REF="origin/producao"
ROLLBACK_TAG="${ROLLBACK_TAG:-rollback/pre-v2717-rh-vale-vila}"
BACKUP_BR="${BACKUP_BR:-producao-backup-pre-v2718-rh-vale-vila}"
SENHA_OK="99738595"
VER_ESPERADA="27.18"

git fetch origin producao teste deploy/prep-rh-vale-vila "$ROLLBACK_TAG" 2>/dev/null \
  || git fetch origin producao teste deploy/prep-rh-vale-vila

PROD="$(git rev-parse --short "$PROD_REF")"
PREP="$(git rev-parse --short "$PREP_REF")"
VER="$(git show "$PREP_REF:VERSION" 2>/dev/null | tr -d '[:space:]' || echo "?")"
echo "producao=$PROD PREP=$PREP VERSION=$VER"

git merge-base --is-ancestor "$PROD_REF" "$PREP_REF" || {
  echo "FAIL: produção não é ancestral do PREP"; exit 2;
}
[[ "$VER" == "$VER_ESPERADA" ]] || {
  echo "FAIL: VERSION=$VER (esperado $VER_ESPERADA)"; exit 2;
}

DELTA="$(git diff --name-only "$PROD_REF...$PREP_REF" | sort)"
echo "--- delta (resumo) ---"
echo "$DELTA" | head -40
echo "---"

MIG="$(echo "$DELTA" | grep -E '/migrations/[0-9]+_' || true)"
[[ -z "$MIG" ]] || { echo "FAIL: migration no pacote: $MIG"; exit 2; }

FORBIDDEN="$(echo "$DELTA" | grep -E \
  'pdv_wizard\.js|mongo_financeiro|caixa_fechar|0145_|lancamentos_manual' || true)"
[[ -z "$FORBIDDEN" ]] || { echo "FAIL: arquivo sensível fora do escopo: $FORBIDDEN"; exit 2; }

PY=python3
[[ -x .venv/bin/python ]] && PY=.venv/bin/python

export AGRO_PIN_TESTE="${AGRO_PIN_TESTE:-9973}"
"$PY" scripts/verify_rh_vale_caixa_vila_path.py | tee /tmp/cutover-rh-vale-vila.log
grep -q 'VERIFY_OK' /tmp/cutover-rh-vale-vila.log

"$PY" scripts/verify_saida_empresa_loja.py | tee /tmp/cutover-rh-vale-vila-saida.log
grep -q 'VERIFY_OK' /tmp/cutover-rh-vale-vila-saida.log

echo "OK: PREP v$VER_ESPERADA · RH-VALE-VILA · rollback $ROLLBACK_TAG"
echo "Pós-deploy: ./scripts/post_deploy_rh_vale_vila.sh"
echo "Smoke loja: caixa Vila aberto · Retirada · Adiantamento (Vale) · PIN · conferir RH"

if [[ "${1:-}" != "--exec" ]]; then exit 0; fi

FRASE="${AGRO_PIN_FRASE:-${AGRO_LOJA_FRASE:-}}"
SENHA="${AGRO_LOJA_SENHA:-}"
[[ "$SENHA" == "$SENHA_OK" ]] || { echo "FAIL: senha"; exit 3; }
echo "$FRASE" | grep -Eiq 'pode.*(produ[cç][aã]o|producao)|sobe.*(produ[cç][aã]o|producao)|subir.*(produ[cç][aã]o|producao)' \
  || { echo "FAIL: autorização"; exit 3; }

git push origin "$PROD_REF:refs/heads/$BACKUP_BR" || true
git push origin "$PREP_REF:producao"
PREP_FULL="$(git rev-parse --short "$PREP_REF")"
echo "PUSH OK · git producao @ $PREP_FULL · VERSION v$VER_ESPERADA"
echo ""
echo ">>> RENDER: confira se iniciou build NOVO em «Sistvale - Produção» (branch producao)."
echo ">>> Se o painel não mostrar deploy após ~1 min: Manual Deploy → Deploy latest commit."
echo ">>> Só depois do build verde: Ctrl+F5 na Vila e smoke vale."
echo ">>> Doc: docs/DEPLOY-PREP-RH-VALE-VILA.md § Render"
