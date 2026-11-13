#!/usr/bin/env bash
# Botão Zap do SistApp · v27.26.60
# Sem --exec: só confere. Com --exec: frase + senha e aí envia a loja.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
PROD_ESPERADO="418fb789bb88ad8098cada93fc254a6fcc4be272"
VER="27.26.60"
SENHA_OK="99738595"
WT="${TMPDIR:-/tmp}/agro-cutover-1311b-$$"

git fetch origin producao
PROD="$(git rev-parse origin/producao)"
[[ "$PROD" == "$PROD_ESPERADO" ]] || { echo "FAIL: producao mudou ($PROD)"; exit 2; }

rm -rf "$WT"
git worktree add --detach "$WT" "$PROD"
cleanup() { git worktree remove --force "$WT" 2>/dev/null || rm -rf "$WT"; }
trap cleanup EXIT

git -C "$WT" apply "$ROOT/scripts/prep_patch_checklist_1311b.patch"
printf '%s\n' "$VER" > "$WT/VERSION"
mkdir -p "$WT/scripts" "$WT/docs"
cp "$ROOT/scripts/cutover_loja_checklist_1311b.sh" "$WT/scripts/"
cp "$ROOT/scripts/post_deploy_checklist_1311b.sh" "$WT/scripts/"
cp "$ROOT/scripts/prep_checkpoint_checklist_1311b.sh" "$WT/scripts/"
cp "$ROOT/scripts/prep_patch_checklist_1311b.patch" "$WT/scripts/"
cp "$ROOT/docs/DEPLOY-PREP-CHECKLIST-1311b.md" "$WT/docs/"

PY=python3
[[ -x "$ROOT/.venv/bin/python" ]] && PY="$ROOT/.venv/bin/python"
( cd "$WT" && "$PY" scripts/verify_vendas_lojas_pwa_install.py )

git -C "$WT" add \
  VERSION \
  produtos/templates/produtos/vendas_lojas_hub.html \
  scripts/verify_vendas_lojas_pwa_install.py \
  scripts/cutover_loja_checklist_1311b.sh \
  scripts/post_deploy_checklist_1311b.sh \
  scripts/prep_checkpoint_checklist_1311b.sh \
  scripts/prep_patch_checklist_1311b.patch \
  docs/DEPLOY-PREP-CHECKLIST-1311b.md
git -C "$WT" -c user.email="$(git -C "$ROOT" config user.email)" \
  -c user.name="$(git -C "$ROOT" config user.name)" \
  commit -m "prep(loja): v27.26.60 botão Zap no SistApp"

echo "OK: corte v$VER em cima de ${PROD:0:8} · sem migrate"
echo "VOLTA: git push origin ${PROD}:producao"
echo "Entra só o botão Zap do SistApp. PDV, caixa e venda não entram."

[[ "${1:-}" != "--exec" ]] && exit 0
FRASE="${AGRO_PIN_FRASE:-${AGRO_LOJA_FRASE:-}}"
SENHA="${AGRO_LOJA_SENHA:-}"
[[ "$SENHA" == "$SENHA_OK" ]] || { echo "FAIL: senha"; exit 3; }
echo "$FRASE" | grep -Eiq 'pode.*(produ[cç][aã]o|producao|enviar)|sobe.*(produ[cç][aã]o|producao)|subir.*(produ[cç][aã]o|producao)|enviar.*(produ[cç][aã]o|producao)' \
  || { echo "FAIL: autorização"; exit 3; }
NOVO="$(git -C "$WT" rev-parse HEAD)"
git push origin "$NOVO:producao"
echo "PUSH OK v$VER"
echo "REVERT: git push origin ${PROD}:producao"
echo "SHELL: ./scripts/post_deploy_checklist_1311b.sh"
