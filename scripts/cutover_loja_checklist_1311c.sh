#!/usr/bin/env bash
# Zap dentro do SistApp · v27.26.61
# Sem --exec: só confere. Com --exec: frase + senha e aí envia a loja.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
PROD_ESPERADO="8cc412a936a6e49099ed27bbc2a86f0b53b93442"
VER="27.26.61"
SENHA_OK="99738595"
WT="${TMPDIR:-/tmp}/agro-cutover-1311c-$$"

git fetch origin producao
PROD="$(git rev-parse origin/producao)"
[[ "$PROD" == "$PROD_ESPERADO" ]] || { echo "FAIL: producao mudou ($PROD)"; exit 2; }

rm -rf "$WT"
git worktree add --detach "$WT" "$PROD"
cleanup() { git worktree remove --force "$WT" 2>/dev/null || rm -rf "$WT"; }
trap cleanup EXIT

git -C "$WT" apply "$ROOT/scripts/prep_patch_checklist_1311c.patch"
printf '%s\n' "$VER" > "$WT/VERSION"
mkdir -p "$WT/scripts" "$WT/docs"
cp "$ROOT/scripts/cutover_loja_checklist_1311c.sh" "$WT/scripts/"
cp "$ROOT/scripts/post_deploy_checklist_1311c.sh" "$WT/scripts/"
cp "$ROOT/scripts/prep_checkpoint_checklist_1311c.sh" "$WT/scripts/"
cp "$ROOT/scripts/prep_patch_checklist_1311c.patch" "$WT/scripts/"
cp "$ROOT/docs/DEPLOY-PREP-CHECKLIST-1311c.md" "$WT/docs/"

PY=python3
[[ -x "$ROOT/.venv/bin/python" ]] && PY="$ROOT/.venv/bin/python"
( cd "$WT" && "$PY" -m py_compile produtos/views_atendimento_whatsapp.py produtos/urls.py )
( cd "$WT" && "$PY" scripts/verify_vendas_lojas_pwa_install.py )

git -C "$WT" add \
  VERSION \
  produtos/views_atendimento_whatsapp.py \
  produtos/urls.py \
  produtos/templates/produtos/vendas_lojas_hub.html \
  produtos/templates/produtos/atendimento_whatsapp_celular.html \
  scripts/verify_vendas_lojas_pwa_install.py \
  scripts/cutover_loja_checklist_1311c.sh \
  scripts/post_deploy_checklist_1311c.sh \
  scripts/prep_checkpoint_checklist_1311c.sh \
  scripts/prep_patch_checklist_1311c.patch \
  docs/DEPLOY-PREP-CHECKLIST-1311c.md
git -C "$WT" -c user.email="$(git -C "$ROOT" config user.email)" \
  -c user.name="$(git -C "$ROOT" config user.name)" \
  commit -m "prep(loja): v27.26.61 Zap dentro do SistApp"

echo "OK: corte v$VER em cima de ${PROD:0:8} · sem migrate"
echo "VOLTA: git push origin ${PROD}:producao"
echo "O botão Zap abre dentro do SistApp. O ícone separado do Zap continua."

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
echo "SHELL: ./scripts/post_deploy_checklist_1311c.sh"
