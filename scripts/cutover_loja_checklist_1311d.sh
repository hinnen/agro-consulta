#!/usr/bin/env bash
# Ligação do Zap dentro do SistApp · v27.26.62
# Sem --exec: só confere. Com --exec: frase + senha e aí envia a loja.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
PROD_ESPERADO="e3745faef6d1767026b6de7526354f23c2c08d20"
VER="27.26.62"
SENHA_OK="99738595"
WT="${TMPDIR:-/tmp}/agro-cutover-1311d-$$"

git fetch origin producao
PROD="$(git rev-parse origin/producao)"
[[ "$PROD" == "$PROD_ESPERADO" ]] || { echo "FAIL: producao mudou ($PROD)"; exit 2; }

rm -rf "$WT"
git worktree add --detach "$WT" "$PROD"
cleanup() { git worktree remove --force "$WT" 2>/dev/null || rm -rf "$WT"; }
trap cleanup EXIT

git -C "$WT" apply "$ROOT/scripts/prep_patch_checklist_1311d.patch"
printf '%s\n' "$VER" > "$WT/VERSION"
mkdir -p "$WT/scripts" "$WT/docs"
cp "$ROOT/scripts/cutover_loja_checklist_1311d.sh" "$WT/scripts/"
cp "$ROOT/scripts/post_deploy_checklist_1311d.sh" "$WT/scripts/"
cp "$ROOT/scripts/prep_checkpoint_checklist_1311d.sh" "$WT/scripts/"
cp "$ROOT/scripts/prep_patch_checklist_1311d.patch" "$WT/scripts/"
cp "$ROOT/docs/DEPLOY-PREP-CHECKLIST-1311d.md" "$WT/docs/"

PY=python3
[[ -x "$ROOT/.venv/bin/python" ]] && PY="$ROOT/.venv/bin/python"
( cd "$WT" && "$PY" -m py_compile produtos/views_atendimento_whatsapp.py produtos/urls.py base/wa_split_middleware.py )

git -C "$WT" add \
  VERSION \
  base/wa_split_middleware.py \
  produtos/views_atendimento_whatsapp.py \
  produtos/urls.py \
  produtos/templates/produtos/atendimento_whatsapp_celular.html \
  scripts/cutover_loja_checklist_1311d.sh \
  scripts/post_deploy_checklist_1311d.sh \
  scripts/prep_checkpoint_checklist_1311d.sh \
  scripts/prep_patch_checklist_1311d.patch \
  docs/DEPLOY-PREP-CHECKLIST-1311d.md
git -C "$WT" -c user.email="$(git -C "$ROOT" config user.email)" \
  -c user.name="$(git -C "$ROOT" config user.name)" \
  commit -m "prep(loja): v27.26.62 ligação do Zap dentro do SistApp"

echo "OK: corte v$VER em cima de ${PROD:0:8} · sem migrate"
echo "VOLTA: git push origin ${PROD}:producao"
echo "A tela continua a mesma. A lista passa a vir do Zap ligado."

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
echo "SHELL: ./scripts/post_deploy_checklist_1311d.sh"
