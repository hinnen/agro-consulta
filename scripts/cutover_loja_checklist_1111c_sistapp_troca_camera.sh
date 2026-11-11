#!/usr/bin/env bash
# Troca de câmera no SistApp · v27.26.57
# Sem --exec: só confere. Com --exec: exige frase + senha e aí sim envia a loja.
# Não usa branch deploy/ nem rollback/ antes da senha.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
SRC="77de8913c24c03450bf9a495de6160e9591f3754"
PROD_ESPERADO="c8256c2664d86d5d9c79cc974d5bc10b104666d7"
VER="27.26.57"
ROLLBACK_TAG="rollback/pre-v272657-1111c-sistapp-troca-camera"
SENHA_OK="99738595"
WT="${TMPDIR:-/tmp}/agro-cutover-1111c-$$"

git fetch origin producao
PROD="$(git rev-parse origin/producao)"
[[ "$PROD" == "$PROD_ESPERADO" ]] || { echo "FAIL: producao mudou ($PROD)"; exit 2; }
git cat-file -e "$SRC^{commit}" || git fetch origin teste
git cat-file -e "$SRC^{commit}"

rm -rf "$WT"
git worktree add --detach "$WT" "$PROD"
cleanup() { git worktree remove --force "$WT" 2>/dev/null || rm -rf "$WT"; }
trap cleanup EXIT

git -C "$ROOT" diff "$SRC^" "$SRC" -- \
  produtos/static/produtos/js/sistapp_camera_escolha.js \
  produtos/templates/produtos/etiquetas_solicitacao/app.html \
  produtos/templates/produtos/fotos_produto/app.html \
  produtos/templates/produtos/mobile_ajuste.html \
  scripts/verify_ajuste_camera_bip_path.py \
  | git -C "$WT" apply --check
git -C "$ROOT" diff "$SRC^" "$SRC" -- \
  produtos/static/produtos/js/sistapp_camera_escolha.js \
  produtos/templates/produtos/etiquetas_solicitacao/app.html \
  produtos/templates/produtos/fotos_produto/app.html \
  produtos/templates/produtos/mobile_ajuste.html \
  scripts/verify_ajuste_camera_bip_path.py \
  | git -C "$WT" apply

printf '%s\n' "$VER" > "$WT/VERSION"
mkdir -p "$WT/scripts" "$WT/docs"
cp "$ROOT/scripts/cutover_loja_checklist_1111c_sistapp_troca_camera.sh" "$WT/scripts/"
cp "$ROOT/scripts/post_deploy_checklist_1111c_sistapp_troca_camera.sh" "$WT/scripts/"
cp "$ROOT/scripts/prep_checkpoint_checklist_1111c_sistapp_troca_camera.sh" "$WT/scripts/"
cp "$ROOT/scripts/verify_sistapp_troca_camera_path.py" "$WT/scripts/"
cp "$ROOT/docs/DEPLOY-PREP-CHECKLIST-1111c-sistapp-troca-camera.md" "$WT/docs/"

PY=python3
[[ -x "$WT/.venv/bin/python" ]] && PY="$WT/.venv/bin/python"
[[ -x "$ROOT/.venv/bin/python" ]] && PY="$ROOT/.venv/bin/python"
( cd "$WT" && "$PY" scripts/verify_sistapp_troca_camera_path.py )

git -C "$WT" add \
  VERSION \
  produtos/static/produtos/js/sistapp_camera_escolha.js \
  produtos/templates/produtos/etiquetas_solicitacao/app.html \
  produtos/templates/produtos/fotos_produto/app.html \
  produtos/templates/produtos/mobile_ajuste.html \
  scripts/verify_ajuste_camera_bip_path.py \
  scripts/verify_sistapp_troca_camera_path.py \
  scripts/cutover_loja_checklist_1111c_sistapp_troca_camera.sh \
  scripts/post_deploy_checklist_1111c_sistapp_troca_camera.sh \
  scripts/prep_checkpoint_checklist_1111c_sistapp_troca_camera.sh \
  docs/DEPLOY-PREP-CHECKLIST-1111c-sistapp-troca-camera.md
git -C "$WT" -c user.email="$(git -C "$ROOT" config user.email)" \
  -c user.name="$(git -C "$ROOT" config user.name)" \
  commit -m "prep(loja): v27.26.57 troca de câmera no SistApp"

DELTA="$(git -C "$WT" diff --name-only HEAD~1 | sort)"
ALLOWED="$(printf '%s\n' \
  VERSION \
  docs/DEPLOY-PREP-CHECKLIST-1111c-sistapp-troca-camera.md \
  produtos/static/produtos/js/sistapp_camera_escolha.js \
  produtos/templates/produtos/etiquetas_solicitacao/app.html \
  produtos/templates/produtos/fotos_produto/app.html \
  produtos/templates/produtos/mobile_ajuste.html \
  scripts/cutover_loja_checklist_1111c_sistapp_troca_camera.sh \
  scripts/post_deploy_checklist_1111c_sistapp_troca_camera.sh \
  scripts/prep_checkpoint_checklist_1111c_sistapp_troca_camera.sh \
  scripts/verify_ajuste_camera_bip_path.py \
  scripts/verify_sistapp_troca_camera_path.py | sort)"
[[ "$DELTA" == "$ALLOWED" ]] || { echo "FAIL: delta"; echo "$DELTA"; exit 2; }
echo "$DELTA" | grep -E '/migrations/' && { echo "FAIL: migrate"; exit 2; } || true

echo "OK: corte v$VER em cima de ${PROD:0:8} · sem migrate · volta seria $ROLLBACK_TAG"
echo "PDV, caixa e venda não entram."

[[ "${1:-}" != "--exec" ]] && exit 0
FRASE="${AGRO_PIN_FRASE:-${AGRO_LOJA_FRASE:-}}"
SENHA="${AGRO_LOJA_SENHA:-}"
[[ "$SENHA" == "$SENHA_OK" ]] || { echo "FAIL: senha"; exit 3; }
echo "$FRASE" | grep -Eiq 'pode.*(produ[cç][aã]o|producao|enviar)|sobe.*(produ[cç][aã]o|producao)|subir.*(produ[cç][aã]o|producao)|enviar.*(produ[cç][aã]o|producao)' \
  || { echo "FAIL: autorização"; exit 3; }
NOVO="$(git -C "$WT" rev-parse HEAD)"
git push origin "$PROD:refs/heads/$ROLLBACK_TAG"
git push origin "$NOVO:producao"
echo "PUSH OK v$VER"
echo "REVERT: git push origin $ROLLBACK_TAG:producao"
echo "SHELL: ./scripts/post_deploy_checklist_1111c_sistapp_troca_camera.sh"
