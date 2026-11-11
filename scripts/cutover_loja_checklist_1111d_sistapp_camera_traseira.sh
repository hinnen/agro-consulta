#!/usr/bin/env bash
# Correção: abre a câmera de trás e o botão troca a lente. v27.26.58
# Sem --exec: só confere. Com --exec: exige frase + senha e envia a loja.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
SRC="87de7db5"
PROD_ESPERADO="007e9bfc"
VER="27.26.58"
SENHA_OK="99738595"
WT="${TMPDIR:-/tmp}/agro-cutover-1111d-$$"
FILES=(
  produtos/static/produtos/js/sistapp_camera_escolha.js
  produtos/templates/produtos/etiquetas_solicitacao/app.html
  produtos/templates/produtos/fotos_produto/app.html
  produtos/templates/produtos/mobile_ajuste.html
)

git fetch origin producao
PROD="$(git rev-parse origin/producao)"
[[ "$(git rev-parse --short=8 "$PROD")" == "$PROD_ESPERADO" ]] || { echo "FAIL: producao mudou ($PROD)"; exit 2; }
git cat-file -e "$SRC^{commit}" || git fetch origin teste
git cat-file -e "$SRC^{commit}"

rm -rf "$WT"
git worktree add --detach "$WT" "$PROD"
cleanup() { git worktree remove --force "$WT" 2>/dev/null || rm -rf "$WT"; }
trap cleanup EXIT

git -C "$ROOT" diff "$SRC^" "$SRC" -- "${FILES[@]}" | git -C "$WT" apply --check
git -C "$ROOT" diff "$SRC^" "$SRC" -- "${FILES[@]}" | git -C "$WT" apply

printf '%s\n' "$VER" > "$WT/VERSION"
mkdir -p "$WT/scripts" "$WT/docs"
cp "$ROOT/scripts/cutover_loja_checklist_1111d_sistapp_camera_traseira.sh" "$WT/scripts/"
cp "$ROOT/scripts/post_deploy_checklist_1111d_sistapp_camera_traseira.sh" "$WT/scripts/"
cp "$ROOT/scripts/verify_sistapp_troca_camera_path.py" "$WT/scripts/"
cp "$ROOT/docs/DEPLOY-PREP-CHECKLIST-1111d-sistapp-camera-traseira.md" "$WT/docs/"

PY=python3
[[ -x "$ROOT/.venv/bin/python" ]] && PY="$ROOT/.venv/bin/python"
( cd "$WT" && "$PY" scripts/verify_sistapp_troca_camera_path.py )

git -C "$WT" add \
  VERSION \
  "${FILES[@]}" \
  scripts/verify_sistapp_troca_camera_path.py \
  scripts/cutover_loja_checklist_1111d_sistapp_camera_traseira.sh \
  scripts/post_deploy_checklist_1111d_sistapp_camera_traseira.sh \
  docs/DEPLOY-PREP-CHECKLIST-1111d-sistapp-camera-traseira.md
git -C "$WT" -c user.email="$(git -C "$ROOT" config user.email)" \
  -c user.name="$(git -C "$ROOT" config user.name)" \
  commit -m "prep(loja): v27.26.58 câmera de trás e troca de lente no SistApp"

echo "OK: corte v$VER em cima de ${PROD:0:8} · sem migrate"
echo "VOLTA: git push origin ${PROD}:producao"
echo "PDV, caixa e venda não entram."

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
echo "SHELL: ./scripts/post_deploy_checklist_1111d_sistapp_camera_traseira.sh"
