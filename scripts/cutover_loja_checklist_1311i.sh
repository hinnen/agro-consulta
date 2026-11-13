#!/usr/bin/env bash
# Câmera abre na hora e a nitidez entra depois · v27.26.67
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
PROD_ESPERADO="edb926e23f6ccabda70cfe7e5c3d18caa3cee4b5"
VER="27.26.67"
SENHA_OK="99738595"
WT="${TMPDIR:-/tmp}/agro-cutover-1311i-$$"
git fetch origin producao
PROD="$(git rev-parse origin/producao)"
[[ "$PROD" == "$PROD_ESPERADO" ]] || { echo "FAIL: producao mudou ($PROD)"; exit 2; }
rm -rf "$WT"
git worktree add --detach "$WT" "$PROD"
cleanup() { git worktree remove --force "$WT" 2>/dev/null || rm -rf "$WT"; }
trap cleanup EXIT
git -C "$WT" apply "$ROOT/scripts/prep_patch_checklist_1311i.patch"
printf '%s\n' "$VER" > "$WT/VERSION"
mkdir -p "$WT/scripts"
cp "$ROOT/scripts/cutover_loja_checklist_1311i.sh" "$WT/scripts/"
cp "$ROOT/scripts/prep_patch_checklist_1311i.patch" "$WT/scripts/"
node --check "$WT/produtos/static/produtos/js/sistapp_camera_escolha.js"
git -C "$WT" add -A
git -C "$WT" -c user.email="$(git -C "$ROOT" config user.email)" -c user.name="$(git -C "$ROOT" config user.name)" \
  commit -m "prep(loja): v27.26.67 câmera abre mais rápido"
echo "OK: corte v$VER em cima de ${PROD:0:8} · sem migrate"
echo "VOLTA: git push origin ${PROD}:producao"
[[ "${1:-}" != "--exec" ]] && exit 0
FRASE="${AGRO_PIN_FRASE:-${AGRO_LOJA_FRASE:-}}"
SENHA="${AGRO_LOJA_SENHA:-}"
[[ "$SENHA" == "$SENHA_OK" ]] || { echo "FAIL: senha"; exit 3; }
echo "$FRASE" | grep -Eiq 'pode.*(produ[cç][aã]o|producao|enviar)|sobe.*(produ[cç][aã]o|producao)|subir.*(produ[cç][aã]o|producao)|enviar.*(produ[cç][aã]o|producao)' || { echo "FAIL: autorização"; exit 3; }
NOVO="$(git -C "$WT" rev-parse HEAD)"
git push origin "$NOVO:producao"
echo "PUSH OK v$VER"
echo "REVERT: git push origin ${PROD}:producao"
