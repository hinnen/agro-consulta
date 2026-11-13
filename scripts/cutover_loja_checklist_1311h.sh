#!/usr/bin/env bash
# Hora da loja na entrega + câmera do SistApp no próprio sistema · v27.26.66
# Sem --exec só confere. Com --exec exige frase + senha.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
PROD_ESPERADO="3b9fc063612d72754a0ac3a657b68a78fdb56bba"
VER="27.26.66"
SENHA_OK="99738595"
WT="${TMPDIR:-/tmp}/agro-cutover-1311h-$$"
git fetch origin producao
PROD="$(git rev-parse origin/producao)"
[[ "$PROD" == "$PROD_ESPERADO" ]] || { echo "FAIL: producao mudou ($PROD)"; exit 2; }
rm -rf "$WT"
git worktree add --detach "$WT" "$PROD"
cleanup() { git worktree remove --force "$WT" 2>/dev/null || rm -rf "$WT"; }
trap cleanup EXIT
git -C "$WT" apply "$ROOT/scripts/prep_patch_checklist_1311h.patch"
cp "$ROOT/produtos/static/produtos/js/html5-qrcode.min.js" "$WT/produtos/static/produtos/js/html5-qrcode.min.js"
printf '%s\n' "$VER" > "$WT/VERSION"
mkdir -p "$WT/scripts" "$WT/docs"
cp "$ROOT/scripts/cutover_loja_checklist_1311h.sh" "$WT/scripts/"
cp "$ROOT/scripts/prep_checkpoint_checklist_1311h.sh" "$WT/scripts/"
cp "$ROOT/scripts/prep_patch_checklist_1311h.patch" "$WT/scripts/"
cp "$ROOT/scripts/verify_entrega_hora_loja_path.py" "$WT/scripts/"
cp "$ROOT/scripts/verify_sistapp_camera_local_path.py" "$WT/scripts/"
cp "$ROOT/docs/DEPLOY-PREP-CHECKLIST-1311h.md" "$WT/docs/"
PY=python3
[[ -x "$ROOT/.venv/bin/python" ]] && PY="$ROOT/.venv/bin/python"
( cd "$WT" && "$PY" scripts/verify_entrega_hora_loja_path.py )
git -C "$WT" add -A
git -C "$WT" -c user.email="$(git -C "$ROOT" config user.email)" -c user.name="$(git -C "$ROOT" config user.name)" \
  commit -m "prep(loja): v27.26.66 hora da entrega e câmera do SistApp"
echo "OK: corte v$VER em cima de ${PROD:0:8} · sem migrate"
echo "VOLTA: git push origin ${PROD}:producao"
echo "Entra: papel da entrega no horário da loja. Câmera e letra do SistApp sem site de fora."
echo "Não muda o valor da venda nem o caixa."
[[ "${1:-}" != "--exec" ]] && exit 0
FRASE="${AGRO_PIN_FRASE:-${AGRO_LOJA_FRASE:-}}"
SENHA="${AGRO_LOJA_SENHA:-}"
[[ "$SENHA" == "$SENHA_OK" ]] || { echo "FAIL: senha"; exit 3; }
echo "$FRASE" | grep -Eiq 'pode.*(produ[cç][aã]o|producao|enviar)|sobe.*(produ[cç][aã]o|producao)|subir.*(produ[cç][aã]o|producao)|enviar.*(produ[cç][aã]o|producao)' || { echo "FAIL: autorização"; exit 3; }
NOVO="$(git -C "$WT" rev-parse HEAD)"
git push origin "$NOVO:producao"
echo "PUSH OK v$VER"
echo "REVERT: git push origin ${PROD}:producao"
