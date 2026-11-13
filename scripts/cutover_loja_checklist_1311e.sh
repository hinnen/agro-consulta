#!/usr/bin/env bash
# Aponta o SistApp para o Zap que está online · v27.26.63
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
PROD_ESPERADO="eb34cf2aeef01b228483a1e97b5e455eaeed656d"
VER="27.26.63"
SENHA_OK="99738595"
WT="${TMPDIR:-/tmp}/agro-cutover-1311e-$$"
git fetch origin producao
PROD="$(git rev-parse origin/producao)"
[[ "$PROD" == "$PROD_ESPERADO" ]] || { echo "FAIL: producao mudou ($PROD)"; exit 2; }
rm -rf "$WT"
git worktree add --detach "$WT" "$PROD"
cleanup() { git worktree remove --force "$WT" 2>/dev/null || rm -rf "$WT"; }
trap cleanup EXIT
git -C "$WT" apply "$ROOT/scripts/prep_patch_checklist_1311e.patch"
printf '%s\n' "$VER" > "$WT/VERSION"
mkdir -p "$WT/scripts" "$WT/docs"
cp "$ROOT/scripts/cutover_loja_checklist_1311e.sh" "$WT/scripts/"
cp "$ROOT/scripts/prep_patch_checklist_1311e.patch" "$WT/scripts/"
cp "$ROOT/docs/DEPLOY-PREP-CHECKLIST-1311e.md" "$WT/docs/"
PY=python3
[[ -x "$ROOT/.venv/bin/python" ]] && PY="$ROOT/.venv/bin/python"
( cd "$WT" && "$PY" -m py_compile produtos/views_atendimento_whatsapp.py )
git -C "$WT" add VERSION produtos/views_atendimento_whatsapp.py scripts/cutover_loja_checklist_1311e.sh scripts/prep_patch_checklist_1311e.patch docs/DEPLOY-PREP-CHECKLIST-1311e.md
git -C "$WT" -c user.email="$(git -C "$ROOT" config user.email)" -c user.name="$(git -C "$ROOT" config user.name)" commit -m "prep(loja): v27.26.63 SistApp fala com o Zap que está online"
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
