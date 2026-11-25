#!/usr/bin/env bash
# Corte 25/11c — busca acha o código do fornecedor.
# Em cima da loja v27.26.99. Não é merge do teste.
# Sem --exec só confere o patch. Não envia.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
PROD_ESPERADO="3143bbb8"
VER="27.26.100"
SENHA_OK="99738595"
WT="${TMPDIR:-/tmp}/agro-cutover-2511c-$$"
git fetch origin producao
PROD="$(git rev-parse origin/producao)"
[[ "$PROD" == "$PROD_ESPERADO"* ]] || { echo "FAIL: producao mudou ($PROD)"; exit 2; }
rm -rf "$WT"
git worktree add --detach "$WT" "$PROD"
cleanup() { git worktree remove --force "$WT" 2>/dev/null || rm -rf "$WT"; }
trap cleanup EXIT
git -C "$WT" apply --binary "$ROOT/scripts/prep_patch_checklist_2511c.patch"
printf '%s' "$VER" > "$WT/VERSION"
grep -q "def overlay_pids_por_codigo_fornecedor" "$WT/produtos/cadastro_busca_codigo_util.py"
grep -q "overlay_pids_por_codigo_fornecedor" "$WT/produtos/catalogo_agro.py"
grep -q "def _celula_ja_igual" "$WT/produtos/cadastro_planilha_util.py"
git -C "$WT" add -A
EMAIL="$(git -C "$ROOT" config user.email || true)"
NAME="$(git -C "$ROOT" config user.name || true)"
EMAIL="${EMAIL:-cursoragent@cursor.com}"
NAME="${NAME:-Cursor Agent}"
git -C "$WT" -c user.email="$EMAIL" -c user.name="$NAME" commit -m "prep(loja): v27.26.100 busca pelo codigo do fornecedor"
echo "OK: corte v$VER em cima de ${PROD:0:8}"
echo "Entra: busca do cadastro e da nota acha o codigo do fornecedor."
echo "Tabela nova: nao."
echo "VOLTA: git push origin ${PROD}:producao"
[[ "${1:-}" != "--exec" ]] && exit 0
FRASE="${AGRO_PIN_FRASE:-${AGRO_LOJA_FRASE:-}}"
SENHA="${AGRO_LOJA_SENHA:-}"
[[ "$SENHA" == "$SENHA_OK" ]] || { echo "FAIL: senha"; exit 3; }
echo "$FRASE" | grep -Eiq 'pode.*(produ[cç][aã]o|producao|enviar)|sobe.*(produ[cç][aã]o|producao)|subir.*(produ[cç][aã]o|producao)|enviar.*(produ[cç][aã]o|producao)' || { echo "FAIL: autorizacao"; exit 3; }
NOVO="$(git -C "$WT" rev-parse HEAD)"
git push origin "$NOVO:producao"
echo "PUSH OK v$VER"
echo "REVERT: git push origin ${PROD}:producao"
