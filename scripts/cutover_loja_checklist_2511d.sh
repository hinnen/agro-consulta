#!/usr/bin/env bash
# Corte 25/11d — gestão de produtos busca código com 4 dígitos ou mais.
# Em cima da loja v27.26.100. Não é merge do teste.
# Sem --exec só confere o patch. Não envia.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
PROD_ESPERADO="fe7ff673"
VER="27.26.102"
SENHA_OK="99738595"
WT="${TMPDIR:-/tmp}/agro-cutover-2511d-$$"
git fetch origin producao
PROD="$(git rev-parse origin/producao)"
[[ "$PROD" == "$PROD_ESPERADO"* ]] || { echo "FAIL: producao mudou ($PROD)"; exit 2; }
rm -rf "$WT"
git worktree add --detach "$WT" "$PROD"
cleanup() { git worktree remove --force "$WT" 2>/dev/null || rm -rf "$WT"; }
trap cleanup EXIT
git -C "$WT" apply --binary "$ROOT/scripts/prep_patch_checklist_2511d.patch"
printf '%s' "$VER" > "$WT/VERSION"
grep -q "lim.length >= 4" "$WT/produtos/static/produtos/js/cadastro_erp_panel.js"
grep -q "cadastro_erp_panel.js' %}?v=32" "$WT/produtos/templates/produtos/produtos_cadastro_erp.html"
grep -q "def overlay_pids_por_codigo_fornecedor" "$WT/produtos/cadastro_busca_codigo_util.py"
git -C "$WT" add -A
EMAIL="$(git -C "$ROOT" config user.email || true)"
NAME="$(git -C "$ROOT" config user.name || true)"
EMAIL="${EMAIL:-cursoragent@cursor.com}"
NAME="${NAME:-Cursor Agent}"
git -C "$WT" -c user.email="$EMAIL" -c user.name="$NAME" commit -m "prep(loja): v27.26.102 gestao busca codigo com 4 digitos"
echo "OK: corte v$VER em cima de ${PROD:0:8}"
echo "Entra: gestao de produtos busca codigo com 4 digitos ou mais."
echo "Tabela nova: nao."
echo "VOLTA: git push origin ${PROD}:producao"
echo "DEPOIS: conferir se o painel da loja comecou o build. Se nao comecar, disparar."
[[ "${1:-}" != "--exec" ]] && exit 0
FRASE="${AGRO_PIN_FRASE:-${AGRO_LOJA_FRASE:-}}"
SENHA="${AGRO_LOJA_SENHA:-}"
[[ "$SENHA" == "$SENHA_OK" ]] || { echo "FAIL: senha"; exit 3; }
echo "$FRASE" | grep -Eiq 'pode.*(produ[cç][aã]o|producao|enviar)|sobe.*(produ[cç][aã]o|producao)|subir.*(produ[cç][aã]o|producao)|enviar.*(produ[cç][aã]o|producao)' || { echo "FAIL: autorizacao"; exit 3; }
NOVO="$(git -C "$WT" rev-parse HEAD)"
git push origin "$NOVO:producao"
echo "PUSH OK v$VER"
echo "REVERT: git push origin ${PROD}:producao"
