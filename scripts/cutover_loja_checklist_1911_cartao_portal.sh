#!/usr/bin/env bash
# Corte 19/11 — pagina do cartao no celular.
# Em cima da loja v27.26.82. Nao e merge do teste.
# Migrate 0157 (dia e mes do aniversario). Sem --exec so monta. Nao envia.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
PROD_ESPERADO="fb15ae8a"
VER="27.26.83"
SENHA_OK="99738595"
WT="${TMPDIR:-/tmp}/agro-cutover-1911-cartao-$$"
git fetch origin producao
PROD="$(git rev-parse origin/producao)"
[[ "$PROD" == "$PROD_ESPERADO"* ]] || { echo "FAIL: producao mudou ($PROD)"; exit 2; }
rm -rf "$WT"
git worktree add --detach "$WT" "$PROD"
cleanup() { git worktree remove --force "$WT" 2>/dev/null || rm -rf "$WT"; }
trap cleanup EXIT
git -C "$WT" apply --binary "$ROOT/scripts/prep_patch_checklist_1911_cartao_portal.patch"
[[ "$(tr -d '[:space:]' < "$WT/VERSION")" == "$VER" ]] || { echo "FAIL: versao"; exit 2; }
grep -q "Minhas compras" "$WT/produtos/templates/produtos/cliente_cartao_portal.html"
grep -q "Meu anivers" "$WT/produtos/templates/produtos/cliente_cartao_portal.html"
grep -q "0156_cliente_cartao_agro" "$WT/produtos/migrations/0157_clienteagro_aniversario.py"
grep -q "aniversario_dia" "$WT/produtos/models.py"
git -C "$WT" add -A
EMAIL="$(git -C "$ROOT" config user.email || true)"
NAME="$(git -C "$ROOT" config user.name || true)"
EMAIL="${EMAIL:-cursoragent@cursor.com}"
NAME="${NAME:-Cursor Agent}"
git -C "$WT" -c user.email="$EMAIL" -c user.name="$NAME" commit -m "prep(loja): v27.26.83 pagina do cartao"
echo "OK: corte v$VER em cima de ${PROD:0:8}"
echo "Entra: tela do celular do cartao. Compras e aniversario em botoes."
echo "Migrate 0157: dia e mes, campos vazios. Nao mexe em venda, caixa nem PDV."
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
