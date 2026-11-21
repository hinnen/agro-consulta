#!/usr/bin/env bash
# Corte 21/11c — barras do cartao ocupam o retangulo branco.
# Em cima da loja v27.26.85. Nao e merge do teste.
# Sem migrate. Sem --exec so monta. Nao envia.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
PROD_ESPERADO="b45d9cbc"
VER="27.26.86"
SENHA_OK="99738595"
WT="${TMPDIR:-/tmp}/agro-cutover-2111c-barras-$$"
git fetch origin producao
PROD="$(git rev-parse origin/producao)"
[[ "$PROD" == "$PROD_ESPERADO"* ]] || { echo "FAIL: producao mudou ($PROD)"; exit 2; }
rm -rf "$WT"
git worktree add --detach "$WT" "$PROD"
cleanup() { git worktree remove --force "$WT" 2>/dev/null || rm -rf "$WT"; }
trap cleanup EXIT
git -C "$WT" apply --binary "$ROOT/scripts/prep_patch_checklist_2111c_cartao_barras.patch"
[[ "$(tr -d '[:space:]' < "$WT/VERSION")" == "$VER" ]] || { echo "FAIL: versao"; exit 2; }
grep -q "preserveAspectRatio" "$WT/produtos/templates/produtos/caixa_cartao_cliente_imprimir.html"
grep -q "margin: 4" "$WT/produtos/templates/produtos/caixa_cartao_cliente_imprimir.html"
grep -q "top: 64.5%" "$WT/produtos/templates/produtos/caixa_cartao_cliente_imprimir.html"
git -C "$WT" add -A
EMAIL="$(git -C "$ROOT" config user.email || true)"
NAME="$(git -C "$ROOT" config user.name || true)"
EMAIL="${EMAIL:-cursoragent@cursor.com}"
NAME="${NAME:-Cursor Agent}"
git -C "$WT" -c user.email="$EMAIL" -c user.name="$NAME" commit -m "prep(loja): v27.26.86 barras grossas do cartao"
echo "OK: corte v$VER em cima de ${PROD:0:8}"
echo "Entra: barras ocupam a largura do retangulo branco."
echo "Nao muda: QR, cartao ja impresso, PDV, caixa, venda. Sem tabela nova."
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
