#!/usr/bin/env bash
# Corte 21/11 — folha do cartao com barras e QR mais folgados.
# Em cima da loja v27.26.83. Nao e merge do teste.
# Sem migrate. Sem --exec so monta. Nao envia.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
PROD_ESPERADO="59ce4326"
VER="27.26.84"
SENHA_OK="99738595"
WT="${TMPDIR:-/tmp}/agro-cutover-2111-folha-$$"
git fetch origin producao
PROD="$(git rev-parse origin/producao)"
[[ "$PROD" == "$PROD_ESPERADO"* ]] || { echo "FAIL: producao mudou ($PROD)"; exit 2; }
rm -rf "$WT"
git worktree add --detach "$WT" "$PROD"
cleanup() { git worktree remove --force "$WT" 2>/dev/null || rm -rf "$WT"; }
trap cleanup EXIT
git -C "$WT" apply --binary "$ROOT/scripts/prep_patch_checklist_2111_cartao_folha.patch"
[[ "$(tr -d '[:space:]' < "$WT/VERSION")" == "$VER" ]] || { echo "FAIL: versao"; exit 2; }
grep -q "width: 60%" "$WT/produtos/templates/produtos/caixa_cartao_cliente_imprimir.html"
grep -q "margin: 12" "$WT/produtos/templates/produtos/caixa_cartao_cliente_imprimir.html"
grep -q "border=4" "$WT/produtos/views_cliente_cartao.py"
git -C "$WT" add -A
EMAIL="$(git -C "$ROOT" config user.email || true)"
NAME="$(git -C "$ROOT" config user.name || true)"
EMAIL="${EMAIL:-cursoragent@cursor.com}"
NAME="${NAME:-Cursor Agent}"
git -C "$WT" -c user.email="$EMAIL" -c user.name="$NAME" commit -m "prep(loja): v27.26.84 folha do cartao"
echo "OK: corte v$VER em cima de ${PROD:0:8}"
echo "Entra: barras mais grossas e QR maior na folha nova."
echo "Nao muda: cartao ja impresso, PDV, caixa, venda. Sem tabela nova."
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
