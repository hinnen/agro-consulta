#!/usr/bin/env bash
# Corte 23/11 — os tres juntos, em cima da loja v27.26.85.
# Troca de preco + aba 10 + barras do cartao. Nao e merge do teste.
# Sem --exec so monta. Nao envia.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
PROD_ESPERADO="b45d9cbc"
VER="27.26.86"
SENHA_OK="99738595"
WT="${TMPDIR:-/tmp}/agro-cutover-2311-tres-$$"
git fetch origin producao
PROD="$(git rev-parse origin/producao)"
[[ "$PROD" == "$PROD_ESPERADO"* ]] || { echo "FAIL: producao mudou ($PROD)"; exit 2; }
rm -rf "$WT"
git worktree add --detach "$WT" "$PROD"
cleanup() { git worktree remove --force "$WT" 2>/dev/null || rm -rf "$WT"; }
trap cleanup EXIT
git -C "$WT" apply --binary "$ROOT/scripts/prep_patch_checklist_2311_tres.patch"
printf '%s' "$VER" > "$WT/VERSION"
grep -q "Troca de preço" "$WT/produtos/templates/produtos/pdv_wizard.html"
grep -q "0157_clienteagro_aniversario" "$WT/produtos/migrations/0158_etiqueta_preco_gondola.py"
grep -q "ok_por" "$WT/produtos/migrations/0159_etiqueta_preco_ok_por.py"
grep -q "border=1" "$WT/produtos/views_cliente_cartao.py"
git -C "$WT" add -A
EMAIL="$(git -C "$ROOT" config user.email || true)"
NAME="$(git -C "$ROOT" config user.name || true)"
EMAIL="${EMAIL:-cursoragent@cursor.com}"
NAME="${NAME:-Cursor Agent}"
git -C "$WT" -c user.email="$EMAIL" -c user.name="$NAME" commit -m "prep(loja): v27.26.86 troca de preco, aba 10 e barras do cartao"
echo "OK: corte v$VER em cima de ${PROD:0:8}"
echo "Entra: troca de preco no PDV, aba 10 so o que mudou, barras do cartao."
echo "Tabela nova: 0158 e 0159. O build da loja ja roda migrate."
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
