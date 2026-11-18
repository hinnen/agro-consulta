#!/usr/bin/env bash
# Corte 18/11c — só a página do QR do cartão.
# Em cima da loja v27.26.77. Não é merge do teste.
# Não troca o token nem o endereço do QR já impresso.
# Sem --exec só monta e confere. Não envia.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
PROD_ESPERADO="2b90880a"
VER="27.26.78"
SENHA_OK="99738595"
WT="${TMPDIR:-/tmp}/agro-cutover-1811c-$$"
git fetch origin producao
PROD="$(git rev-parse origin/producao)"
[[ "$PROD" == "$PROD_ESPERADO"* ]] || { echo "FAIL: producao mudou ($PROD)"; exit 2; }
rm -rf "$WT"
git worktree add --detach "$WT" "$PROD"
cleanup() { git worktree remove --force "$WT" 2>/dev/null || rm -rf "$WT"; }
trap cleanup EXIT
git -C "$WT" apply --binary "$ROOT/scripts/prep_patch_checklist_1811c.patch"
printf '%s\n' "$VER" > "$WT/VERSION"
grep -q 'cartao.token' "$WT/produtos/views_cliente_cartao.py"
grep -q 'Falar com a loja' "$WT/produtos/templates/produtos/cliente_cartao_portal.html"
grep -q "cartao/<str:token>/" "$WT/produtos/urls.py"
if grep -RIn $'\x3c\x3c\x3c\x3c\x3c\x3c\x3c' "$WT/produtos/views_cliente_cartao.py" "$WT/produtos/templates/produtos/cliente_cartao_portal.html" >/dev/null; then
  echo "FAIL: conflito no codigo"
  exit 2
fi
git -C "$WT" add -A
git -C "$WT" -c user.email="cursoragent@cursor.com" -c user.name="Cursor Agent" \
  commit -m "prep(loja): v27.26.78 pagina do QR do cartao"
echo "OK: corte v$VER em cima de ${PROD:0:8}"
echo "Entra: QR já impresso abre pedindo o WhatsApp do cadastro. Se bater, mostra as compras. Se não, botão para a loja com o número do cartão."
echo "Não muda: barras, token, endereço do QR, PDV, caixa, venda. Sem tabela nova."
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
