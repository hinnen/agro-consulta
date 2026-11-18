#!/usr/bin/env bash
# Corte 18/11 — cartão do cliente, imposto no boleto, cadastro pela nota.
# Em cima da loja v27.26.76. Não é merge do teste.
# Sem --exec só monta e confere. Não envia.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
PROD_ESPERADO="f58903ed"
VER="27.26.77"
SENHA_OK="99738595"
WT="${TMPDIR:-/tmp}/agro-cutover-1811-$$"
git fetch origin producao
PROD="$(git rev-parse origin/producao)"
[[ "$PROD" == "$PROD_ESPERADO"* ]] || { echo "FAIL: producao mudou ($PROD)"; exit 2; }
rm -rf "$WT"
git worktree add --detach "$WT" "$PROD"
cleanup() { git worktree remove --force "$WT" 2>/dev/null || rm -rf "$WT"; }
trap cleanup EXIT
git -C "$WT" apply --binary "$ROOT/scripts/prep_patch_checklist_1811.patch"
[[ "$(tr -d '[:space:]' < "$WT/VERSION")" == "$VER" ]] || { echo "FAIL: versao"; exit 2; }
test -f "$WT/produtos/migrations/0156_cliente_cartao_agro.py"
test ! -f "$WT/produtos/migrations/0153_cliente_cartao_agro.py"
test -f "$WT/produtos/migrations/0153_nfe_fiscal_contador.py"
grep -q '0155_pdv_operacao_log_agro' "$WT/produtos/migrations/0156_cliente_cartao_agro.py"
grep -q 'qrcode>=8.0' "$WT/requirements.txt"
grep -q "slice(0, 3) === '981'" "$WT/produtos/static/produtos/js/pdv_wizard.js"
grep -q 'soma_parcelas_entrada_nf_aceita' "$WT/produtos/nfe_entrada_util.py"
node --check "$WT/produtos/static/produtos/js/pdv_wizard.js"
node --check "$WT/produtos/static/produtos/js/pdv_state.js"
if grep -RIn $'\x3c\x3c\x3c\x3c\x3c\x3c\x3c' "$WT/produtos" "$WT/requirements.txt" >/dev/null; then
  echo "FAIL: conflito no codigo"
  exit 2
fi
git -C "$WT" add -A
git -C "$WT" -c user.email="$(git -C "$ROOT" config user.email)" -c user.name="$(git -C "$ROOT" config user.name)" \
  commit -m "prep(loja): v27.26.77 cartão, boleto e cadastro da nota"
echo "OK: corte v$VER em cima de ${PROD:0:8}"
echo "Entra: cartão (tabela 0156), imposto no boleto, cadastro pela nota, texto das regras."
echo "Fora: o resto do teste. PDV, caixa e venda seguem; o 981 deixa de cair como produto."
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
echo "SHELL: ./scripts/post_deploy_checklist_1811.sh"
