#!/usr/bin/env bash
# Corte 24/11 — preco do pacote na nota + pedido de etiquetas no PDV.
# Em cima da loja v27.26.86. Nao e merge do teste.
# Sem --exec so confere o patch. Nao envia.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
PROD_ESPERADO="a7fa0150"
VER="27.26.95"
SENHA_OK="99738595"
WT="${TMPDIR:-/tmp}/agro-cutover-2411-preco-etq-$$"
git fetch origin producao
PROD="$(git rev-parse origin/producao)"
[[ "$PROD" == "$PROD_ESPERADO"* ]] || { echo "FAIL: producao mudou ($PROD)"; exit 2; }
rm -rf "$WT"
git worktree add --detach "$WT" "$PROD"
cleanup() { git worktree remove --force "$WT" 2>/dev/null || rm -rf "$WT"; }
trap cleanup EXIT
git -C "$WT" apply --binary "$ROOT/scripts/prep_patch_checklist_2411_preco_etq.patch"
printf '%s' "$VER" > "$WT/VERSION"
grep -q "entradaNfeLinhaCorrigirBasePacote" "$WT/produtos/templates/produtos/entrada_nota.html"
grep -q "pdv-topbar-etiquetas-btn" "$WT/produtos/templates/produtos/pdv_wizard.html"
grep -q "Troca de pre" "$WT/produtos/templates/produtos/pdv_wizard.html"
grep -q "api_etiquetas_solicitacao_pdv_criar" "$WT/produtos/views_etiquetas_solicitacao.py"
grep -q "etiquetas/solicitacoes/pdv/" "$WT/produtos/urls.py"
git -C "$WT" add -A
EMAIL="$(git -C "$ROOT" config user.email || true)"
NAME="$(git -C "$ROOT" config user.name || true)"
EMAIL="${EMAIL:-cursoragent@cursor.com}"
NAME="${NAME:-Cursor Agent}"
git -C "$WT" -c user.email="$EMAIL" -c user.name="$NAME" commit -m "prep(loja): v27.26.95 preco do pacote e etiquetas do PDV"
echo "OK: corte v$VER em cima de ${PROD:0:8}"
echo "Entra: pacote da nota nao multiplica de novo; PDV pede etiquetas."
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
