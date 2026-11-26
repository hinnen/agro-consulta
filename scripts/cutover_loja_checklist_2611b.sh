#!/usr/bin/env bash
# Corte 26/11b — gaveta de Vendas com diferença e horário da leitura.
# Em cima da loja v27.26.105. Não é merge do teste.
# Sem --exec só confere o patch. Não envia.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
PROD_ESPERADO="c3526a16"
VER="27.26.107"
SENHA_OK="99738595"
WT="${TMPDIR:-/tmp}/agro-cutover-2611b-$$"
git fetch origin producao
PROD="$(git rev-parse origin/producao)"
[[ "$PROD" == "$PROD_ESPERADO"* ]] || { echo "FAIL: producao mudou ($PROD)"; exit 2; }
rm -rf "$WT"
git worktree add --detach "$WT" "$PROD"
cleanup() { git worktree remove --force "$WT" 2>/dev/null || rm -rf "$WT"; }
trap cleanup EXIT
git -C "$WT" apply --binary "$ROOT/scripts/prep_patch_checklist_2611b.patch"
printf '%s' "$VER" > "$WT/VERSION"
grep -q "leitura_fmt" "$WT/produtos/views.py"
grep -q "Estipulado" "$WT/produtos/templates/produtos/vendas_lojas_resumo.html"
grep -q "def _resolver_nome_codigo" "$WT/produtos/etiqueta_preco_pendente_util.py"
git -C "$WT" add -A
EMAIL="$(git -C "$ROOT" config user.email || true)"
NAME="$(git -C "$ROOT" config user.name || true)"
EMAIL="${EMAIL:-cursoragent@cursor.com}"
NAME="${NAME:-Cursor Agent}"
git -C "$WT" -c user.email="$EMAIL" -c user.name="$NAME" commit -m "prep(loja): v27.26.107 gaveta de vendas com hora"
echo "OK: corte v$VER em cima de ${PROD:0:8}"
echo "Entra: gaveta de Vendas mostra a diferenca e a hora da leitura."
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
