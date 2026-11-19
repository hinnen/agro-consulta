#!/usr/bin/env bash
# Corte 19/11b — ajuste do card escuro, do R$ da previsão e do espaço.
# Em cima da loja v27.26.80. Não é merge do teste.
# Sem migrate. Sem --exec só monta e confere. Não envia.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
PROD_ESPERADO="76ac13ac"
VER="27.26.81"
SENHA_OK="99738595"
WT="${TMPDIR:-/tmp}/agro-cutover-1911-vendas-$$"
git fetch origin producao
PROD="$(git rev-parse origin/producao)"
[[ "$PROD" == "$PROD_ESPERADO"* ]] || { echo "FAIL: producao mudou ($PROD)"; exit 2; }
rm -rf "$WT"
git worktree add --detach "$WT" "$PROD"
cleanup() { git worktree remove --force "$WT" 2>/dev/null || rm -rf "$WT"; }
trap cleanup EXIT
git -C "$WT" apply --binary "$ROOT/scripts/prep_patch_checklist_1911_vendas_mobile.patch"
printf '%s\n' "$VER" > "$WT/VERSION"
grep -q '.vl-total .vl-fiado-tag' "$WT/produtos/templates/produtos/vendas_lojas_resumo.html"
grep -q 'vl-prev-mini .vl-money' "$WT/produtos/templates/produtos/vendas_lojas_resumo.html"
grep -q 'data-loja="total"' "$WT/produtos/templates/produtos/vendas_lojas_resumo.html"
grep -q 'meses_fechados' "$WT/produtos/templates/produtos/vendas_lojas_resumo.html"
grep -q 'def vendas_lojas_seis_meses_fechados' "$WT/produtos/vendas_lojas_util.py"
grep -q 'vendas_lojas_seis_meses_fechados' "$WT/produtos/views.py"
git -C "$WT" add -A
git -C "$WT" -c user.email="cursoragent@cursor.com" -c user.name="Cursor Agent" \
  commit -m "prep(loja): v27.26.81 ajuste visual vendas SistApp"
echo "OK: corte v$VER em cima de ${PROD:0:8}"
echo "Entra: fiado empilhado no card escuro, R$ pequeno na previsão, tela mais cheia."
echo "Não muda: PDV, caixa, venda, Pulso, calendário. Sem tabela nova."
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
