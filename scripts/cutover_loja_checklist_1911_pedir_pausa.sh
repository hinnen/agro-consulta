#!/usr/bin/env bash
# Corte 19/11 — Pedir loja pausado.
# Em cima da loja v27.26.81. Não é merge do teste.
# Sem migrate. Sem --exec só monta e confere. Não envia.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
PROD_ESPERADO="59f184fa"
VER="27.26.82"
SENHA_OK="99738595"
WT="${TMPDIR:-/tmp}/agro-cutover-1911-pedir-$$"
git fetch origin producao
PROD="$(git rev-parse origin/producao)"
[[ "$PROD" == "$PROD_ESPERADO"* ]] || { echo "FAIL: producao mudou ($PROD)"; exit 2; }
rm -rf "$WT"
git worktree add --detach "$WT" "$PROD"
cleanup() { git worktree remove --force "$WT" 2>/dev/null || rm -rf "$WT"; }
trap cleanup EXIT
git -C "$WT" apply --binary "$ROOT/scripts/prep_patch_checklist_1911.patch"
[[ "$(tr -d '[:space:]' < "$WT/VERSION")" == "$VER" ]] || { echo "FAIL: versao"; exit 2; }
grep -q 'PEDIR_LOJA_PAUSADO = true' "$WT/produtos/static/produtos/js/pdv_pedir_loja.js"
grep -q 'avisoPedirPausado' "$WT/produtos/static/produtos/js/pdv_pedir_loja.js"
grep -q 'window.showPdvAviso = showPdvAviso' "$WT/produtos/static/produtos/js/pdv_wizard.js"
grep -q 'pdv-wiz-topbar-btn--pausado' "$WT/produtos/templates/produtos/pdv_wizard.html"
node --check "$WT/produtos/static/produtos/js/pdv_pedir_loja.js"
node --check "$WT/produtos/static/produtos/js/pdv_wizard.js"
git -C "$WT" add -A
git -C "$WT" -c user.email="$(git -C "$ROOT" config user.email)" -c user.name="$(git -C "$ROOT" config user.name)" \
  commit -m "prep(loja): v27.26.82 Pedir loja pausado"
echo "OK: corte v$VER em cima de ${PROD:0:8}"
echo "Entra: botão Pedir loja cinza. O clique avisa e não abre. Sem apito."
echo "Não muda: venda, caixa, estoque, fiado. Sem tabela nova."
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
