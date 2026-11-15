#!/usr/bin/env bash
# Botão Loja mostra a janela quando o orçamento foi · v27.26.74
# Sem --exec: só monta o corte e para. Não mexe na loja.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
PROD_ESPERADO="b25069cfbd01f37fe580d0e48e070f9dde057718"
VER="27.26.74"
SENHA_OK="99738595"
WT="${TMPDIR:-/tmp}/agro-cutover-1511c-zap-aviso-$$"
git fetch origin producao
PROD="$(git rev-parse origin/producao)"
[[ "$PROD" == "$PROD_ESPERADO" ]] || { echo "FAIL: producao mudou ($PROD)"; exit 2; }
rm -rf "$WT"
git worktree add --detach "$WT" "$PROD"
cleanup() { git worktree remove --force "$WT" 2>/dev/null || rm -rf "$WT"; }
trap cleanup EXIT
git -C "$WT" apply "$ROOT/scripts/prep_patch_checklist_1511c_pdv_zap_aviso.patch"
grep -q "Orçamento enviado no Zap da loja." "$WT/produtos/static/produtos/js/pdv_wizard.js"
node --check "$WT/produtos/static/produtos/js/pdv_wizard.js"
printf '%s\n' "$VER" > "$WT/VERSION"
mkdir -p "$WT/scripts"
cp "$ROOT/scripts/cutover_loja_checklist_1511c_pdv_zap_aviso.sh" "$WT/scripts/"
cp "$ROOT/scripts/prep_patch_checklist_1511c_pdv_zap_aviso.patch" "$WT/scripts/"
git -C "$WT" add -A
git -C "$WT" -c user.email="$(git -C "$ROOT" config user.email)" -c user.name="$(git -C "$ROOT" config user.name)" \
  commit -m "prep(loja): v27.26.74 aviso quando o orçamento vai no Zap"
echo "OK: corte v$VER em cima de ${PROD:0:8} · sem migrate"
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
