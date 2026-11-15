#!/usr/bin/env bash
# Confere se a loja ainda está na base deste corte. Não envia nada.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
ESPERADO="fba12fa1a3ea1e2829fe951bb0c793885d8c2d63"
git fetch origin producao
ATUAL="$(git rev-parse origin/producao)"
[[ "$ATUAL" == "$ESPERADO" ]] || { echo "FAIL: loja mudou ($ATUAL)"; exit 2; }
grep -q 'https://zap.zapsistvale.online/api/atendimento-whatsapp/bridge/orcamento/' scripts/prep_patch_checklist_1511_pdv_zap_orcamento.patch
echo "PREP_CHECKPOINT_OK v27.26.72 base ${ESPERADO:0:8} sem migrate"
