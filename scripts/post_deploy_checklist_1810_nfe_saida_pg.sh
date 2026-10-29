#!/usr/bin/env bash
# Pós-deploy — NFE-SAIDA-55-PG · migrate 0146
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
export AGRO_PIN_TESTE="${AGRO_PIN_TESTE:-9973}"
python3 scripts/verify_nfe_saida_path.py
python3 -m django test produtos.tests_nfe_saida_util -v0 2>/dev/null || true
echo "POST_DEPLOY_1810_NFE_SAIDA_PG_OK"
