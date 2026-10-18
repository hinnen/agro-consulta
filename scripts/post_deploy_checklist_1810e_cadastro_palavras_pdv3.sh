#!/usr/bin/env bash
# Pós-deploy · CADASTRO-PALAVRAS-PDV-3
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
export AGRO_PIN_TESTE="${AGRO_PIN_TESTE:-9973}"
python scripts/verify_cadastro_busca_med_path.py | tail -5
echo "Uma vez: python manage.py backfill_busca_palavras_overlay"
