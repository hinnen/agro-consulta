#!/usr/bin/env bash
# Pós-deploy — CADASTRO-PALAVRAS-REABRE · 16/10e · v27.24
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
PY=python3
[[ -x .venv/bin/python ]] && PY=.venv/bin/python
export AGRO_PIN_TESTE="${AGRO_PIN_TESTE:-9973}"
"$PY" scripts/verify_cadastro_busca_med_path.py
echo "POST_DEPLOY_OK 1610e-cadastro-palavras-reabre"
