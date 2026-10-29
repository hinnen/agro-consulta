#!/usr/bin/env bash
# Pós-deploy — CADASTRO-PALAVRAS-PDV · 16/10f · v27.25
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
PY=python3
[[ -x .venv/bin/python ]] && PY=.venv/bin/python
export AGRO_PIN_TESTE="${AGRO_PIN_TESTE:-9973}"
"$PY" scripts/verify_cadastro_busca_med_path.py
"$PY" -m django test produtos.tests_cadastro_busca_pdv_alinhada -v 0
echo "POST_DEPLOY_OK 1610f-cadastro-palavras-pdv"
