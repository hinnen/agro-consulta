#!/usr/bin/env bash
# Pós-deploy — ETQ-NF-QTD-FILA · 16/10d · v27.23
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
PY=python3
[[ -x .venv/bin/python ]] && PY=.venv/bin/python
export AGRO_PIN_TESTE="${AGRO_PIN_TESTE:-9973}"
"$PY" scripts/verify_etq_filtro_nf_entrada_path.py
echo "POST_DEPLOY_OK 1610d-etq-nf-qtd"
