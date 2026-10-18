#!/usr/bin/env bash
# PACOTE CADASTRO-PALAVRAS-PDV-3 · v27.26.5
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
PY="${PY:-$ROOT/.venv/bin/python}"
if [[ ! -x "$PY" ]]; then PY=python3; fi
export AGRO_PIN_TESTE="${AGRO_PIN_TESTE:-9973}"
cat VERSION
"$PY" scripts/verify_cadastro_busca_med_path.py
