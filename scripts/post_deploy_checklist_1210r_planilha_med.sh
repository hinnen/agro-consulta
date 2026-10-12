#!/usr/bin/env bash
# Pós-deploy loja — PLANILHA-MED-EXCEL v27.13
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
PY=python3
[[ -x .venv/bin/python ]] && PY=.venv/bin/python
echo "== Pós-deploy 12/10r planilha MED =="
AGRO_PIN_TESTE="${AGRO_PIN_TESTE:-9973}" "$PY" scripts/verify_planilha_med_path.py | tail -3
echo "Smoke: cadastro ERP → Excel ↓ → colunas Med · Ctrl+F5"
