#!/usr/bin/env bash
# Pós-deploy — hotfix GESTAO-MED-MODAL-GV (sem migrate)
set -euo pipefail
cd "$(dirname "$0")/.."

if [[ -n "${PORT:-}" ]]; then
  curl -sf "http://127.0.0.1:${PORT}/healthz" && echo " healthz OK"
fi

PY=python3
[[ -x .venv/bin/python ]] && PY=.venv/bin/python
AGRO_PIN_TESTE="${AGRO_PIN_TESTE:-9973}" "$PY" scripts/verify_classificacao_medicamentos_path.py | tail -3
echo "OK pós-deploy — /produtos/gestao/ + modal cadastro ERP medicamentos"
