#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
[[ -n "${PORT:-}" ]] && curl -sf "http://127.0.0.1:${PORT}/healthz" && echo " healthz OK"
PY=python3
[[ -x .venv/bin/python ]] && PY=.venv/bin/python
export AGRO_PIN_TESTE="${AGRO_PIN_TESTE:-9973}"
"$PY" scripts/verify_planilha_med_path.py | tail -2
"$PY" scripts/verify_etq_solicitacao_vl_pin_path.py | tail -2
echo "OK pós-deploy 14/10d — planilha Med/ID + etiquetas PC fila/modal"
