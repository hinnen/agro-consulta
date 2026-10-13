#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
[[ -n "${PORT:-}" ]] && curl -sf "http://127.0.0.1:${PORT}/healthz" && echo " healthz OK"
PY=python3
[[ -x .venv/bin/python ]] && PY=.venv/bin/python
export AGRO_PIN_TESTE="${AGRO_PIN_TESTE:-9973}"
"$PY" scripts/verify_med_subcategoria_dicas_path.py | tail -3
echo "OK pós-deploy 13/10b — dicas subcategorias MED"
