#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
[[ -n "${PORT:-}" ]] && curl -sf "http://127.0.0.1:${PORT}/healthz" && echo " healthz OK"
PY=python3
[[ -x .venv/bin/python ]] && PY=.venv/bin/python
export AGRO_PIN_TESTE="${AGRO_PIN_TESTE:-9973}"
"$PY" scripts/verify_caixa_fechar_loja_path.py | tail -2
"$PY" scripts/verify_credito_limite_revisao_path.py | tail -2
echo "OK pós-deploy 15/10+15/10b"
