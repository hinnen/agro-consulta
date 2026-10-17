#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
[[ -n "${PORT:-}" ]] && curl -sf "http://127.0.0.1:${PORT}/healthz" && echo " healthz OK"
PY=python3
[[ -x .venv/bin/python ]] && PY=.venv/bin/python
export AGRO_PIN_TESTE="${AGRO_PIN_TESTE:-9973}"
"$PY" scripts/verify_rh_vale_caixa_vila_path.py | tail -5
"$PY" scripts/verify_saida_empresa_loja.py | tail -3
echo "OK pós-deploy RH-VALE-VILA — validar 1 vale real na Vila (lista Quem está levando + PIN)"
