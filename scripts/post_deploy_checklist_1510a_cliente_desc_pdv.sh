#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
PY=python3
[[ -x .venv/bin/python ]] && PY=.venv/bin/python
"$PY" manage.py migrate --noinput
export AGRO_PIN_TESTE="${AGRO_PIN_TESTE:-9973}"
"$PY" scripts/verify_cliente_lista_desc_pdv_path.py | tail -2
echo "OK pós-deploy 15/10a — migrate 0145"
