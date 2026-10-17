#!/usr/bin/env bash
# Pós-deploy — NFE-SAIDA-55 · 17/10 · v27.26
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
PY=python3
[[ -x .venv/bin/python ]] && PY=.venv/bin/python
export AGRO_PIN_TESTE="${AGRO_PIN_TESTE:-9973}"
"$PY" scripts/verify_nfe_saida_path.py
"$PY" manage.py test produtos.tests_nfe_saida_util -v 0
echo "POST_DEPLOY_OK 1710-nfe-saida-55"
