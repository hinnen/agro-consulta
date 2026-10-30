#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
PY=python3
[[ -x .venv/bin/python ]] && PY=.venv/bin/python
"$PY" scripts/verify_dfe_multi_cnpj.py
"$PY" scripts/verify_dfe_nsu_656.py
echo "POST_DEPLOY_OK 2210-dfe-vila-buscar"
