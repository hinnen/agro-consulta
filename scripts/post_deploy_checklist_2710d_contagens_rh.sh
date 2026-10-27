#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
PY=python3
[[ -x .venv/bin/python ]] && PY=.venv/bin/python
"$PY" scripts/verify_contagem_ciclica_admin_path.py
"$PY" scripts/verify_vendas_lojas_pwa_install.py
echo "POST_DEPLOY_OK 2710d-contagens-rh"
