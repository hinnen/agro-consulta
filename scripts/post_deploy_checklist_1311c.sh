#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
PY=python3
[[ -x .venv/bin/python ]] && PY=.venv/bin/python
"$PY" scripts/verify_vendas_lojas_pwa_install.py
echo "POST_DEPLOY_OK 1311c-zap-no-sistapp"
echo "Sem migrate. Ctrl+F5 no SistApp. Zap abre dentro do aplicativo. Início volta ao menu."
