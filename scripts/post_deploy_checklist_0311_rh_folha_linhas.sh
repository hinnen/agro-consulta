#!/usr/bin/env bash
# Render Shell, depois do push da loja. Só a migração do RH.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
PY=python3
[[ -x .venv/bin/python ]] && PY=.venv/bin/python
"$PY" manage.py migrate rh 0007_ajustemanualfolha --noinput
"$PY" scripts/verify_rh_folha_linhas_path.py
echo "POST_DEPLOY_OK 0311-rh-folha-linhas"
