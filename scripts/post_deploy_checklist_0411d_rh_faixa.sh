#!/usr/bin/env bash
# Sem migrate. Corrigir faixa salarial não grava folha.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
PY=python3
[[ -x .venv/bin/python ]] && PY=.venv/bin/python
"$PY" scripts/verify_rh_faixa_salarial_path.py
echo "POST_DEPLOY_OK 0411d-rh-faixa"
