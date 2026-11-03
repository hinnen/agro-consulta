#!/usr/bin/env bash
# Sem migrate. Holerite só lê a folha.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
PY=python3
[[ -x .venv/bin/python ]] && PY=.venv/bin/python
"$PY" scripts/verify_rh_folha_impressao_excel_path.py
echo "POST_DEPLOY_OK 0311c-rh-holerite-visual"
