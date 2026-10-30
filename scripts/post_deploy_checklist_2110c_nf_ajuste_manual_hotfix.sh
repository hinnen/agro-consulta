#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
PY=python3
[[ -x .venv/bin/python ]] && PY=.venv/bin/python
"$PY" scripts/verify_nf_ajuste_manual_rateio_path.py
"$PY" scripts/verify_nf_forn_unif_ajuste_path.py
echo "POST_DEPLOY_OK 2110c-nf-ajuste-manual-hotfix"
