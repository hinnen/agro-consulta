#!/usr/bin/env bash
# 13º · rh.0008 sobe no build (migrate). Confere a migração aplicada.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
PY=python3
[[ -x .venv/bin/python ]] && PY=.venv/bin/python
"$PY" scripts/verify_rh_decimo_terceiro_path.py
"$PY" manage.py showmigrations rh | grep -E '0008_decimoterceiro'
echo "POST_DEPLOY_OK 0411-rh-decimo"
