#!/usr/bin/env bash
# 13º sugestão · rh.0009 sobe no build.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
PY=python3
[[ -x .venv/bin/python ]] && PY=.venv/bin/python
DATABASE_URL= "$PY" scripts/verify_rh_decimo_terceiro_path.py
"$PY" manage.py showmigrations rh | grep -E '0009_ajustedecimoterceiro'
echo "POST_DEPLOY_OK 0411c-rh-decimo-sugestao"
