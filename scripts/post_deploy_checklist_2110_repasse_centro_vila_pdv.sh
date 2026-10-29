#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
PY=python3
[[ -x .venv/bin/python ]] && PY=.venv/bin/python
export AGRO_PIN_TESTE="${AGRO_PIN_TESTE:-9973}"
git fetch origin teste 2>/dev/null || true
"$PY" manage.py migrate --noinput
"$PY" scripts/verify_repasse_centro_vila_path.py
"$PY" scripts/verify_repasse_vila_path.py
"$PY" scripts/verify_repasse_pdv_overlay_path.py
"$PY" scripts/verify_repasse_centro_vila_deep.py
echo "POST_DEPLOY_OK 2110-repasse-centro-vila-pdv"
