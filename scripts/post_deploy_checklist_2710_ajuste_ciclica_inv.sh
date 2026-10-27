#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
PY=python3
[[ -x .venv/bin/python ]] && PY=.venv/bin/python
"$PY" manage.py migrate estoque 0021_inventario_absoluto --noinput
"$PY" scripts/verify_ajuste_camera_bip_path.py
"$PY" scripts/verify_contagem_ciclica_admin_path.py
echo "POST_DEPLOY_OK 2710-ajuste-ciclica-inv"
