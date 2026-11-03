#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
PY=python3
[[ -x .venv/bin/python ]] && PY=.venv/bin/python
"$PY" manage.py migrate produtos 0154_whatsapp_public_origin --noinput
"$PY" scripts/verify_wa_public_origin_register.py
echo "POST_DEPLOY_OK 0311-wa-origin-pc"
