#!/usr/bin/env bash
# Pós-deploy Render — META-POR-LOJA · migration 0144
set -euo pipefail
cd "$(dirname "$0")/.."
PY=python3
[[ -x .venv/bin/python ]] && PY=.venv/bin/python

"$PY" manage.py migrate produtos 0144 --noinput
curl -fsS -o /dev/null -w "healthz HTTP %{http_code}\n" "${RENDER_EXTERNAL_URL:-http://127.0.0.1:8000}/healthz" || true

AGRO_PIN_TESTE=9973 "$PY" -c "
import os, sys
from pathlib import Path
sys.path.insert(0, str(Path('.').resolve()))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
import django
django.setup()
from scripts.verify_meta_mostruario_path import test_arquivos_rotas, test_math_e_format
test_arquivos_rotas()
test_math_e_format()
print('post-deploy META OK')
"
