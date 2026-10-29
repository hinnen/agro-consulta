#!/usr/bin/env bash
# Pós-deploy Render — CHECKLIST 11/10l (sem migrate)
set -euo pipefail
cd "$(dirname "$0")/.."

echo "== healthz =="
if [[ -n "${PORT:-}" ]]; then
  curl -sf "http://127.0.0.1:${PORT}/healthz" && echo " healthz OK"
else
  echo "(skip healthz — abra /healthz no browser)"
fi

PY=python3
[[ -x .venv/bin/python ]] && PY=.venv/bin/python
"$PY" manage.py test produtos.tests_vendas_lojas_ritmo -v0 2>&1 | tail -3
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
print('META path OK')
" | tail -2

echo "OK pós-deploy 11/10l — conferir /meta/ (Centro+Vila) e Vendas lojas (ritmo hora)"
