#!/usr/bin/env bash
# Pós-deploy Render — CHECKLIST 11/10k (migrate 0144)
set -euo pipefail
cd "$(dirname "$0")/.."

echo "== migrate 0144 (META por loja) =="
PY=python3
[[ -x .venv/bin/python ]] && PY=.venv/bin/python
"$PY" manage.py migrate produtos 0144_metavendafaixaagro_deposito --noinput

echo "== healthz =="
if [[ -n "${PORT:-}" ]]; then
  curl -sf "http://127.0.0.1:${PORT}/healthz" && echo " healthz OK"
else
  echo "(skip healthz — PORT não definido; abra /healthz no browser)"
fi

"$PY" scripts/verify_vendas_lojas_pwa_install.py | tail -3
AGRO_PIN_TESTE=9973 "$PY" -c "
import os, sys
from pathlib import Path
sys.path.insert(0, str(Path('.').resolve()))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
import django; django.setup()
from scripts.verify_meta_mostruario_path import test_arquivos_rotas, test_math_e_format, test_seed_e_crud
test_arquivos_rotas(); test_math_e_format(); test_seed_e_crud()
print('META core OK')
" | tail -2

echo "OK pós-deploy 11/10k — SistApp Ajuste hub · META Centro/Vila · Zap enxuto"
