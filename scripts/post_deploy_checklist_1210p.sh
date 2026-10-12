#!/usr/bin/env bash
# Pós-deploy Render — CHECKLIST 12/10p
set -euo pipefail
cd "$(dirname "$0")/.."

echo "== healthz =="
if [[ -n "${PORT:-}" ]]; then
  curl -sf "http://127.0.0.1:${PORT}/healthz" && echo " healthz OK"
else
  echo "(skip healthz)"
fi

PY=python3
[[ -x .venv/bin/python ]] && PY=.venv/bin/python

echo "== migrate 0145 =="
"$PY" manage.py migrate produtos 0145 --noinput

echo "== limpar overlays agro:vn fantasma (uma vez) =="
"$PY" manage.py limpar_overlays_agro_vn_fantasma || echo "(aviso: comando retornou erro — conferir logs)"

"$PY" scripts/verify_cadastro_agro_vn_path.py | tail -3
"$PY" scripts/verify_classificacao_medicamentos_path.py | tail -3

echo "OK pós-deploy 12/10p — cadastro VN · MED · clientes % PDV"
