#!/usr/bin/env bash
# Pós-deploy Render — CHECKLIST 12/10n (sem migrate)
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
"$PY" scripts/verify_etiquetas_mobile_flow_path.py | tail -2
"$PY" scripts/verify_classificacao_medicamentos_path.py | tail -2

echo "OK pós-deploy 12/10n — GM etiquetas mobile · /fiado/.../revisar-limites/ · cadastro ERP medicamentos"
