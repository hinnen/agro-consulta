#!/usr/bin/env bash
# Pós-deploy — 12/10p itens 1+2 (sem migrate)
set -euo pipefail
cd "$(dirname "$0")/.."

echo "== healthz =="
if [[ -n "${PORT:-}" ]]; then
  curl -sf "http://127.0.0.1:${PORT}/healthz" && echo " healthz OK"
fi

PY=python3
[[ -x .venv/bin/python ]] && PY=.venv/bin/python
echo "== limpar overlays agro:vn fantasma =="
"$PY" manage.py limpar_overlays_agro_vn_fantasma || echo "(conferir log)"

"$PY" scripts/verify_cadastro_agro_vn_path.py | tail -2
"$PY" scripts/verify_classificacao_medicamentos_path.py | tail -2
echo "OK pós-deploy 12/10p VN+MED"
