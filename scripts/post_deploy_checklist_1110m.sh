#!/usr/bin/env bash
# Pós-deploy Render — CHECKLIST 11/10m (sem migrate)
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
"$PY" -c "
import scripts.verify_meta_c_vila_abertura as v
v.test_static()
v.test_ramp_vila()
print('META 11/10m smoke OK')
"

echo "OK pós-deploy 11/10m — conferir BI Meta C Vila (domingo R\$ 0) e filtro Números"
